"""Named development workloads; canonical P11 remains a separate contract."""
from pathlib import Path
import sys

import p11_profile


DEVELOPMENT = {
    "smoke": (5_000, 60, 2, 30, 4096),
    "small": (100_000, 300, 4, 120, 4096),
    "million": (1_000_000, 1800, 4, 600, 4096),
    "medium": (5_000_000, 7200, 10, 1800, 8192),
    "scale-short": (50_000_000, 4*3600, 10, 3600, 8192),
    "soak": (1_000_000, 24*3600, 10, 6*3600, 8192),
}
NAMES = tuple(DEVELOPMENT) + ("p11",)


def parameters(name, protocol=None):
    if name not in NAMES:
        raise ValueError("unknown validation profile: " + str(name))
    values = dict(p11_profile.PARAMETERS)
    if name != "p11":
        rows, seconds, tasks, faults, memory = DEVELOPMENT[name]
        values.update(
            rows=rows, duration_seconds=seconds, dynamic_tasks=tasks,
            fault_every_seconds=faults, memory_mb=memory,
            checkpoint_seconds=min(300, max(10, seconds//10)),
        )
        if name == "smoke":
            # Match the existing live E2E's deliberately short cold-start/fault
            # workload. This is a protocol smoke, not the performance gate.
            values.update(sample_seconds=0.5, snapshot_rows=512, fault_every_seconds=20)
    if protocol is not None:
        if protocol not in ("merge_async", "transaction"):
            raise ValueError("invalid output protocol")
        if name == "p11" and protocol != values["load_mode"]:
            raise ValueError("canonical P11 protocol cannot be overridden")
        values["load_mode"] = protocol
    return values


def plan(name, root, directory, protocol=None, service_pids=None):
    root = Path(root).resolve()
    directory = Path(directory).resolve()
    values = parameters(name, protocol)
    workload = [sys.executable, str(root/"tools/longhaul_workload.py"), "--isolated"]
    for key, value in values.items():
        workload.extend(["--" + key.replace("_", "-"), str(value)])
    if name == "p11":
        workload.append("--certification-profile")
    for key, value in sorted((service_pids or {}).items()):
        if key not in ("mysql_resource_pid", "starrocks_fe_resource_pid", "starrocks_be_resource_pid"):
            raise ValueError("unknown resource PID field")
        if int(value) <= 0:
            raise ValueError("resource PID must be positive")
        workload.extend(["--" + key.replace("_", "-"), str(int(value))])
    workload.extend([
        "--work-directory", str(directory/"work"),
        "--output", str(directory/"workload.json"),
    ])
    gate = [sys.executable, str(root/"tools/longhaul_gate.py"), str(directory/"workload.json")]
    thresholds = dict(max_p95_seconds=5, max_p99_seconds=10,
                      min_cdc_rows_per_second=49, min_latency_samples_per_second=0.90 if name == "p11" else 0.75)
    if name == "smoke":
        thresholds.update(max_p95_seconds=30, max_p99_seconds=60,
                          min_cdc_rows_per_second=40, min_latency_samples_per_second=0.2)
    if name == "p11":
        gate.append("--require-profile")
    else:
        gate.extend([
            "--min-elapsed-seconds", str(values["duration_seconds"]),
            "--min-snapshot-rows", str(values["rows"]),
            "--min-dynamic-tasks", str(values["dynamic_tasks"]),
            "--min-faults", "1",
        ])
        for key, value in thresholds.items():
            gate.extend(["--" + key.replace("_", "-"), str(value)])
    gate.extend(["--output", str(directory/"gate.json")])
    return dict(
        profile=name, formal=name == "p11", parameters=values, gate_thresholds=thresholds,
        workload_command=workload, gate_command=gate,
        scope=p11_profile.NAME if name == "p11" else "development:" + name,
    )


def validate_report(report, expected, revision):
    if report.get("kind") != "m2s_longhaul_workload":
        raise ValueError("not a finished workload report")
    if report.get("workload_profile") != (p11_profile.NAME if expected["formal"] else "custom"):
        raise ValueError("workload report profile identity differs")
    fields = dict(load_mode="protocol", rows="initial_rows", duration_seconds="source_schedule_seconds")
    for key, value in expected["parameters"].items():
        if report.get(fields.get(key, key)) != value:
            raise ValueError("workload report parameter differs: " + key)
    if report.get("software_fingerprint", {}).get("code_revision") != revision:
        raise ValueError("workload report revision differs")
    if report.get("work_directory_persistent") is not True:
        raise ValueError("workload report lacks persistent evidence")


def hosted_compatible(name):
    # Allow seed/drain/oracle/build overhead; never map 24h/72h onto a 6h host.
    return parameters(name)["duration_seconds"] <= 2*3600
