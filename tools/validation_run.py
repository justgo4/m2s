#!/usr/bin/env python3
"""Plan, supervise and inspect isolated validation without an online AI session.

Each run has a new private directory and immutable workload identity. Resume
can only reevaluate an already completed, checksum-verified workload; it never
reinitializes interrupted databases. Linux process identities guard cancellation.
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import validation_profiles


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def revision(root=ROOT):
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True)
    if result.returncode or len(result.stdout.strip()) != 40:
        raise RuntimeError("validation requires a Git checkout with an exact revision")
    clean = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True)
    if clean.returncode or clean.stdout.strip():
        raise RuntimeError("validation requires a clean checkout; commit changes before running or resuming")
    return result.stdout.strip()


def process_identity(pid):
    try:
        raw = Path("/proc/%d/stat" % int(pid)).read_text()
        fields = raw[raw.rfind(")") + 2:].split()
        return dict(pid=int(pid), start_ticks=fields[19],
                    boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    except (OSError, ValueError, IndexError):
        return None


def alive(identity):
    return bool(identity) and process_identity(identity["pid"]) == identity


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024*1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_state(directory, record, **changes):
    record.update(changes, updated_at_unix=time.time())
    atomic_json(Path(directory)/"status.json", record)


def expected_plan(directory, state):
    directory = Path(directory).resolve()
    if state.get("format_version") != 1 or state.get("directory") != str(directory):
        raise ValueError("run identity/directory differs")
    if state["revision"] != revision():
        raise ValueError("code revision changed; preserve this run and use its original checkout")
    expected = validation_profiles.plan(
        state["profile"], ROOT, directory, state["protocol"], state["service_pids"])
    if expected["parameters"] != state["parameters"] or expected["scope"] != state["scope"]:
        raise ValueError("saved workload identity differs from the current profile")
    return expected


def verified_workload(directory, state, expected):
    path = Path(directory)/"workload.json"
    if not state.get("workload_passed") or checksum(path) != state.get("workload_sha256"):
        raise ValueError("no completed checksum-verified workload; initialization must not be repeated")
    validation_profiles.validate_report(read_json(path), expected, state["revision"])


def prepare(directory, name, protocol=None, service_pids=None):
    directory = Path(directory).resolve()
    expected = validation_profiles.plan(name, ROOT, directory, protocol, service_pids)
    current_revision = revision()
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    state = dict(
        format_version=1, run_id=uuid.uuid4().hex, directory=str(directory),
        revision=current_revision, profile=name, protocol=expected["parameters"]["load_mode"],
        parameters=expected["parameters"], service_pids=service_pids or {}, scope=expected["scope"],
        state="prepared", stage=None, workload_passed=False, gate_passed=False,
        created_at_unix=time.time(), supervisor=None, child=None,
    )
    atomic_json(directory/"plan.json", expected)
    write_state(directory, state)
    return state


def stop_child(proc):
    if proc.poll() is not None:
        return
    # Repeated cancellation must not interrupt the first cleanup and orphan
    # the workload/daemon while its own finally block is still unwinding.
    previous = {s: signal.signal(s, signal.SIG_IGN) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=120)  # workload unwinds daemon cleanup (90s).
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def execute_stage(directory, state, stage, command):
    with (Path(directory)/(stage + ".log")).open("ab") as log:
        proc = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        try:
            write_state(directory, state, stage=stage, child=process_identity(proc.pid))
            while proc.poll() is None:
                write_state(directory, state, heartbeat_at_unix=time.time())
                time.sleep(1)
            return proc.returncode
        finally:
            stop_child(proc)
            write_state(directory, state, child=None)


def cancellation_signal(signum, frame):
    raise InterruptedError("validation cancelled by signal %d" % signum)


@contextlib.contextmanager
def cancellation_guard():
    previous = {signum: signal.signal(signum, cancellation_signal)
                for signum in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def supervise(directory, resume_gate=False):
    directory = Path(directory).resolve()
    with (directory/"supervisor.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("this run already has a supervisor")
        state = read_json(directory/"status.json")
        expected = expected_plan(directory, state)
        if alive(state.get("supervisor")):
            raise RuntimeError("recorded supervisor is still alive")
        if resume_gate:
            if state.get("gate_passed"):
                raise ValueError("run already passed; no gate retry is needed")
            verified_workload(directory, state, expected)
            if alive(state.get("child")):
                raise RuntimeError("recorded workload/gate child is still alive")
        elif state["state"] != "prepared":
            raise ValueError("run already started; interrupted workloads cannot be restarted")
        write_state(directory, state, state="running", supervisor=process_identity(os.getpid()))
        try:
            with cancellation_guard():
                if not resume_gate:
                    code = execute_stage(directory, state, "workload", expected["workload_command"])
                    if code:
                        raise RuntimeError("workload exited with code %d" % code)
                    report_path = directory/"workload.json"
                    validation_profiles.validate_report(read_json(report_path), expected, state["revision"])
                    write_state(directory, state, workload_passed=True,
                                workload_sha256=checksum(report_path), stage="gate_ready")
                code = execute_stage(directory, state, "gate", expected["gate_command"])
                gate = read_json(directory/"gate.json")
                if code or gate.get("ok") is not True:
                    raise RuntimeError("gate failed; inspect gate.json and gate.log")
                write_state(directory, state, state="passed", gate_passed=True, stage="complete", error=None)
                return 0
        except (InterruptedError, KeyboardInterrupt) as exc:
            write_state(directory, state, state="interrupted", error=str(exc))
            return 130
        except Exception as exc:
            write_state(directory, state, state="failed", error=str(exc))
            return 1
        finally:
            write_state(directory, state, supervisor=None, child=None)


def status(directory):
    directory = Path(directory).resolve()
    state = read_json(directory/"status.json")
    result = dict(state)
    result["supervisor_alive"] = alive(state.get("supervisor"))
    result["child_alive"] = alive(state.get("child"))
    if state["state"] == "running" and not result["supervisor_alive"]:
        result["state"] = "interrupted"
    result["can_resume_gate"] = False
    if not state.get("gate_passed") and not result["supervisor_alive"] and not result["child_alive"]:
        try:
            verified_workload(directory, state, expected_plan(directory, state))
            result["can_resume_gate"] = True
        except (OSError, ValueError, RuntimeError):
            pass
    checkpoint = directory/"workload-checkpoint.json"
    if checkpoint.exists():
        result["checkpoint"] = read_json(checkpoint)
    return result


def cancel(directory):
    state = read_json(Path(directory)/"status.json")
    owner = state.get("supervisor")
    if not alive(owner):
        raise RuntimeError("no verified live supervisor; refusing to signal a reused or unknown PID")
    # Bind the actual process, then recheck its start identity. PIDFD signaling
    # cannot accidentally target a new process if the numeric PID is reused.
    descriptor = os.pidfd_open(owner["pid"])
    try:
        if not alive(owner):
            raise RuntimeError("supervisor identity changed; refusing cancellation")
        signal.pidfd_send_signal(descriptor, signal.SIGTERM)
    finally:
        os.close(descriptor)
    return dict(cancellation_requested=True, run_id=state["run_id"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "run", "status", "cancel", "resume-gate", "_supervise"))
    parser.add_argument("--profile", choices=validation_profiles.NAMES, default="small")
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--isolated", action="store_true")
    parser.add_argument("--detach", action="store_true")
    parser.add_argument("--protocol", choices=("merge_async", "transaction"))
    for key in ("mysql_resource_pid", "starrocks_fe_resource_pid", "starrocks_be_resource_pid"):
        parser.add_argument("--" + key.replace("_", "-"), type=int)
    args = parser.parse_args()
    directory = args.run_directory.resolve()
    try:
        if args.command == "plan":
            value = validation_profiles.plan(args.profile, ROOT, directory, args.protocol)
        elif args.command == "status":
            value = status(directory)
        elif args.command == "cancel":
            value = cancel(directory)
        elif args.command == "resume-gate":
            return supervise(directory, resume_gate=True)
        elif args.command == "_supervise":
            return supervise(directory)
        else:
            if not args.isolated:
                parser.error("--isolated is required; workload initializes disposable databases")
            pids = {key: getattr(args, key) for key in
                    ("mysql_resource_pid", "starrocks_fe_resource_pid", "starrocks_be_resource_pid")
                    if getattr(args, key) is not None}
            value = prepare(directory, args.profile, args.protocol, pids)
            if not args.detach:
                return supervise(directory)
            with (directory/"supervisor.log").open("ab") as log:
                proc = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), "_supervise", "--run-directory", str(directory)],
                    cwd=ROOT, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True)
            value = dict(run_id=value["run_id"], directory=str(directory), launcher_pid=proc.pid)
        print(json.dumps(value, sort_keys=True), flush=True)
        return 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps(dict(error=str(exc)), sort_keys=True), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
