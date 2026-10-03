#!/usr/bin/env python3
"""Evaluate private daemon evidence; exit 0 healthy, 1 alert, 2 unknown."""
import argparse
import json
import math
from pathlib import Path
import shutil
import time


def number(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
        raise ValueError("invalid numeric evidence")
    return value


def evaluate(summary, status, disk_free_bytes, now, max_age_seconds,
             min_free_bytes, max_pending_bytes, max_queue_seconds):
    for value in (disk_free_bytes, now, max_age_seconds, min_free_bytes, max_pending_bytes, max_queue_seconds):
        number(value)
    if not isinstance(summary, dict) or not isinstance(status, dict):
        raise ValueError("missing report objects")
    if status.get("format_version") != 1 or status.get("state_exists") is not True:
        raise ValueError("state evidence missing or unsupported")
    if summary.get("event") not in ("metrics", "run_summary") or not summary.get("run_id"):
        raise ValueError("daemon report identity missing")
    age = now - number(summary.get("timestamp"))
    if age < -5 or age > max_age_seconds:
        raise ValueError("daemon evidence is stale or its clock differs")
    state, jobs = summary.get("state"), status.get("jobs")
    if not isinstance(state, dict) or not isinstance(jobs, dict):
        raise ValueError("missing durable/runtime counters")
    uncertain = max(number(state.get("merge_uncertain_rows")), number(jobs.get("merge_uncertain")))
    pending = number(state.get("pending_bytes"))
    age_queue = number(state.get("oldest_queue_seconds"))
    source_pending = number(state.get("source_apply_pending_bytes"))
    quarantine = state.get("quarantined_tables")
    if not isinstance(quarantine, dict):
        raise ValueError("missing quarantine evidence")
    alerts = []
    if summary["event"] == "run_summary":
        alerts.append("daemon_stopped")
    if uncertain or quarantine:
        alerts.append("unknown_output_isolated")
    if state.get("health") != "normal":
        alerts.append("daemon_degraded")
    if state.get("errors"):
        alerts.append("worker_errors")
    if disk_free_bytes < min_free_bytes:
        alerts.append("low_disk")
    if max(pending, source_pending) > max_pending_bytes:
        alerts.append("backlog_bytes")
    if age_queue > max_queue_seconds:
        alerts.append("backlog_age")
    return dict(format_version=1, ok=not alerts, availability_known=True, exit_code=int(bool(alerts)),
                alerts=alerts, evidence_age_seconds=max(0, age),
                pending_bytes=pending, source_apply_pending_bytes=source_pending,
                oldest_queue_seconds=age_queue, disk_free_bytes=disk_free_bytes,
                merge_uncertain_rows=uncertain, quarantined_targets=len(quarantine))


def unknown():
    return dict(format_version=1, ok=False, availability_known=False,
                exit_code=2, alerts=["evidence_unavailable_or_invalid"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--state-directory", type=Path, required=True)
    # Thresholds belong to the deployed workload; the tool never changes daemon
    # budgets or formal SLO. Idle status may be spaced 300s apart by default.
    parser.add_argument("--max-age-seconds", type=float, default=660)
    parser.add_argument("--min-free-bytes", type=int, required=True)
    parser.add_argument("--max-pending-bytes", type=int, required=True)
    parser.add_argument("--max-queue-seconds", type=float, required=True)
    args = parser.parse_args()
    try:
        now = time.time()
        # Status is an operator-generated sample, not a daemon heartbeat.
        if now - args.status.stat().st_mtime > args.max_age_seconds or args.status.stat().st_mtime > now+5:
            raise ValueError("durable status sample is stale")
        value = evaluate(json.loads(args.summary.read_text()), json.loads(args.status.read_text()),
                         shutil.disk_usage(args.state_directory).free, now,
                         args.max_age_seconds, args.min_free_bytes,
                         args.max_pending_bytes, args.max_queue_seconds)
    except (OSError, ValueError, TypeError, OverflowError):
        value = unknown()
    print(json.dumps(value, sort_keys=True), flush=True)
    return value["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
