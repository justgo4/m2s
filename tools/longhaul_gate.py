#!/usr/bin/env python3
"""Evaluate one finished daemon run against the fixed-resource long-haul gate.

The production target is 50M initial rows + 50 source rows/s for 72h with
healthy-run MySQL commit -> StarRocks queryable P95 <= 5s and P99 <= 10s.
This evaluator is intentionally separate from the workload driver: it consumes
the daemon's durable run_summary JSON and fails closed on missing evidence.
"""
import argparse
import json
from pathlib import Path
import sys


def _number(value,name):
    if value is None:
        raise ValueError(name+" is missing")
    value=float(value)
    if value<0:
        raise ValueError(name+" cannot be negative")
    return value


def _integer(value,name):
    value=int(_number(value,name))
    if value<0:
        raise ValueError(name+" cannot be negative")
    return value


def evaluate(
        summary,
        min_elapsed_seconds=72*3600,
        min_snapshot_rows=50_000_000,
        max_p95_seconds=5.0,
        max_p99_seconds=10.0,
        min_cdc_samples=1,
        min_cdc_rows_per_second=49.0,
        require_drained=True,
):
    failures=[]
    evidence=dict()

    if not isinstance(summary,dict):
        return dict(ok=False,failures=["summary_not_object"],evidence={})
    if summary.get("event")!="run_summary":
        failures.append("not_run_summary")

    elapsed=_number(
        summary.get("elapsed_seconds"),"elapsed_seconds")
    evidence["elapsed_seconds"]=elapsed
    if elapsed<float(min_elapsed_seconds):
        failures.append("elapsed_seconds")

    source=dict(summary.get("source") or {})
    log_stats=dict(source.get("log_stats") or {})
    source_event_rows=0
    source_commits=0
    for table,value in sorted(log_stats.items()):
        value=dict(value or {})
        source_event_rows+=_integer(
            value.get("event_rows",0),
            str(table)+".source_event_rows")
        source_commits+=_integer(
            value.get("commits",0),
            str(table)+".source_commits")
    source_rate=(
        float(source_event_rows)/elapsed
        if elapsed>0 else 0.0)
    evidence["source_cdc"]=dict(
        event_rows=source_event_rows,
        commits=source_commits,
        rows_per_second=source_rate,
    )
    if not log_stats:
        failures.append("source_log_stats_missing")
    if source_rate<float(min_cdc_rows_per_second):
        failures.append("source_cdc_rows_per_second")

    state=dict(summary.get("state") or {})
    if str(state.get("health"))!="normal":
        failures.append("daemon_health")
    if list(state.get("errors") or ()):
        failures.append("daemon_errors")
    if dict(state.get("quarantined_tables") or {}):
        failures.append("quarantined_tables")
    if require_drained:
        for name in (
            "pending_jobs","prepared_bytes",
            "prepare_reserved_bytes","inflight",
            "merge_uncertain_rows",
        ):
            if _integer(state.get(name,0),name)!=0:
                failures.append(name)

    tables=dict(summary.get("tables") or {})
    if not tables:
        failures.append("no_tables")
    max_snapshot=0
    total_cdc_samples=0
    table_evidence={}
    for table,value in sorted(tables.items()):
        total=dict((value or {}).get("total") or {})
        snapshot_rows=max(
            _integer(
                total.get("snapshot_read_rows",0),
                table+".snapshot_read_rows"),
            _integer(
                total.get("snapshot_rows",0),
                table+".snapshot_rows"),
        )
        max_snapshot=max(max_snapshot,snapshot_rows)
        cdc=dict(total.get("cdc_age_seconds") or {})
        n=_integer(
            cdc.get("total_n",cdc.get("n",0)),
            table+".cdc_age_seconds.n")
        p95=cdc.get("p95")
        p99=cdc.get("p99")
        if n:
            p95=_number(
                p95,table+".cdc_age_seconds.p95")
            p99=_number(
                p99,table+".cdc_age_seconds.p99")
            if p95>float(max_p95_seconds):
                failures.append(table+":p95")
            if p99>float(max_p99_seconds):
                failures.append(table+":p99")
        total_cdc_samples+=n
        table_evidence[str(table)]=dict(
            snapshot_rows=snapshot_rows,
            cdc_samples=n,
            cdc_p95_seconds=p95,
            cdc_p99_seconds=p99,
            lag_over_10=_integer(
                total.get("lag_over_10",0),
                table+".lag_over_10"),
        )
    evidence["tables"]=table_evidence
    evidence["max_snapshot_rows"]=max_snapshot
    evidence["cdc_samples"]=total_cdc_samples
    if max_snapshot<int(min_snapshot_rows):
        failures.append("snapshot_rows")
    if total_cdc_samples<int(min_cdc_samples):
        failures.append("cdc_samples")

    stateful=dict(summary.get("stateful") or {})
    if not stateful:
        failures.append("stateful_summary_missing")
    else:
        sharing=dict(stateful.get("sharing") or {})
        physical=dict(stateful.get("physical") or {})
        rebuilds=dict(stateful.get("rebuilds") or {})
        health=dict(physical.get("health") or {})
        if int(health.get("failed",0))>0:
            failures.append("physical_failed")
        if int(rebuilds.get("active",0))>0:
            failures.append("rebuilds_active")
        evidence["stateful"]=dict(
            aggregate_shared_followers=int(
                stateful.get("aggregate_shared_followers",0)),
            join_shared_followers=int(
                stateful.get("join_shared_followers",0)),
            max_visible_lag=int(
                sharing.get("max_visible_lag",0)),
            physical_total=int(
                physical.get("total",0)),
            physical_failed=int(
                health.get("failed",0)),
            rebuilds_active=int(
                rebuilds.get("active",0)),
        )

    return dict(
        ok=not failures,
        failures=sorted(set(failures)),
        evidence=evidence,
        thresholds=dict(
            min_elapsed_seconds=float(min_elapsed_seconds),
            min_snapshot_rows=int(min_snapshot_rows),
            max_p95_seconds=float(max_p95_seconds),
            max_p99_seconds=float(max_p99_seconds),
            min_cdc_samples=int(min_cdc_samples),
            min_cdc_rows_per_second=float(
                min_cdc_rows_per_second),
            require_drained=bool(require_drained),
        ),
    )


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary",type=Path)
    parser.add_argument(
        "--min-elapsed-seconds",type=float,
        default=72*3600)
    parser.add_argument(
        "--min-snapshot-rows",type=int,
        default=50_000_000)
    parser.add_argument(
        "--max-p95-seconds",type=float,
        default=5.0)
    parser.add_argument(
        "--max-p99-seconds",type=float,
        default=10.0)
    parser.add_argument(
        "--min-cdc-samples",type=int,
        default=1)
    parser.add_argument(
        "--min-cdc-rows-per-second",type=float,
        default=49.0)
    parser.add_argument(
        "--allow-undrained",action="store_true")
    parser.add_argument(
        "--output",type=Path)
    args=parser.parse_args()

    summary=json.loads(
        args.summary.read_text(encoding="utf-8"))
    result=evaluate(
        summary,
        min_elapsed_seconds=args.min_elapsed_seconds,
        min_snapshot_rows=args.min_snapshot_rows,
        max_p95_seconds=args.max_p95_seconds,
        max_p99_seconds=args.max_p99_seconds,
        min_cdc_samples=args.min_cdc_samples,
        min_cdc_rows_per_second=args.min_cdc_rows_per_second,
        require_drained=not args.allow_undrained,
    )
    payload=json.dumps(
        result,indent=2,sort_keys=True)+"\n"
    if args.output is not None:
        args.output.parent.mkdir(
            parents=True,exist_ok=True)
        args.output.write_text(
            payload,encoding="utf-8")
    sys.stdout.write(payload)
    raise SystemExit(0 if result["ok"] else 1)


if __name__=="__main__":
    main()
