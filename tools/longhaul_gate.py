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

    source_run=dict(summary.get("source_run") or {})
    log_stats=dict(source_run.get("log_stats") or {})
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
    if list(source_run.get("counter_regressions") or ()):
        failures.append("source_counter_regression")
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


def evaluate_workload(
        report,
        min_elapsed_seconds=72*3600,
        min_initial_rows=50_000_000,
        min_cdc_rows_per_second=49.0,
        max_p95_seconds=5.0,
        max_p99_seconds=10.0,
        min_latency_samples_per_second=0.90,
        min_dynamic_tasks=10,
        min_faults=1,
        require_drained=True,
        require_sharing=True,
):
    failures=[]
    evidence=dict()
    if not isinstance(report,dict):
        return dict(
            ok=False,
            failures=["report_not_object"],
            evidence={})
    if report.get("kind")!="m2s_longhaul_workload":
        failures.append("not_longhaul_workload")

    elapsed=_number(
        report.get("duration_seconds"),
        "duration_seconds")
    initial_rows=_integer(
        report.get("initial_rows"),
        "initial_rows")
    live_rows=_integer(
        report.get("live_rows"),
        "live_rows")
    configured_rate=_number(
        report.get("rows_per_second"),
        "rows_per_second")
    observed_rate=(
        float(live_rows)/elapsed
        if elapsed>0 else 0.0)
    samples=_integer(
        report.get("latency_samples"),
        "latency_samples")
    sample_rate=(
        float(samples)/elapsed
        if elapsed>0 else 0.0)
    p95=_number(
        report.get("latency_p95_seconds"),
        "latency_p95_seconds")
    p99=_number(
        report.get("latency_p99_seconds"),
        "latency_p99_seconds")

    evidence.update(
        elapsed_seconds=elapsed,
        initial_rows=initial_rows,
        live_rows=live_rows,
        configured_rows_per_second=configured_rate,
        observed_rows_per_second=observed_rate,
        latency_samples=samples,
        latency_samples_per_second=sample_rate,
        latency_p95_seconds=p95,
        latency_p99_seconds=p99,
    )
    if elapsed<float(min_elapsed_seconds):
        failures.append("elapsed_seconds")
    if initial_rows<int(min_initial_rows):
        failures.append("initial_rows")
    if configured_rate<50.0:
        failures.append("configured_rows_per_second")
    if observed_rate<float(min_cdc_rows_per_second):
        failures.append("observed_rows_per_second")
    if sample_rate<float(min_latency_samples_per_second):
        failures.append("latency_sample_density")
    if p95>float(max_p95_seconds):
        failures.append("latency_p95")
    if p99>float(max_p99_seconds):
        failures.append("latency_p99")

    dynamic_tasks=_integer(
        report.get("dynamic_tasks",0),
        "dynamic_tasks")
    ready=dict(
        report.get("dynamic_task_ready_seconds") or {})
    ready_values={}
    for sink,value in sorted(ready.items()):
        ready_values[str(sink)]=_number(
            value,str(sink)+".time_to_ready")
    evidence["dynamic_tasks"]=dict(
        requested=dynamic_tasks,
        ready=len(ready_values),
        time_to_ready_seconds=ready_values,
        max_time_to_ready_seconds=(
            max(ready_values.values())
            if ready_values else None),
    )
    if dynamic_tasks<int(min_dynamic_tasks):
        failures.append("dynamic_tasks")
    if len(ready_values)!=dynamic_tasks:
        failures.append("dynamic_tasks_not_ready")

    faults=list(report.get("faults") or ())
    restart_seconds=[]
    catchup_seconds=[]
    frontiers=[]
    for index,item in enumerate(faults):
        item=dict(item or {})
        restart_seconds.append(_number(
            item.get("restart_seconds"),
            "fault_%d.restart_seconds" % index))
        catchup_seconds.append(_number(
            item.get("catchup_seconds"),
            "fault_%d.catchup_seconds" % index))
        frontier=dict(
            item.get("source_frontier") or {})
        durable=_integer(
            frontier.get("log_durable_seq",0),
            "fault_%d.log_durable_seq" % index)
        applied=_integer(
            frontier.get("base_applied_seq",0),
            "fault_%d.base_applied_seq" % index)
        frontiers.append(dict(
            log_durable_seq=durable,
            base_applied_seq=applied))
        if durable!=applied:
            failures.append("fault_source_not_caught_up")
    evidence["faults"]=dict(
        count=len(faults),
        restart_seconds=restart_seconds,
        catchup_seconds=catchup_seconds,
        source_frontiers=frontiers,
        max_restart_seconds=(
            max(restart_seconds)
            if restart_seconds else None),
        max_catchup_seconds=(
            max(catchup_seconds)
            if catchup_seconds else None),
    )
    if len(faults)<int(min_faults):
        failures.append("fault_injection")

    final_state=dict(report.get("final_state") or {})
    if not final_state:
        failures.append("final_state_missing")
    else:
        pending=_integer(
            final_state.get("pending",0),
            "final_state.pending")
        deliveries=_integer(
            final_state.get("deliveries",0),
            "final_state.deliveries")
        durable=_integer(
            final_state.get("log_durable_seq",0),
            "final_state.log_durable_seq")
        applied=_integer(
            final_state.get("base_applied_seq",0),
            "final_state.base_applied_seq")
        followers=_integer(
            final_state.get("shared_followers",0),
            "final_state.shared_followers")
        evidence["final_state"]=dict(
            pending=pending,
            deliveries=deliveries,
            log_durable_seq=durable,
            base_applied_seq=applied,
            shared_followers=followers,
        )
        if require_drained and pending:
            failures.append("pending_jobs")
        if require_drained and deliveries:
            failures.append("inflight")
        if durable!=applied:
            failures.append("source_base_not_caught_up")
        if (
            require_sharing
            and dynamic_tasks
            and str(report.get("share_mode"))!="off"
            and followers<dynamic_tasks
        ):
            failures.append("shared_followers")

    source_totals=report.get("source_totals")
    target_totals=report.get("target_totals")
    evidence["source_target_exact"]=(
        source_totals==target_totals
        and source_totals is not None)
    if source_totals is None or target_totals is None:
        failures.append("source_target_totals_missing")
    elif source_totals!=target_totals:
        failures.append("source_target_mismatch")

    aggregate_checks=dict(
        report.get("aggregate_checks") or {})
    aggregate_tables=dict(
        aggregate_checks.get("tables") or {})
    aggregate_matches={
        str(table):bool(
            (value or {}).get("match",False))
        for table,value in aggregate_tables.items()
    }
    expected_aggregate_targets=dynamic_tasks+1
    evidence["aggregate_exactness"]=dict(
        expected_targets=expected_aggregate_targets,
        checked_targets=len(aggregate_matches),
        all_match=(
            bool(aggregate_checks.get("all_match",False))
            and bool(aggregate_matches)
            and all(aggregate_matches.values())
        ),
        matches=aggregate_matches,
    )
    if len(aggregate_matches)!=expected_aggregate_targets:
        failures.append("aggregate_targets_missing")
    if (
        not aggregate_checks.get("all_match",False)
        or not aggregate_matches
        or not all(aggregate_matches.values())
    ):
        failures.append("aggregate_target_mismatch")

    return dict(
        ok=not failures,
        failures=sorted(set(failures)),
        evidence=evidence,
        thresholds=dict(
            min_elapsed_seconds=float(
                min_elapsed_seconds),
            min_initial_rows=int(min_initial_rows),
            min_cdc_rows_per_second=float(
                min_cdc_rows_per_second),
            max_p95_seconds=float(max_p95_seconds),
            max_p99_seconds=float(max_p99_seconds),
            min_latency_samples_per_second=float(
                min_latency_samples_per_second),
            min_dynamic_tasks=int(min_dynamic_tasks),
            min_faults=int(min_faults),
            require_drained=bool(require_drained),
            require_sharing=bool(require_sharing),
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
        "--min-latency-samples-per-second",
        type=float,default=0.90)
    parser.add_argument(
        "--min-dynamic-tasks",type=int,
        default=10)
    parser.add_argument(
        "--min-faults",type=int,
        default=1)
    parser.add_argument(
        "--allow-private-state",
        action="store_true")
    parser.add_argument(
        "--output",type=Path)
    args=parser.parse_args()

    summary=json.loads(
        args.summary.read_text(encoding="utf-8"))
    if summary.get("kind")=="m2s_longhaul_workload":
        result=evaluate_workload(
            summary,
            min_elapsed_seconds=args.min_elapsed_seconds,
            min_initial_rows=args.min_snapshot_rows,
            min_cdc_rows_per_second=(
                args.min_cdc_rows_per_second),
            max_p95_seconds=args.max_p95_seconds,
            max_p99_seconds=args.max_p99_seconds,
            min_latency_samples_per_second=(
                args.min_latency_samples_per_second),
            min_dynamic_tasks=args.min_dynamic_tasks,
            min_faults=args.min_faults,
            require_drained=not args.allow_undrained,
            require_sharing=not args.allow_private_state,
        )
    else:
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
