#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

from tools.longhaul_gate import evaluate,evaluate_workload


def summary():
    elapsed=72*3600+1
    return dict(
        event="run_summary",
        elapsed_seconds=elapsed,
        source_run=dict(
            log_stats={
                "mysql.events":dict(
                    commits=100000,
                    event_rows=int(50*elapsed),
                    payload_bytes=1024**3,
                ),
            },
            counter_regressions=[],
        ),
        tables=dict(
            sink=dict(
                total=dict(
                    snapshot_read_rows=50_000_000,
                    snapshot_rows=50_000_000,
                    cdc_age_seconds=dict(
                        n=1000,total_n=1000,
                        p95=4.9,p99=9.9),
                    lag_over_10=3,
                )
            )
        ),
        state=dict(
            health="normal",
            errors=[],
            quarantined_tables={},
            pending_jobs=0,
            prepared_bytes=0,
            prepare_reserved_bytes=0,
            inflight=0,
            merge_uncertain_rows=0,
        ),
        stateful=dict(
            aggregate_shared_followers=9,
            join_shared_followers=4,
            sharing=dict(
                max_visible_lag=2),
            physical=dict(
                total=3,
                health=dict(ready=3),
            ),
            rebuilds=dict(
                active=0,phases={}),
        ),
    )


def workload():
    elapsed=72*3600+1
    tasks={
        "starrocks.agg_%03d" % index:30.0+index
        for index in range(1,11)
    }
    return dict(
        format_version=1,
        kind="m2s_longhaul_workload",
        protocol="merge_async",
        initial_rows=50_000_000,
        rows_per_second=50,
        duration_seconds=elapsed,
        live_rows=int(50*elapsed),
        latency_samples=int(elapsed),
        latency_p50_seconds=1.0,
        latency_p95_seconds=4.9,
        latency_p99_seconds=9.9,
        latency_max_seconds=12.0,
        dynamic_tasks=10,
        dynamic_task_ready_seconds=tasks,
        faults=[
            dict(
                sequence=100,
                restart_seconds=4.0,
                catchup_seconds=2.0,
                source_frontier=dict(
                    log_durable_seq=500,
                    base_applied_seq=500)),
            dict(
                sequence=200,
                restart_seconds=5.0,
                catchup_seconds=3.0,
                source_frontier=dict(
                    log_durable_seq=800,
                    base_applied_seq=800)),
        ],
        final_state=dict(
            pending=0,
            deliveries=0,
            log_durable_seq=1000,
            base_applied_seq=1000,
            shared_followers=10,
        ),
        source_totals=[62_960_000,12345],
        target_totals=[62_960_000,12345],
        share_mode="adaptive",
    )


def main():
    good=evaluate(summary())
    assert good["ok"],good
    assert good["evidence"]["max_snapshot_rows"]==50_000_000
    assert good["evidence"]["cdc_samples"]==1000

    bad=summary()
    bad["tables"]["sink"]["total"]["cdc_age_seconds"]["p99"]=10.01
    result=evaluate(bad)
    assert not result["ok"]
    assert "sink:p99" in result["failures"]

    bad=summary()
    bad["source_run"]["log_stats"]["mysql.events"]["event_rows"]=100
    result=evaluate(bad)
    assert not result["ok"]
    assert "source_cdc_rows_per_second" in result["failures"]

    bad=summary()
    bad["source_run"]["log_stats"]={}
    result=evaluate(bad)
    assert not result["ok"]
    assert "source_log_stats_missing" in result["failures"]

    bad=summary()
    bad["source_run"]["counter_regressions"]=[
        "mysql.events.event_rows:100>99"]
    result=evaluate(bad)
    assert not result["ok"]
    assert "source_counter_regression" in result["failures"]

    bad=summary()
    bad["elapsed_seconds"]=3600
    bad["state"]["pending_jobs"]=1
    bad["stateful"]["rebuilds"]["active"]=1
    result=evaluate(bad)
    assert not result["ok"]
    assert "elapsed_seconds" in result["failures"]
    assert "pending_jobs" in result["failures"]
    assert "rebuilds_active" in result["failures"]

    bad=summary()
    del bad["stateful"]
    result=evaluate(bad)
    assert not result["ok"]
    assert "stateful_summary_missing" in result["failures"]

    smoke=evaluate(
        summary(),
        min_elapsed_seconds=1,
        min_snapshot_rows=1,
        min_cdc_samples=1,
    )
    assert smoke["ok"]

    full=evaluate_workload(workload())
    assert full["ok"],full
    assert full["evidence"]["faults"]["count"]==2
    assert full["evidence"]["dynamic_tasks"]["ready"]==10

    bad=workload()
    bad["faults"][0]["source_frontier"][
        "base_applied_seq"]=499
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "fault_source_not_caught_up" in full["failures"]

    bad=workload()
    bad["live_rows"]=100
    bad["latency_p99_seconds"]=10.01
    bad["dynamic_task_ready_seconds"].pop(
        "starrocks.agg_010")
    bad["faults"]=[]
    bad["final_state"]["pending"]=1
    bad["target_totals"]=[1,2]
    full=evaluate_workload(bad)
    assert not full["ok"]
    for reason in (
        "observed_rows_per_second",
        "latency_p99",
        "dynamic_tasks_not_ready",
        "fault_injection",
        "pending_jobs",
        "source_target_mismatch",
    ):
        assert reason in full["failures"],full

    print(
        "longhaul_gate_test ok 50m_72h 50rps p95_p99 cross_restart "
        "drain stateful_health fail_closed",
        flush=True,
    )


if __name__=="__main__":
    main()
