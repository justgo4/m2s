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
                    exact_cdc_age_seconds=dict(
                        n=1000,avg=1.5,max=12.0),
                    lag_over_5=50,
                    lag_over_10=10,
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
        memory_mb=4096,
        resource_fingerprint=dict(
            architecture="x86_64",
            system="Linux",
            kernel_release="test",
            python_version="3.14.0",
            logical_cpus=8,
            cgroup_cpu_quota_cores=8.0,
            cgroup_memory_limit_bytes=8*1024**3,
        ),
        software_fingerprint=dict(
            code_revision="a"*40,
            mysql_version="8.4.6",
            mysql_gtid_mode="ON",
            mysql_binlog_format="ROW",
            mysql_binlog_row_image="FULL",
            starrocks_version="4.1.1-test",
            duckdb_version="1.5.5",
            pyarrow_version="25.0.1",
            pymysql_version="1.2.3",
            sqlglot_version="30.18.0",
        ),
        daemon_resources=dict(
            supported=True,
            samples=int(elapsed),
            process_identities_seen=12,
            peak_processes=3,
            peak_rss_bytes=768*1024**2,
            cpu_seconds=36*3600,
            read_bytes=4*1024**3,
            write_bytes=8*1024**3,
        ),
        live_rows=int(50*elapsed),
        latency_samples=int(elapsed),
        latency_p50_seconds=1.0,
        latency_p95_seconds=4.9,
        latency_p99_seconds=9.9,
        latency_max_seconds=12.0,
        latency_over_5_seconds=200,
        latency_over_10_seconds=20,
        recovery_latency_samples=8,
        recovery_latency_p95_seconds=15.0,
        recovery_latency_p99_seconds=20.0,
        recovery_latency_max_seconds=22.0,
        dynamic_tasks=10,
        dynamic_task_ready_seconds=tasks,
        faults=[
            dict(
                sequence=100,
                sequence_after=300,
                source_rows_during_fault=200,
                restart_seconds=4.0,
                catchup_seconds=2.0,
                source_frontier=dict(
                    log_durable_seq=500,
                    base_applied_seq=500)),
            dict(
                sequence=200,
                sequence_after=450,
                source_rows_during_fault=250,
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
        debt=dict(
            state_storage_bytes=512*1024*1024,
            stateful_rows=1024,
            stateful_payload_bytes=32*1024*1024,
            max_rowset=73,
            rowset_red=700,
            per_table_max_rowset={
                "starrocks.events":73,
                "starrocks.agg_000":31,
            },
            per_table_version_recovery={
                "starrocks.events":False,
                "starrocks.agg_000":False,
            },
            version_recovery_active=False,
            pending_bytes=0,
            prepared_budget_used=0,
            field_overflow_rows=0,
        ),
        aggregate_checks=dict(
            expected_rows=1024,
            expected_digest="source-digest",
            all_match=True,
            tables={
                "agg_000":dict(
                    rows=1024,
                    digest="digest-000",
                    match=True),
                "agg_001":dict(
                    rows=1024,
                    digest="digest-001",
                    match=True),
                "agg_002":dict(
                    rows=1024,
                    digest="digest-002",
                    match=True),
                "agg_003":dict(
                    rows=1024,
                    digest="digest-003",
                    match=True),
                "agg_004":dict(
                    rows=1024,
                    digest="digest-004",
                    match=True),
                "agg_005":dict(
                    rows=1024,
                    digest="digest-005",
                    match=True),
                "agg_006":dict(
                    rows=1024,
                    digest="digest-006",
                    match=True),
                "agg_007":dict(
                    rows=1024,
                    digest="digest-007",
                    match=True),
                "agg_008":dict(
                    rows=1024,
                    digest="digest-008",
                    match=True),
                "agg_009":dict(
                    rows=1024,
                    digest="digest-009",
                    match=True),
                "agg_010":dict(
                    rows=1024,
                    digest="digest-010",
                    match=True),
            },
        ),
        share_mode="adaptive",
    )


def main():
    good=evaluate(summary())
    assert good["ok"],good
    assert good["evidence"]["max_snapshot_rows"]==50_000_000
    assert good["evidence"]["cdc_samples"]==1000

    # Lifetime exact threshold counts, not the bounded recent quantile
    # window, decide the default 5s/10s SLO gate.
    recent_only=summary()
    recent_only["tables"]["sink"]["total"]["cdc_age_seconds"]["p99"]=99.0
    result=evaluate(recent_only)
    assert result["ok"],result
    assert (
        result["evidence"]["tables"]["sink"]["slo_scope"]
        =="lifetime_exact_threshold_counts"
    )

    bad=summary()
    bad["tables"]["sink"]["total"]["lag_over_10"]=11
    result=evaluate(bad)
    assert not result["ok"]
    assert "sink:p99" in result["failures"]

    bad=summary()
    bad["tables"]["sink"]["total"]["lag_over_5"]=51
    result=evaluate(bad)
    assert not result["ok"]
    assert "sink:p95" in result["failures"]

    bad=summary()
    del bad["tables"]["sink"]["total"]["lag_over_5"]
    result=evaluate(bad)
    assert not result["ok"]
    assert "sink:exact_slo_counters_missing" in result["failures"]

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
    assert full["evidence"]["faults"][
        "source_rows_during_fault"]==[200,250]
    assert full["evidence"]["dynamic_tasks"]["ready"]==10
    assert full["evidence"]["latency_over_10_seconds"]==20
    assert full["evidence"]["recovery_latency_samples"]==8
    assert full["evidence"]["debt"]["max_rowset"]==73
    assert full["evidence"]["resources"]["logical_cpus"]==8
    assert full["evidence"]["resources"]["configured_memory_mb"]==4096
    assert full["evidence"]["software"]["code_revision"]=="a"*40
    assert full["evidence"]["software"]["mysql_gtid_mode"]=="ON"
    assert full["evidence"]["software"]["starrocks_version"].startswith("4.1.1")
    assert full["evidence"]["daemon_resources"]["peak_rss_bytes"]==768*1024**2
    assert abs(
        full["evidence"]["daemon_resources"]["cpu_core_equivalent"]
        -(36*3600)/workload()["duration_seconds"]
    )<1e-12

    bad=workload()
    del bad["resource_fingerprint"]
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "resource_fingerprint_missing" in full["failures"]

    bad=workload()
    bad["resource_fingerprint"]["cgroup_memory_limit_bytes"]=1024**3
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "configured_memory_exceeds_cgroup" in full["failures"]

    bad=workload()
    del bad["software_fingerprint"]
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "software_fingerprint_missing" in full["failures"]

    bad=workload()
    bad["software_fingerprint"]["code_revision"]="not-a-revision"
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "software_fingerprint_incomplete" in full["failures"]

    bad=workload()
    del bad["daemon_resources"]
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "daemon_resource_evidence_missing" in full["failures"]

    bad=workload()
    bad["daemon_resources"]["supported"]=False
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "daemon_resource_probe_unsupported" in full["failures"]

    bad=workload()
    bad["debt"]["max_rowset"]=-1
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "version_debt_unknown" in full["failures"]

    bad=workload()
    bad["debt"]["max_rowset"]=700
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "rowset_red_exceeded" in full["failures"]

    bad=workload()
    bad["debt"]["version_recovery_active"]=True
    bad["debt"]["per_table_version_recovery"][
        "starrocks.events"]=True
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "version_recovery_active" in full["failures"]

    bad=workload()
    bad["debt"]["field_overflow_rows"]=1
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "field_overflow_rows" in full["failures"]

    bad=workload()
    bad["debt"]={}
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "debt_evidence_missing" in full["failures"]

    bad=workload()
    bad["recovery_latency_samples"]=0
    bad["recovery_latency_p95_seconds"]=None
    bad["recovery_latency_p99_seconds"]=None
    bad["recovery_latency_max_seconds"]=None
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "fault_recovery_latency_missing" in full["failures"]

    bad=workload()
    bad["faults"][0]["source_frontier"][
        "base_applied_seq"]=499
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "fault_source_not_caught_up" in full["failures"]

    bad=workload()
    bad["faults"][0][
        "source_rows_during_fault"]=0
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "fault_source_stalled" in full["failures"]

    bad=workload()
    bad["live_rows"]=100
    bad["latency_p99_seconds"]=10.01
    bad["dynamic_task_ready_seconds"].pop(
        "starrocks.agg_010")
    bad["faults"]=[]
    bad["final_state"]["pending"]=1
    bad["target_totals"]=[1,2]
    bad["aggregate_checks"]["all_match"]=False
    bad["aggregate_checks"]["tables"]["agg_010"]["match"]=False
    full=evaluate_workload(bad)
    assert not full["ok"]
    for reason in (
        "observed_rows_per_second",
        "latency_p99",
        "dynamic_tasks_not_ready",
        "fault_injection",
        "pending_jobs",
        "source_target_mismatch",
        "aggregate_target_mismatch",
    ):
        assert reason in full["failures"],full

    print(
        "longhaul_gate_test ok 50m_72h 50rps lifetime_exact_p95_p99 cross_restart "
        "healthy_recovery_latency drain stateful_health space_version_debt "
        "rowset_recovery overflow_fail_closed reproducible_resource_fingerprint "
        "daemon_process_resource_evidence software_fingerprint",
        flush=True,
    )


if __name__=="__main__":
    main()
