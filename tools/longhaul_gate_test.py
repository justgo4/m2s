#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import p11_profile
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
    source_schedule=72*3600
    # Final drain/recovery is allowed to extend wall time beyond the source
    # generation window. Throughput/sample density must still be measured
    # against the scheduled source window.
    elapsed=source_schedule+601
    tasks={
        (
            "starrocks.agg_%03d" % index
            if index%2
            else "starrocks.join_%03d" % index
        ):30.0+index
        for index in range(1,11)
    }
    return dict(
        format_version=1,
        kind="m2s_longhaul_workload",
        workload_profile=p11_profile.NAME,
        protocol="merge_async",
        initial_rows=50_000_000,
        rows_per_second=50,
        duration_seconds=elapsed,
        source_schedule_seconds=source_schedule,
        healthy_observation_seconds=source_schedule,
        memory_mb=8192,
        work_directory_persistent=True,
        topology_resource_preflight=dict(
            ok=True,
            requested_memory_mb=8192,
            memory_mb=8192,
            detected_memory_mb=16384,
            memory_budget_available=True,
            cpu_cap=8,
            cpu_target=8,
            cpu_target_assumption="cpu_cap_worst_case",
            initial_physical_sinks=3,
            final_physical_sinks=13,
            dynamic_tasks=10,
            load_mode="merge_async",
            snapshot_workers=2,
            writer_max=2,
            requested_duckdb_mb=128,
            effective_duckdb_mb=128,
            minimum_per_engine_cap_mb=141,
            failing_physical_sinks=[],
            stages=[
                dict(
                    physical_sinks=value,
                    writers_per_sink=(
                        2 if value<=4 else 1),
                    engine_slots=(
                        3+4*value
                        if value<=4
                        else 3+2*value),
                    per_engine_cap_mb=(
                        8192//2
                        //(
                            3+4*value
                            if value<=4
                            else 3+2*value)),
                    admitted=True,
                )
                for value in range(3,14)
            ],
        ),
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
            code_worktree_clean=True,
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
        service_resources={
            "mysql":dict(
                supported=True,
                scope_stable=True,
                limits_stable=True,
                cpu_quota_cores=2.0,
                memory_limit_bytes=4*1024**3,
                mode="cgroup_v2",
                scope_fingerprint="mysql-scope",
                selection_source="listen_port",
                selection_reason="resolved",
                samples=int(elapsed),
                peak_memory_bytes=2*1024**3,
                cpu_seconds=10*3600,
                read_bytes=12*1024**3,
                write_bytes=6*1024**3,
            ),
            "starrocks_fe":dict(
                supported=True,
                scope_stable=True,
                limits_stable=True,
                cpu_quota_cores=2.0,
                memory_limit_bytes=4*1024**3,
                mode="cgroup_v2",
                scope_fingerprint="starrocks-fe-scope",
                selection_source="listen_port",
                selection_reason="resolved",
                samples=int(elapsed),
                peak_memory_bytes=1024**3,
                cpu_seconds=5*3600,
                read_bytes=2*1024**3,
                write_bytes=3*1024**3,
            ),
            "starrocks_be":dict(
                supported=True,
                scope_stable=True,
                limits_stable=True,
                cpu_quota_cores=2.0,
                memory_limit_bytes=4*1024**3,
                mode="cgroup_v2",
                scope_fingerprint="starrocks-be-scope",
                selection_source="listen_port",
                selection_reason="resolved",
                samples=int(elapsed),
                peak_memory_bytes=4*1024**3,
                cpu_seconds=20*3600,
                read_bytes=20*1024**3,
                write_bytes=30*1024**3,
            ),
        },
        live_rows=int(50*source_schedule),
        latency_samples=int(source_schedule),
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
        dynamic_task_mix="mixed",
        snapshot_rows=16_384,
        sample_seconds=1.0,
        fault_every_seconds=6*3600,
        fault_recovery_timeout_seconds=1800.0,
        seed_chunk=10_000,
        drain_timeout_seconds=1800.0,
        checkpoint_seconds=300.0,
        dynamic_task_ready_seconds=tasks,
        join_right_updates=2,
        faults=[
            dict(
                sequence=100,
                sequence_after=300,
                source_rows_during_fault=200,
                restart_seconds=4.0,
                catchup_seconds=2.0,
                join_right_update=dict(
                    bucket=0,revision=1,
                    source_rows=48829,
                    target_rows=48829,
                    fully_visible=True,
                    recovery_seconds=6.5),
                source_frontier=dict(
                    log_durable_seq=500,
                    base_applied_seq=500)),
            dict(
                sequence=200,
                sequence_after=450,
                source_rows_during_fault=250,
                restart_seconds=5.0,
                catchup_seconds=3.0,
                join_right_update=dict(
                    bucket=1,revision=2,
                    source_rows=48828,
                    target_rows=48828,
                    fully_visible=True,
                    recovery_seconds=7.0),
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
            expected_digest="source-aggregate-digest",
            all_match=True,
            tables={
                "agg_000":dict(
                    rows=1024,
                    digest="digest-agg-000",
                    match=True),
                "agg_001":dict(
                    rows=1024,
                    digest="digest-agg-001",
                    match=True),
                "agg_003":dict(
                    rows=1024,
                    digest="digest-agg-003",
                    match=True),
                "agg_005":dict(
                    rows=1024,
                    digest="digest-agg-005",
                    match=True),
                "agg_007":dict(
                    rows=1024,
                    digest="digest-agg-007",
                    match=True),
                "agg_009":dict(
                    rows=1024,
                    digest="digest-agg-009",
                    match=True),
            },
        ),
        join_checks=dict(
            expected_rows=1024,
            expected_digest="source-join-digest",
            all_match=True,
            tables={
                "join_000":dict(
                    rows=1024,
                    digest="digest-join-000",
                    match=True),
                "join_002":dict(
                    rows=1024,
                    digest="digest-join-002",
                    match=True),
                "join_004":dict(
                    rows=1024,
                    digest="digest-join-004",
                    match=True),
                "join_006":dict(
                    rows=1024,
                    digest="digest-join-006",
                    match=True),
                "join_008":dict(
                    rows=1024,
                    digest="digest-join-008",
                    match=True),
                "join_010":dict(
                    rows=1024,
                    digest="digest-join-010",
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

    full=evaluate_workload(
        workload(),
        require_profile=p11_profile.NAME)
    assert full["ok"],full
    assert (
        full["evidence"]["workload_profile"]
        ==p11_profile.NAME
    )
    assert full["evidence"]["faults"]["count"]==2
    assert full["evidence"]["faults"][
        "source_rows_during_fault"]==[200,250]
    assert full["evidence"]["faults"][
        "join_right_updates"]==[
            dict(
                bucket=0,revision=1,
                source_rows=48829,
                target_rows=48829,
                fully_visible=True,
                recovery_seconds=6.5),
            dict(
                bucket=1,revision=2,
                source_rows=48828,
                target_rows=48828,
                fully_visible=True,
                recovery_seconds=7.0),
        ]
    assert full["evidence"]["dynamic_tasks"]["ready"]==10
    assert full["evidence"]["dynamic_tasks"]["mix"]=="mixed"
    assert full["evidence"]["dynamic_tasks"]["aggregate_ready"]==5
    assert full["evidence"]["dynamic_tasks"]["join_ready"]==5
    assert full["evidence"]["aggregate_exactness"]["checked_targets"]==6
    assert full["evidence"]["join_exactness"]["checked_targets"]==6
    assert full["evidence"]["latency_over_10_seconds"]==20
    assert (
        full["evidence"]["source_schedule_seconds"]
        ==72*3600
    )
    assert (
        full["evidence"]["observed_rows_per_second"]
        ==50.0
    )
    assert (
        full["evidence"]["latency_samples_per_second"]
        ==1.0
    )
    assert (
        full["evidence"]["healthy_observation_seconds"]
        ==workload()["source_schedule_seconds"]
    )
    assert full["evidence"]["recovery_latency_samples"]==8
    assert full["evidence"]["debt"]["max_rowset"]==73
    assert full["evidence"]["resources"]["logical_cpus"]==8
    assert full["evidence"]["resources"]["configured_memory_mb"]==8192
    assert full["evidence"]["software"]["code_revision"]=="a"*40
    assert full["evidence"]["software"]["code_worktree_clean"] is True
    assert full["evidence"]["software"]["mysql_gtid_mode"]=="ON"
    assert full["evidence"]["software"]["starrocks_version"].startswith("4.1.1")
    assert full["evidence"]["daemon_resources"]["peak_rss_bytes"]==768*1024**2
    assert abs(
        full["evidence"]["daemon_resources"]["cpu_core_equivalent"]
        -(36*3600)/workload()["duration_seconds"]
    )<1e-12
    assert (
        full["evidence"]["service_resources"]["mysql"]["mode"]
        =="cgroup_v2"
    )
    assert (
        full["evidence"]["service_resources"]["mysql"][
            "limits_stable"]
    )
    assert (
        full["evidence"]["service_resources"]["mysql"][
            "cpu_quota_cores"]==2.0
    )
    assert (
        full["evidence"]["service_resources"]["mysql"][
            "memory_limit_bytes"]==4*1024**3
    )
    assert (
        full["evidence"]["pipeline_resources"]["unique_service_scopes"]
        ==3
    )
    assert abs(
        full["evidence"]["pipeline_resources"]["total_cpu_seconds"]
        -71*3600
    )<1e-12

    bad=workload()
    del bad["service_resources"]
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "service_resource_evidence_missing" in full["failures"]
    assert "mysql_resource_evidence_missing" in full["failures"]

    bad=workload()
    bad["service_resources"]["mysql"]["supported"]=False
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "mysql_resource_probe_unsupported" in full["failures"]

    bad=workload()
    bad["service_resources"]["starrocks_be"]["scope_stable"]=False
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "starrocks_be_resource_scope_unstable" in full["failures"]

    bad=workload()
    bad["service_resources"]["mysql"]["limits_stable"]=False
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "mysql_resource_limits_unstable" in full["failures"]

    bad=workload()
    bad["service_resources"]["starrocks_fe"]["scope_fingerprint"]=(
        bad["service_resources"]["mysql"]["scope_fingerprint"])
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "service_resource_scope_overlap" in full["failures"]

    bad=workload()
    value=bad["dynamic_task_ready_seconds"].pop(
        "starrocks.join_010")
    bad["dynamic_task_ready_seconds"][
        "starrocks.agg_010"]=value
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "dynamic_task_mix_mismatch" in full["failures"]

    bad=workload()
    bad["join_checks"]["tables"].pop("join_010")
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "join_targets_missing" in full["failures"]

    bad=workload()
    bad["workload_profile"]="custom"
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert "workload_profile_mismatch" in full["failures"]

    bad=workload()
    bad["memory_mb"]=4096
    bad["topology_resource_preflight"][
        "requested_memory_mb"]=4096
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert (
        "workload_profile_parameters_mismatch"
        in full["failures"]
    )
    assert full["evidence"]["profile_parameters"][
        "mismatches"]["memory_mb"]==dict(
            expected=8192,actual=4096)

    bad=workload()
    bad["dynamic_task_mix"]="aggregate"
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert (
        "workload_profile_parameters_mismatch"
        in full["failures"]
    )

    bad=workload()
    bad["topology_resource_preflight"][
        "stages"][1]["admitted"]=False
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert (
        "topology_resource_preflight_failed"
        in full["failures"]
    )

    bad=workload()
    del bad["topology_resource_preflight"]
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert (
        "topology_resource_preflight_missing"
        in full["failures"]
    )

    bad=workload()
    bad["work_directory_persistent"]=False
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert (
        "persistent_work_directory_missing"
        in full["failures"]
    )

    bad=workload()
    bad["software_fingerprint"]["code_worktree_clean"]=False
    full=evaluate_workload(
        bad,require_profile=p11_profile.NAME)
    assert not full["ok"]
    assert "code_worktree_not_clean" in full["failures"]

    compatibility=evaluate_workload(
        dict(workload(),service_resources=None),
        require_service_resources=False)
    assert compatibility["ok"],compatibility

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
    bad["daemon_resources"]["peak_rss_bytes"]=(
        bad["memory_mb"]*1024**2+1)
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "daemon_rss_exceeds_configured_memory" in full["failures"]

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
    bad["faults"][0].pop(
        "join_right_update")
    bad["join_right_updates"]=1
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert "join_right_fault_coverage" in full["failures"]

    bad=workload()
    bad.pop("join_right_updates")
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "join_right_fault_evidence_missing"
        in full["failures"]
    )

    bad=workload()
    bad["faults"][0]["join_right_update"].pop(
        "recovery_seconds")
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "join_right_fault_recovery_evidence_missing"
        in full["failures"]
    )

    bad=workload()
    bad["faults"][0]["join_right_update"][
        "target_rows"]-=1
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "join_right_fault_not_recovered"
        in full["failures"]
    )

    bad=workload()
    bad["faults"][1]["join_right_update"][
        "revision"]=3
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "join_right_fault_revision_sequence"
        in full["failures"]
    )


    bad=workload()
    bad["source_schedule_seconds"]=(
        bad["duration_seconds"]+2)
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "source_schedule_exceeds_elapsed"
        in full["failures"]
    )

    coverage=workload()
    coverage["healthy_observation_seconds"]=10
    coverage["latency_samples"]=8
    full=evaluate_workload(
        coverage,
        min_latency_samples_per_second=.5)
    assert full["ok"],full
    assert abs(
        full["evidence"][
            "latency_samples_per_second"]-.8
    )<1e-12

    bad=workload()
    bad["healthy_observation_seconds"]=(
        bad["source_schedule_seconds"]+2)
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "healthy_observation_exceeds_source_schedule"
        in full["failures"]
    )

    bad=workload()
    bad["healthy_observation_seconds"]=0
    full=evaluate_workload(bad)
    assert not full["ok"]
    assert (
        "healthy_observation_seconds"
        in full["failures"]
    )

    bad=workload()
    bad["live_rows"]=100
    bad["latency_p99_seconds"]=10.01
    bad["dynamic_task_ready_seconds"].pop(
        "starrocks.join_010")
    bad["faults"]=[]
    bad["final_state"]["pending"]=1
    bad["target_totals"]=[1,2]
    bad["aggregate_checks"]["all_match"]=False
    bad["aggregate_checks"]["tables"]["agg_009"]["match"]=False
    bad["join_checks"]["all_match"]=False
    bad["join_checks"]["tables"]["join_010"]["match"]=False
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
        "join_target_mismatch",
    ):
        assert reason in full["failures"],full

    print(
        "longhaul_gate_test ok 50m_72h 50rps lifetime_exact_p95_p99 cross_restart "
        "healthy_recovery_latency drain stateful_health space_version_debt "
        "rowset_recovery overflow_fail_closed reproducible_resource_fingerprint "
        "daemon_process_resource_evidence daemon_rss_budget source_sink_resource_evidence "
        "service_limit_drift_fail_closed scope_overlap_fail_closed exact_profile_gate "
        "clean_worktree_gate persistent_workdir_gate independent_profile_parameter_gate "
        "topology_preflight_gate "
        "software_fingerprint healthy_observation_window "
        "mixed_aggregate_join_exactness join_right_full_recovery_gate",
        flush=True,
    )


if __name__=="__main__":
    main()
