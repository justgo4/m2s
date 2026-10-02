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

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import p11_profile


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
        exact_cdc=dict(
            total.get("exact_cdc_age_seconds") or {})
        n=_integer(
            exact_cdc.get(
                "n",
                cdc.get("total_n",cdc.get("n",0))),
            table+".cdc_age_seconds.n")
        p95=cdc.get("p95")
        p99=cdc.get("p99")
        if p95 is not None:
            p95=_number(
                p95,table+".cdc_age_seconds.p95")
        if p99 is not None:
            p99=_number(
                p99,table+".cdc_age_seconds.p99")
        lag5_raw=total.get("lag_over_5")
        lag10_raw=total.get("lag_over_10")
        exact_default_thresholds=(
            float(max_p95_seconds)==5.0
            and float(max_p99_seconds)==10.0
        )
        if exact_default_thresholds:
            if lag5_raw is None or lag10_raw is None:
                failures.append(
                    table+":exact_slo_counters_missing")
                lag5=0
                lag10=0
            else:
                lag5=_integer(
                    lag5_raw,table+".lag_over_5")
                lag10=_integer(
                    lag10_raw,table+".lag_over_10")
                if lag5>n or lag10>lag5:
                    failures.append(
                        table+":invalid_slo_counters")
                if n and lag5*20>n:
                    failures.append(table+":p95")
                if n and lag10*100>n:
                    failures.append(table+":p99")
        else:
            lag5=(
                None if lag5_raw is None
                else _integer(
                    lag5_raw,table+".lag_over_5"))
            lag10=(
                None if lag10_raw is None
                else _integer(
                    lag10_raw,table+".lag_over_10"))
            if n:
                if p95 is None or p99 is None:
                    failures.append(
                        table+":recent_quantiles_missing")
                else:
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
            lag_over_5=lag5,
            lag_over_10=lag10,
            slo_scope=(
                "lifetime_exact_threshold_counts"
                if exact_default_thresholds
                else "recent_quantile_window"),
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
        require_service_resources=True,
        require_profile=None,
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
    workload_profile=str(
        report.get("workload_profile") or "")
    evidence["workload_profile"]=(
        workload_profile or None)
    if (
        require_profile is not None
        and workload_profile!=str(require_profile)
    ):
        failures.append(
            "workload_profile_mismatch")

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
    source_schedule_raw=report.get(
        "source_schedule_seconds")
    source_schedule=(
        elapsed
        if source_schedule_raw is None
        else _number(
            source_schedule_raw,
            "source_schedule_seconds")
    )
    if source_schedule<=0:
        failures.append(
            "source_schedule_seconds")
        source_schedule=elapsed
    if (
        elapsed>0
        and source_schedule>elapsed+1.0
    ):
        failures.append(
            "source_schedule_exceeds_elapsed")
    # Source generation ends at the requested workload deadline. Recovery and
    # final drain may legitimately extend wall-clock duration; counting that
    # post-source time in the throughput or sampling denominator fabricates a
    # rate regression even when every scheduled source tick was generated.
    observed_rate=(
        float(live_rows)/source_schedule
        if source_schedule>0 else 0.0)
    healthy_observation_raw=report.get(
        "healthy_observation_seconds")
    healthy_observation=(
        source_schedule
        if healthy_observation_raw is None
        else _number(
            healthy_observation_raw,
            "healthy_observation_seconds")
    )
    if healthy_observation<=0:
        failures.append(
            "healthy_observation_seconds")
        healthy_observation=source_schedule
    if (
        source_schedule>0
        and healthy_observation>source_schedule+1.0
    ):
        failures.append(
            "healthy_observation_exceeds_source_schedule")
    samples=_integer(
        report.get("latency_samples"),
        "latency_samples")
    # Healthy CDC latency excludes injected-fault recovery. Recovery latency is
    # evaluated independently through recovery_latency_* below.
    sample_rate=(
        float(samples)/healthy_observation
        if healthy_observation>0 else 0.0)
    p95=_number(
        report.get("latency_p95_seconds"),
        "latency_p95_seconds")
    p99=_number(
        report.get("latency_p99_seconds"),
        "latency_p99_seconds")
    over5=_integer(
        report.get("latency_over_5_seconds"),
        "latency_over_5_seconds")
    over10=_integer(
        report.get("latency_over_10_seconds"),
        "latency_over_10_seconds")
    recovery_samples=_integer(
        report.get("recovery_latency_samples",0),
        "recovery_latency_samples")
    recovery_p95=report.get(
        "recovery_latency_p95_seconds")
    recovery_p99=report.get(
        "recovery_latency_p99_seconds")
    recovery_max=report.get(
        "recovery_latency_max_seconds")
    if recovery_samples:
        recovery_p95=_number(
            recovery_p95,
            "recovery_latency_p95_seconds")
        recovery_p99=_number(
            recovery_p99,
            "recovery_latency_p99_seconds")
        recovery_max=_number(
            recovery_max,
            "recovery_latency_max_seconds")

    evidence.update(
        elapsed_seconds=elapsed,
        source_schedule_seconds=source_schedule,
        healthy_observation_seconds=healthy_observation,
        initial_rows=initial_rows,
        live_rows=live_rows,
        configured_rows_per_second=configured_rate,
        observed_rows_per_second=observed_rate,
        latency_samples=samples,
        latency_samples_per_second=sample_rate,
        latency_p95_seconds=p95,
        latency_p99_seconds=p99,
        latency_over_5_seconds=over5,
        latency_over_10_seconds=over10,
        recovery_latency_samples=recovery_samples,
        recovery_latency_p95_seconds=recovery_p95,
        recovery_latency_p99_seconds=recovery_p99,
        recovery_latency_max_seconds=recovery_max,
    )

    configured_memory_mb=_integer(
        report.get("memory_mb"),
        "memory_mb")
    resources=report.get("resource_fingerprint")
    if not isinstance(resources,dict) or not resources:
        failures.append("resource_fingerprint_missing")
        evidence["resources"]=dict(
            configured_memory_mb=configured_memory_mb)
    else:
        architecture=str(
            resources.get("architecture") or "").strip()
        system=str(
            resources.get("system") or "").strip()
        python_version=str(
            resources.get("python_version") or "").strip()
        logical_cpus=_integer(
            resources.get("logical_cpus",0),
            "resource_fingerprint.logical_cpus")
        cpu_quota=resources.get(
            "cgroup_cpu_quota_cores")
        if cpu_quota is not None:
            cpu_quota=_number(
                cpu_quota,
                "resource_fingerprint.cgroup_cpu_quota_cores")
        memory_limit=resources.get(
            "cgroup_memory_limit_bytes")
        if memory_limit is not None:
            memory_limit=_integer(
                memory_limit,
                "resource_fingerprint.cgroup_memory_limit_bytes")
        if (
            not architecture
            or not system
            or not python_version
            or logical_cpus<=0
        ):
            failures.append(
                "resource_fingerprint_incomplete")
        if configured_memory_mb<=0:
            failures.append(
                "configured_memory_mb")
        if (
            memory_limit is not None
            and configured_memory_mb*1024**2
                >memory_limit
        ):
            failures.append(
                "configured_memory_exceeds_cgroup")
        evidence["resources"]=dict(
            configured_memory_mb=configured_memory_mb,
            architecture=architecture,
            system=system,
            kernel_release=str(
                resources.get("kernel_release") or ""),
            python_version=python_version,
            logical_cpus=logical_cpus,
            cgroup_cpu_quota_cores=cpu_quota,
            cgroup_memory_limit_bytes=memory_limit,
        )

    software=report.get("software_fingerprint")
    if not isinstance(software,dict) or not software:
        failures.append(
            "software_fingerprint_missing")
        evidence["software"]={}
    else:
        code_revision=str(
            software.get("code_revision") or ""
        ).strip().lower()
        code_worktree_clean=software.get(
            "code_worktree_clean")
        mysql_version=str(
            software.get("mysql_version") or ""
        ).strip()
        mysql_gtid_mode=str(
            software.get("mysql_gtid_mode") or ""
        ).strip().upper()
        mysql_binlog_format=str(
            software.get("mysql_binlog_format") or ""
        ).strip().upper()
        mysql_binlog_row_image=str(
            software.get("mysql_binlog_row_image") or ""
        ).strip().upper()
        starrocks_version=str(
            software.get("starrocks_version") or ""
        ).strip()
        dependencies={
            name:str(software.get(name) or "").strip()
            for name in (
                "duckdb_version",
                "pyarrow_version",
                "pymysql_version",
                "sqlglot_version",
            )
        }
        revision_valid=(
            7<=len(code_revision)<=64
            and all(
                char in "0123456789abcdef"
                for char in code_revision)
        )
        if (
            not revision_valid
            or not mysql_version
            or mysql_gtid_mode not in {"ON","OFF"}
            or mysql_binlog_format!="ROW"
            or mysql_binlog_row_image!="FULL"
            or not starrocks_version.startswith("4.1.1")
            or any(
                not value
                for value in dependencies.values())
        ):
            failures.append(
                "software_fingerprint_incomplete")
        if (
            require_profile is not None
            and code_worktree_clean is not True
        ):
            failures.append(
                "code_worktree_not_clean")
        evidence["software"]=dict(
            code_revision=code_revision,
            code_worktree_clean=code_worktree_clean,
            mysql_version=mysql_version,
            mysql_gtid_mode=mysql_gtid_mode,
            mysql_binlog_format=mysql_binlog_format,
            mysql_binlog_row_image=mysql_binlog_row_image,
            starrocks_version=starrocks_version,
            **dependencies,
        )

    daemon_resources=report.get("daemon_resources")
    if not isinstance(daemon_resources,dict):
        failures.append(
            "daemon_resource_evidence_missing")
        evidence["daemon_resources"]={}
    else:
        daemon_supported=bool(
            daemon_resources.get("supported",False))
        daemon_samples=_integer(
            daemon_resources.get("samples",0),
            "daemon_resources.samples")
        daemon_identities=_integer(
            daemon_resources.get(
                "process_identities_seen",0),
            "daemon_resources.process_identities_seen")
        daemon_peak_processes=_integer(
            daemon_resources.get("peak_processes",0),
            "daemon_resources.peak_processes")
        daemon_peak_rss=_integer(
            daemon_resources.get("peak_rss_bytes",0),
            "daemon_resources.peak_rss_bytes")
        daemon_cpu=_number(
            daemon_resources.get("cpu_seconds",0),
            "daemon_resources.cpu_seconds")
        daemon_read=_integer(
            daemon_resources.get("read_bytes",0),
            "daemon_resources.read_bytes")
        daemon_write=_integer(
            daemon_resources.get("write_bytes",0),
            "daemon_resources.write_bytes")
        if not daemon_supported:
            failures.append(
                "daemon_resource_probe_unsupported")
        if (
            daemon_samples<=0
            or daemon_identities<=0
            or daemon_peak_processes<=0
            or daemon_peak_rss<=0
            or daemon_cpu<0
            or daemon_read<0
            or daemon_write<0
        ):
            failures.append(
                "daemon_resource_evidence_incomplete")
        memory_budget_bytes=max(
            0,configured_memory_mb)*1024**2
        if (
            configured_memory_mb>0
            and daemon_peak_rss
                >configured_memory_mb*1024**2
        ):
            failures.append(
                "daemon_rss_exceeds_configured_memory")
        evidence["daemon_resources"]=dict(
            supported=daemon_supported,
            samples=daemon_samples,
            process_identities_seen=daemon_identities,
            peak_processes=daemon_peak_processes,
            peak_rss_bytes=daemon_peak_rss,
            peak_rss_fraction_of_configured=(
                float(daemon_peak_rss)
                /memory_budget_bytes
                if memory_budget_bytes>0 else None),
            cpu_seconds=daemon_cpu,
            cpu_core_equivalent=(
                daemon_cpu/elapsed
                if elapsed>0 else None),
            read_bytes=daemon_read,
            write_bytes=daemon_write,
        )
    service_resources=report.get("service_resources")
    service_evidence={}
    service_scopes={}
    expected_services=(
        "mysql","starrocks_fe","starrocks_be")
    if not isinstance(service_resources,dict):
        if require_service_resources:
            failures.append(
                "service_resource_evidence_missing")
        service_resources={}
    for name in expected_services:
        value=service_resources.get(name)
        if not isinstance(value,dict):
            if require_service_resources:
                failures.append(
                    name+"_resource_evidence_missing")
            continue
        supported=bool(
            value.get("supported",False))
        scope_stable=bool(
            value.get("scope_stable",False))
        limits_stable=bool(
            value.get("limits_stable",False))
        cpu_quota=value.get(
            "cpu_quota_cores")
        if cpu_quota is not None:
            cpu_quota=_number(
                cpu_quota,
                name+"_resources.cpu_quota_cores")
        memory_limit=value.get(
            "memory_limit_bytes")
        if memory_limit is not None:
            memory_limit=_integer(
                memory_limit,
                name+"_resources.memory_limit_bytes")
        mode=str(value.get("mode") or "")
        scope=str(
            value.get("scope_fingerprint")
            or "").strip()
        samples_count=_integer(
            value.get("samples",0),
            name+"_resources.samples")
        peak_memory=_integer(
            value.get("peak_memory_bytes",0),
            name+"_resources.peak_memory_bytes")
        cpu_seconds=_number(
            value.get("cpu_seconds",0),
            name+"_resources.cpu_seconds")
        read_bytes=_integer(
            value.get("read_bytes",0),
            name+"_resources.read_bytes")
        write_bytes=_integer(
            value.get("write_bytes",0),
            name+"_resources.write_bytes")
        if require_service_resources:
            if not supported:
                failures.append(
                    name+"_resource_probe_unsupported")
            if not scope_stable:
                failures.append(
                    name+"_resource_scope_unstable")
            if not limits_stable:
                failures.append(
                    name+"_resource_limits_unstable")
            if (
                mode not in {
                    "cgroup_v2","process_tree"}
                or not scope
                or samples_count<=0
                or peak_memory<=0
            ):
                failures.append(
                    name+"_resource_evidence_incomplete")
        if scope:
            service_scopes[name]=scope
        service_evidence[name]=dict(
            supported=supported,
            scope_stable=scope_stable,
            limits_stable=limits_stable,
            cpu_quota_cores=cpu_quota,
            memory_limit_bytes=memory_limit,
            mode=mode,
            scope_fingerprint=scope or None,
            selection_source=str(
                value.get("selection_source") or ""),
            selection_reason=str(
                value.get("selection_reason") or ""),
            samples=samples_count,
            peak_memory_bytes=peak_memory,
            cpu_seconds=cpu_seconds,
            cpu_core_equivalent=(
                cpu_seconds/elapsed
                if elapsed>0 else None),
            read_bytes=read_bytes,
            write_bytes=write_bytes,
        )

    mysql_scope=service_scopes.get("mysql")
    sink_scopes={
        service_scopes.get(name)
        for name in (
            "starrocks_fe","starrocks_be")
        if service_scopes.get(name)
    }
    if (
        require_service_resources
        and mysql_scope
        and mysql_scope in sink_scopes
    ):
        failures.append(
            "service_resource_scope_overlap")

    unique_service_values={}
    for name,value in service_evidence.items():
        scope=value.get("scope_fingerprint")
        if scope and scope not in unique_service_values:
            unique_service_values[scope]=value
    service_cpu=sum(
        float(value.get("cpu_seconds",0))
        for value in unique_service_values.values())
    service_read=sum(
        int(value.get("read_bytes",0))
        for value in unique_service_values.values())
    service_write=sum(
        int(value.get("write_bytes",0))
        for value in unique_service_values.values())
    service_peak_memory=sum(
        int(value.get("peak_memory_bytes",0))
        for value in unique_service_values.values())
    daemon_evidence=evidence.get(
        "daemon_resources",{})
    pipeline_cpu=(
        float(daemon_evidence.get(
            "cpu_seconds",0))
        +service_cpu)
    pipeline_read=(
        int(daemon_evidence.get(
            "read_bytes",0))
        +service_read)
    pipeline_write=(
        int(daemon_evidence.get(
            "write_bytes",0))
        +service_write)
    pipeline_peak_memory_upper=(
        int(daemon_evidence.get(
            "peak_rss_bytes",0))
        +service_peak_memory)
    evidence["service_resources"]=service_evidence
    evidence["pipeline_resources"]=dict(
        unique_service_scopes=len(
            unique_service_values),
        service_cpu_seconds=service_cpu,
        service_read_bytes=service_read,
        service_write_bytes=service_write,
        service_peak_memory_upper_bound_bytes=
            service_peak_memory,
        total_cpu_seconds=pipeline_cpu,
        total_cpu_core_equivalent=(
            pipeline_cpu/elapsed
            if elapsed>0 else None),
        total_read_bytes=pipeline_read,
        total_write_bytes=pipeline_write,
        total_peak_memory_upper_bound_bytes=
            pipeline_peak_memory_upper,
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
    source_rows_during_fault=[]
    frontiers=[]
    for index,item in enumerate(faults):
        item=dict(item or {})
        restart_seconds.append(_number(
            item.get("restart_seconds"),
            "fault_%d.restart_seconds" % index))
        catchup_seconds.append(_number(
            item.get("catchup_seconds"),
            "fault_%d.catchup_seconds" % index))
        produced=_integer(
            item.get("source_rows_during_fault",0),
            "fault_%d.source_rows_during_fault" % index)
        source_rows_during_fault.append(produced)
        if produced<=0:
            failures.append("fault_source_stalled")
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
        source_rows_during_fault=source_rows_during_fault,
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
    if faults and recovery_samples<1:
        failures.append("fault_recovery_latency_missing")

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

    debt=dict(report.get("debt") or {})
    if not debt:
        failures.append("debt_evidence_missing")
    else:
        state_storage_bytes=_integer(
            debt.get("state_storage_bytes",0),
            "debt.state_storage_bytes")
        stateful_rows=_integer(
            debt.get("stateful_rows",0),
            "debt.stateful_rows")
        stateful_payload_bytes=_integer(
            debt.get("stateful_payload_bytes",0),
            "debt.stateful_payload_bytes")
        max_rowset=int(
            debt.get("max_rowset",-1))
        rowset_red=_integer(
            debt.get("rowset_red",0),
            "debt.rowset_red")
        version_recovery_active=bool(
            debt.get("version_recovery_active",False))
        pending_bytes=_integer(
            debt.get("pending_bytes",0),
            "debt.pending_bytes")
        prepared_budget_used=_integer(
            debt.get("prepared_budget_used",0),
            "debt.prepared_budget_used")
        field_overflow_rows=_integer(
            debt.get("field_overflow_rows",0),
            "debt.field_overflow_rows")
        evidence["debt"]=dict(
            state_storage_bytes=state_storage_bytes,
            stateful_rows=stateful_rows,
            stateful_payload_bytes=stateful_payload_bytes,
            max_rowset=max_rowset,
            rowset_red=rowset_red,
            per_table_max_rowset=dict(
                debt.get("per_table_max_rowset") or {}),
            per_table_version_recovery=dict(
                debt.get("per_table_version_recovery") or {}),
            version_recovery_active=version_recovery_active,
            pending_bytes=pending_bytes,
            prepared_budget_used=prepared_budget_used,
            field_overflow_rows=field_overflow_rows,
        )
        if state_storage_bytes<=0:
            failures.append("space_debt_unknown")
        if dynamic_tasks and (
            stateful_rows<=0
            or stateful_payload_bytes<=0
        ):
            failures.append(
                "stateful_space_debt_unknown")
        if max_rowset<0 or rowset_red<=0:
            failures.append("version_debt_unknown")
        elif max_rowset>=rowset_red:
            failures.append("rowset_red_exceeded")
        if version_recovery_active:
            failures.append("version_recovery_active")
        if field_overflow_rows:
            failures.append("field_overflow_rows")
        if require_drained and (
            pending_bytes
            or prepared_budget_used
        ):
            failures.append("final_debt_not_drained")

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
            require_service_resources=bool(
                require_service_resources),
            require_profile=(
                None
                if require_profile is None
                else str(require_profile)),
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
        "--require-profile",
        nargs="?",const=p11_profile.NAME,
        help=(
            "require an exact workload profile identity; "
            "without a value requires "+p11_profile.NAME))
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
            require_profile=args.require_profile,
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
