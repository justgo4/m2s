#!/usr/bin/env python3
import argparse
import base64
import bisect
import contextlib
import ctypes
import datetime
import decimal
import gzip
import hashlib
import io
import os
import pickle
import re
import resource
import select
import shutil
import signal
import sqlite3
import struct
import subprocess
import zlib
import sys
import tempfile
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import duckdb
import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pycurl
import pymysql
import sqlglot
from sqlglot import exp
import aggregate_job_bridge
import aggregate_outbox
import aggregate_shared_runtime
import aggregate_state
import aggregate_task_catalog
import aggregate_task_runner
import cdc_catalog
import incremental_contract
import incremental_ir
import join_job_bridge
import join_outbox
import join_shared_runtime
import join_state
import join_task_catalog
import join_task_runner
import physical_state_catalog
import relational_ir
import source_state
import stateful_catalog_runtime
import stateful_physical_registry
import stateful_rebuild
import stateful_share_policy
import stateful_task_plan
import task_generation

mappings = []


def log(message):
    print(f"[cdc][{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


METRIC_SAMPLE_LIMIT = 2048
METRIC_TXN_WINDOW = 2048


def metric_bucket():
    return dict(
        visible_deliveries=0,visible_rows=0,visible_bytes=0,visible_loads=0,
        visible_merge_txns=0,visible_seconds=[],
        visible_json_rows=0,visible_json_bytes=0,
        snapshot_deliveries=0,snapshot_rows=0,snapshot_rows_per_delivery=[],bundle_lanes={},
        snapshot_chunks=0,snapshot_read_rows=0,snapshot_source_rows=0,
        snapshot_read_seconds=[],
        snapshot_read_seconds_total=0.0,snapshot_admit_seconds=[],
        snapshot_admit_seconds_total=0.0,snapshot_chunk_seconds=[],
        snapshot_chunk_seconds_total=0.0,
        cdc_deliveries=0,cdc_rows=0,cdc_age=[],lag_over_10=0,
        mixed_deliveries=0,mixed_rows=0,
        merge_requests=0,merge_rows=0,merge_left_ms=[],merge_txns={},
        merge_txn_window_misses=0,merge_txn_evictions=0,
        merge_retries=0,version_pauses=0,version_recovers=0,
        metric_exact={},
    )


def metric_sample_add(bucket, name, value):
    value = float(value)
    exact = bucket["metric_exact"].setdefault(
        name,dict(count=0,total=0.0,max=None))
    exact["count"] += 1
    exact["total"] += value
    exact["max"] = value if exact["max"] is None else max(exact["max"],value)
    samples = bucket[name]
    if len(samples) >= METRIC_SAMPLE_LIMIT:
        # Quantiles are explicitly a strict recent window; lifetime avg/max stay exact.
        del samples[:len(samples)-METRIC_SAMPLE_LIMIT+1]
    samples.append(value)


def metric_merge_txn_add(bucket, txn_id):
    key = str(int(txn_id))
    txns = bucket["merge_txns"]
    previous = txns.pop(key,0)
    if not previous:
        bucket["merge_txn_window_misses"] += 1
        if len(txns) >= METRIC_TXN_WINDOW:
            del txns[next(iter(txns))]
            bucket["merge_txn_evictions"] += 1
    # Reinsert so dict order is an LRU-style recent-activity window.
    txns[key] = previous+1


def init_run_metrics(prepared):
    return dict(
        lock=threading.Lock(),
        run_id=uuid.uuid4().hex,
        started=time.time(),
        tables={
            mapping_key(mapping):dict(total=metric_bucket(),interval=metric_bucket())
            for mapping in prepared
        },
    )


def metric_quantiles(values):
    if not values:
        return dict(n=0,p50=None,p95=None,p99=None,max=None,avg=None)
    ordered = sorted(values)
    def quantile(p):
        pos = (len(ordered)-1)*p
        low,high = int(pos),min(len(ordered)-1,int(pos)+1)
        return ordered[low] if low == high else ordered[low]+(ordered[high]-ordered[low])*(pos-low)
    return dict(
        n=len(ordered),p50=quantile(0.50),p95=quantile(0.95),p99=quantile(0.99),
        max=ordered[-1],avg=sum(ordered)/len(ordered),
    )


def metric_exact_summary(bucket, name):
    exact = bucket["metric_exact"].get(name,dict(count=0,total=0.0,max=None))
    count = int(exact["count"])
    return dict(
        n=count,
        avg=(float(exact["total"])/count if count else None),
        max=exact["max"],
    )


def metric_sample_summary(bucket, name):
    result = metric_quantiles(bucket[name])
    exact = metric_exact_summary(bucket,name)
    result.update(
        scope="recent_window",window_limit=METRIC_SAMPLE_LIMIT,
        total_n=exact["n"],total_avg=exact["avg"],total_max=exact["max"],
    )
    return result


def metric_add_visible(runtime, table, kind, input_rows, byte_count, loads, merge_txns,
                       seconds, age, bundle_lanes=1, json_rows=0, json_bytes=0):
    metrics = runtime.get("metrics")
    if not metrics or table not in metrics["tables"]:
        return
    with metrics["lock"]:
        for scope in ("total","interval"):
            bucket = metrics["tables"][table][scope]
            bucket["visible_deliveries"] += 1
            bucket["visible_rows"] += int(input_rows or 0)
            bucket["visible_bytes"] += int(byte_count or 0)
            bucket["visible_loads"] += int(loads or 0)
            bucket["visible_merge_txns"] += int(merge_txns or 0)
            metric_sample_add(bucket,"visible_seconds",float(seconds))
            bucket["visible_json_rows"] += int(json_rows or 0)
            bucket["visible_json_bytes"] += int(json_bytes or 0)
            kinds = {part.strip() for part in str(kind or "").split(",") if part.strip()}
            if kinds == {"snapshot"}:
                bucket["snapshot_deliveries"] += 1
                bucket["snapshot_rows"] += int(input_rows or 0)
                metric_sample_add(bucket,"snapshot_rows_per_delivery",int(input_rows or 0))
                key = str(int(bundle_lanes or 1))
                bucket["bundle_lanes"][key] = bucket["bundle_lanes"].get(key,0)+1
            elif kinds == {"cdc"}:
                bucket["cdc_deliveries"] += 1
                bucket["cdc_rows"] += int(input_rows or 0)
                metric_sample_add(bucket,"cdc_age",float(age or 0))
                if float(age or 0) > 10:
                    bucket["lag_over_10"] += 1
            else:
                bucket["mixed_deliveries"] += 1
                bucket["mixed_rows"] += int(input_rows or 0)


def metric_add_snapshot_chunk(runtime, table, rows, read_seconds, admit_seconds, chunk_seconds,
                              source_rows=None):
    metrics = runtime.get("metrics")
    if not metrics or table not in metrics["tables"]:
        return
    with metrics["lock"]:
        for scope in ("total","interval"):
            bucket = metrics["tables"][table][scope]
            bucket["snapshot_chunks"] += 1
            bucket["snapshot_read_rows"] += int(rows or 0)
            bucket["snapshot_source_rows"] += int(
                rows if source_rows is None else source_rows or 0)
            bucket["snapshot_read_seconds_total"] += float(read_seconds or 0)
            bucket["snapshot_admit_seconds_total"] += float(admit_seconds or 0)
            bucket["snapshot_chunk_seconds_total"] += float(chunk_seconds or 0)
            metric_sample_add(bucket,"snapshot_read_seconds",float(read_seconds or 0))
            metric_sample_add(bucket,"snapshot_admit_seconds",float(admit_seconds or 0))
            metric_sample_add(bucket,"snapshot_chunk_seconds",float(chunk_seconds or 0))


def metric_add_merge(runtime, table, txn_id, rows, left_merge_ms):
    metrics = runtime.get("metrics")
    if not metrics or table not in metrics["tables"]:
        return
    with metrics["lock"]:
        for scope in ("total","interval"):
            bucket = metrics["tables"][table][scope]
            bucket["merge_requests"] += 1
            bucket["merge_rows"] += int(rows or 0)
            metric_sample_add(bucket,"merge_left_ms",int(left_merge_ms or 0))
            metric_merge_txn_add(bucket,txn_id)


def metric_increment(runtime, table, name, amount=1):
    metrics = runtime.get("metrics")
    if not metrics or table not in metrics["tables"]:
        return
    with metrics["lock"]:
        for scope in ("total","interval"):
            bucket = metrics["tables"][table][scope]
            bucket[name] = int(bucket.get(name,0))+int(amount)


def metric_bucket_summary(bucket, include_quantiles=True):
    tx_counts = list(bucket["merge_txns"].values()) if include_quantiles else None
    snapshot_read_total = float(bucket["snapshot_read_seconds_total"])
    snapshot_admit_total = float(bucket["snapshot_admit_seconds_total"])
    snapshot_chunk_total = float(bucket["snapshot_chunk_seconds_total"])
    result = dict(
        visible_deliveries=bucket["visible_deliveries"],
        visible_rows=bucket["visible_rows"],
        visible_bytes=bucket["visible_bytes"],
        visible_loads=bucket["visible_loads"],
        visible_merge_txns=bucket["visible_merge_txns"],
        visible_json_rows=bucket["visible_json_rows"],
        visible_json_bytes=bucket["visible_json_bytes"],
        avg_json_row_bytes=(
            bucket["visible_json_bytes"]/bucket["visible_json_rows"]
            if bucket["visible_json_rows"] else None
        ),
        snapshot_deliveries=bucket["snapshot_deliveries"],
        snapshot_rows=bucket["snapshot_rows"],
        snapshot_chunks=bucket["snapshot_chunks"],
        snapshot_read_rows=bucket["snapshot_read_rows"],
        snapshot_source_rows=bucket["snapshot_source_rows"],
        snapshot_read_amplification=(
            bucket["snapshot_source_rows"]/bucket["snapshot_read_rows"]
            if bucket["snapshot_read_rows"] else None),
        snapshot_read_seconds_total=snapshot_read_total,
        snapshot_admit_seconds_total=snapshot_admit_total,
        snapshot_chunk_seconds_total=snapshot_chunk_total,
        snapshot_read_rows_per_second=(
            bucket["snapshot_read_rows"]/snapshot_read_total if snapshot_read_total else None),
        snapshot_admit_rows_per_second=(
            bucket["snapshot_read_rows"]/snapshot_admit_total if snapshot_admit_total else None),
        snapshot_chunk_rows_per_second=(
            bucket["snapshot_read_rows"]/snapshot_chunk_total if snapshot_chunk_total else None),
        cdc_deliveries=bucket["cdc_deliveries"],
        cdc_rows=bucket["cdc_rows"],
        lag_over_10=bucket["lag_over_10"],
        mixed_deliveries=bucket["mixed_deliveries"],
        mixed_rows=bucket["mixed_rows"],
        merge_requests=bucket["merge_requests"],
        merge_rows=bucket["merge_rows"],
        merge_distinct_txns=len(bucket["merge_txns"]),
        merge_distinct_txns_scope="recent_window",
        merge_txn_window=len(bucket["merge_txns"]),
        merge_txn_window_misses=bucket["merge_txn_window_misses"],
        merge_txn_evictions=bucket["merge_txn_evictions"],
        metric_sample_limit=METRIC_SAMPLE_LIMIT,
        exact_visible_seconds=metric_exact_summary(bucket,"visible_seconds"),
        exact_cdc_age_seconds=metric_exact_summary(bucket,"cdc_age"),
        exact_snapshot_rows_per_delivery=metric_exact_summary(
            bucket,"snapshot_rows_per_delivery"),
        exact_snapshot_read_seconds=metric_exact_summary(
            bucket,"snapshot_read_seconds"),
        exact_snapshot_admit_seconds=metric_exact_summary(
            bucket,"snapshot_admit_seconds"),
        exact_snapshot_chunk_seconds=metric_exact_summary(
            bucket,"snapshot_chunk_seconds"),
        exact_merge_left_ms=metric_exact_summary(bucket,"merge_left_ms"),
        bundle_lanes=dict(bucket["bundle_lanes"]),
        merge_retries=bucket["merge_retries"],
        version_pauses=bucket["version_pauses"],
        version_recovers=bucket["version_recovers"],
    )
    if include_quantiles:
        requests_per_merge_txn = metric_quantiles(tx_counts)
        requests_per_merge_txn.update(
            scope="recent_txn_window",window_limit=METRIC_TXN_WINDOW)
        result.update(
            visible_seconds=metric_sample_summary(bucket,"visible_seconds"),
            cdc_age_seconds=metric_sample_summary(bucket,"cdc_age"),
            snapshot_rows_per_delivery=metric_sample_summary(
                bucket,"snapshot_rows_per_delivery"),
            snapshot_read_seconds=metric_sample_summary(bucket,"snapshot_read_seconds"),
            snapshot_admit_seconds=metric_sample_summary(bucket,"snapshot_admit_seconds"),
            snapshot_chunk_seconds=metric_sample_summary(bucket,"snapshot_chunk_seconds"),
            merge_left_ms=metric_sample_summary(bucket,"merge_left_ms"),
            requests_per_merge_txn=requests_per_merge_txn,
        )
    return result


def report_paths(cfg):
    return cfg["state"]+".metrics.jsonl",cfg["state"]+".summary.json"


def append_report(path, payload, max_bytes=64*1024*1024):
    data = orjson.dumps(payload,option=orjson.OPT_SORT_KEYS)+b"\n"
    try:
        size = os.path.getsize(path)
    except FileNotFoundError:
        size = 0
    if max_bytes > 0 and size and size+len(data) > int(max_bytes):
        old = path+".1"
        with contextlib.suppress(FileNotFoundError):
            os.unlink(old)
        os.replace(path,old)
    with open(path,"ab") as handle:
        handle.write(data)
        handle.flush()


def write_summary(path, payload):
    tmp = path+".tmp"
    with open(tmp,"wb") as handle:
        handle.write(orjson.dumps(payload,option=orjson.OPT_SORT_KEYS|orjson.OPT_INDENT_2))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp,path)


def metrics_status_record(runtime, cfg, prepared, state):
    metrics = runtime["metrics"]
    now = time.time()
    with metrics["lock"]:
        tables = {}
        for mapping in prepared:
            table = mapping_key(mapping)
            interval = metrics["tables"][table]["interval"]
            total = metrics["tables"][table]["total"]
            tables[table] = dict(
                interval=metric_bucket_summary(interval),
                total=metric_bucket_summary(total,include_quantiles=False),
                active_writers=writer_target(runtime,table) if cfg["load_mode"] == "merge_async" else 1,
                max_rowset=runtime["max_rowset"].get(table,-1),
                version_recovery=bool(runtime["version_recovery"].get(table,False)),
            )
            metrics["tables"][table]["interval"] = metric_bucket()
        record = dict(
            event="metrics",run_id=metrics["run_id"],timestamp=now,
            elapsed_seconds=now-metrics["started"],state=state,
            source_reader=cfg.get("source_reader","native_c_v1"),tables=tables,
        )
    metrics_path,summary_path = report_paths(cfg)
    append_report(metrics_path,record,cfg.get("metrics_max_bytes",64*1024*1024))
    write_summary(summary_path,record)
    log("METRICS "+orjson.dumps(record,option=orjson.OPT_SORT_KEYS).decode())
    return record


def final_run_summary(runtime, cfg, prepared, con, reason):
    metrics = runtime["metrics"]
    now = time.time()
    with metrics["lock"]:
        tables = {
            mapping_key(mapping):dict(
                total=metric_bucket_summary(metrics["tables"][mapping_key(mapping)]["total"]),
                active_writers=writer_target(runtime,mapping_key(mapping)) if cfg["load_mode"] == "merge_async" else 1,
                max_rowset=runtime["max_rowset"].get(mapping_key(mapping),-1),
                version_recovery=bool(runtime["version_recovery"].get(mapping_key(mapping),False)),
            )
            for mapping in prepared
        }
    pending = con.execute(
        "SELECT COUNT(*),COALESCE(SUM(logical_bytes),0),MIN(created) FROM active_jobs").fetchone()
    done = con.execute("SELECT COUNT(*) FROM table_state WHERE snapshot_done=1").fetchone()[0]
    inflight = con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    prepared_bytes = con.execute(
        "SELECT COALESCE(SUM(length(payload)),0) FROM load_parts WHERE visible=0"
    ).fetchone()[0]
    reserved_bytes = con.execute(
        "SELECT COALESCE(SUM(reserved_bytes),0) FROM prepare_reservations"
    ).fetchone()[0]
    requirement_state = con.execute("""
        SELECT COUNT(*),COALESCE(SUM(required_bytes),0),
               COALESCE(SUM(full_prepare_attempts),0)
        FROM prepare_requirements WHERE required_bytes IS NOT NULL
    """).fetchone()
    overflows = con.execute("SELECT COUNT(*) FROM field_overflow").fetchone()[0]
    uncertain = con.execute("SELECT COUNT(*) FROM merge_uncertain").fetchone()[0]
    read = meta_get(con,"read_position")
    summary = dict(
        event="run_summary",run_id=metrics["run_id"],timestamp=now,
        started=metrics["started"],elapsed_seconds=now-metrics["started"],reason=reason,
        protocol=cfg["load_mode"],source_reader=cfg.get("source_reader","native_c_v1"),
        tables=tables,
        state=dict(
            snapshot_done=done,snapshot_tables=len(prepared),pending_jobs=pending[0],
            pending_bytes=pending[1],prepared_bytes=prepared_bytes,
            prepare_reserved_bytes=reserved_bytes,
            prepare_known=requirement_state[0],
            prepare_required_bytes=requirement_state[1],
            prepare_full_attempts=requirement_state[2],
            prepared_budget_used=prepared_bytes+reserved_bytes,inflight=inflight,
            oldest_queue_seconds=max(0,now-pending[2]) if pending[2] is not None else 0,
            durable_position=f"{read[0]}:{read[1]}",field_overflow_rows=overflows,
            merge_uncertain_rows=uncertain,errors=list(runtime.get("errors",[])),
            catalog_activation=dict(runtime.get("catalog_activation",{})),
            quarantined_tables=dict(runtime.get("quarantined_tables",{})),
            health="degraded" if runtime.get("quarantined_tables") else "normal",
        ),
    )
    metrics_path,summary_path = report_paths(cfg)
    append_report(metrics_path,summary,cfg.get("metrics_max_bytes",64*1024*1024))
    write_summary(summary_path,summary)
    log("RUN SUMMARY "+orjson.dumps(summary,option=orjson.OPT_SORT_KEYS).decode())
    return summary


_CATALOG_VARIABLES = None
_CATALOG_VARIABLE_CONTEXT = threading.local()


@contextlib.contextmanager
def catalog_variable_scope(variables):
    previous = getattr(_CATALOG_VARIABLE_CONTEXT,"variables",None)
    _CATALOG_VARIABLE_CONTEXT.variables = {
        str(name):str(value) for name,value in (variables or {}).items()}
    try:
        yield
    finally:
        if previous is None:
            with contextlib.suppress(AttributeError):
                delattr(_CATALOG_VARIABLE_CONTEXT,"variables")
        else:
            _CATALOG_VARIABLE_CONTEXT.variables = previous


def catalog_variables():
    global _CATALOG_VARIABLES
    override = getattr(_CATALOG_VARIABLE_CONTEXT,"variables",None)
    if override is not None:
        return override
    if _CATALOG_VARIABLES is None:
        paths = cdc_catalog.catalog_paths(__file__)
        _CATALOG_VARIABLES = cdc_catalog.variables_get(paths["catalog"])
    return _CATALOG_VARIABLES


def env(name, default=None, required=False):
    variables = catalog_variables()
    release_override = (
        str(os.environ.get("CDC_RELEASE_ISOLATED","")).strip().lower()
        in ("1","true","yes","on")
        or str(os.environ.get("CDC_RELEASE_RESOURCE_FROZEN","")).strip().lower()
        in ("1","true","yes","on"))
    if release_override and name in os.environ:
        # Internal certification workers must be able to redirect state/resource
        # paths away from production while still reading DB credentials from the
        # persistent catalog.
        value = os.environ[name]
    elif name in variables:
        value = variables[name]
    elif name in os.environ:
        # Compatibility fallback for unset non-database options.
        value = os.environ[name]
    else:
        value = default
    if required and value is None:
        raise ValueError(
            f"missing persistent configuration: {name}; configure it with "
            "'python j4.py cli' or 'python j4.py sql <file.sql>'")
    return value


def env_int(name, default, minimum=1, maximum=2**63-1):
    value = int(env(name, str(default)))
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def env_bool(name, default=False):
    value = str(env(name, "true" if default else "false")).strip().lower()
    if value not in ("1","0","true","false","yes","no","on","off"):
        raise ValueError(f"{name} must be true or false")
    return value in ("1","true","yes","on")


_RESOURCE_POLICY_APPLIED = None


def available_cpu_ids():
    if hasattr(os, "sched_getaffinity"):
        try:
            cpus = sorted(os.sched_getaffinity(0))
            if cpus:
                return cpus
        except OSError:
            pass
    return list(range(max(1, os.cpu_count() or 1)))


def cgroup_cpu_quota_cores():
    try:
        value = open("/sys/fs/cgroup/cpu.max", "r", encoding="ascii").read().strip()
        quota,period = value.split()
        if quota != "max":
            return max(1,int(int(quota)/int(period)))
    except (OSError,ValueError,ZeroDivisionError):
        pass
    try:
        quota = int(open(
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us","r",encoding="ascii").read().strip())
        period = int(open(
            "/sys/fs/cgroup/cpu/cpu.cfs_period_us","r",encoding="ascii").read().strip())
        if quota > 0 and period > 0:
            return max(1,int(quota/period))
    except (OSError,ValueError,ZeroDivisionError):
        pass
    return None


def system_memory_stats():
    values = {}
    try:
        with open("/proc/meminfo","r",encoding="ascii") as handle:
            for line in handle:
                parts = line.split()
                if len(parts) >= 2 and parts[0].rstrip(":") in (
                    "MemTotal","MemAvailable","SwapTotal","SwapFree"):
                    values[parts[0].rstrip(":")] = int(parts[1])//1024
    except (OSError,ValueError):
        pass

    total_known = "MemTotal" in values
    available_known = "MemAvailable" in values
    total = int(values.get("MemTotal",0))
    available = int(values.get("MemAvailable",0))
    swap_total = int(values.get("SwapTotal",0))
    swap_free = int(values.get("SwapFree",0))
    cgroup_max = None
    cgroup_current = None

    for path in (
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ):
        try:
            raw = open(path,"r",encoding="ascii").read().strip()
            if raw and raw != "max":
                amount = int(raw)
                if 64*1024**2 <= amount < 1<<60:
                    cgroup_max = amount//1024**2
                    break
        except (OSError,ValueError):
            pass

    for path in (
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    ):
        try:
            cgroup_current = int(open(path,"r",encoding="ascii").read().strip())//1024**2
            break
        except (OSError,ValueError):
            pass

    if cgroup_max:
        total = min(total,cgroup_max) if total_known else cgroup_max
        total_known = True
        if cgroup_current is not None:
            cgroup_available = max(0,cgroup_max-cgroup_current)
            available = min(available,cgroup_available) if available_known else cgroup_available
            available_known = True

    return dict(
        total_mb=total,
        available_mb=available,
        used_mb=max(0,total-available) if total_known and available_known else None,
        total_known=total_known,
        available_known=available_known,
        swap_total_mb=swap_total,
        swap_used_mb=max(0,swap_total-swap_free),
        cgroup_max_mb=cgroup_max,
        cgroup_current_mb=cgroup_current,
    )


def system_memory_mb():
    return system_memory_stats()["total_mb"]


def process_tree_rss_stats():
    root = os.getpid()
    children = {}
    rss = {}
    try:
        entries = list(os.scandir("/proc"))
    except OSError:
        return dict(known=False,rss_mb=0.0,processes=0)
    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        ppid = None
        rss_kb = None
        try:
            with open(os.path.join(entry.path,"status"),"r",encoding="ascii") as handle:
                for line in handle:
                    if line.startswith("PPid:"):
                        ppid = int(line.split()[1])
                    elif line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
        except (OSError,ValueError):
            continue
        if ppid is not None:
            children.setdefault(ppid,[]).append(pid)
        if rss_kb is not None:
            rss[pid] = rss_kb
    if root not in rss:
        return dict(known=False,rss_mb=0.0,processes=0)
    stack = [root]
    seen = set()
    total_kb = 0
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        total_kb += rss.get(pid,0)
        stack.extend(children.get(pid,()))
    return dict(known=True,rss_mb=total_kb/1024.0,processes=len(seen))


def process_vms_mb():
    try:
        with open("/proc/self/status","r",encoding="ascii") as handle:
            for line in handle:
                if line.startswith("VmSize:"):
                    return int(line.split()[1])/1024
    except (OSError,ValueError):
        pass
    return 0.0


def starrocks_be_running():
    # Diagnostic only. Resource allocation never depends on a process name.
    try:
        entries = os.scandir("/proc")
    except OSError:
        return False
    with entries:
        for entry in entries:
            if not entry.name.isdigit() or int(entry.name) == os.getpid():
                continue
            text = ""
            for leaf in ("comm","cmdline"):
                try:
                    data = open(os.path.join(entry.path,leaf),"rb").read(4096)
                except OSError:
                    continue
                text += " "+data.replace(b"\0",b" ").decode("utf-8","ignore").lower()
            if (
                "starrocks_be" in text
                or "starrocks-be" in text
                or ("/be/bin/" in text and "starrocks" in text)
            ):
                return True
    return False


def resource_auto_defaults(cpu_count, memory_total_mb, memory_available_mb, load1):
    cpu_count = max(1,int(cpu_count or 1))
    memory_total_mb = max(0,int(memory_total_mb or 0))
    memory_available_mb = max(0,int(memory_available_mb or 0))
    load1 = max(0.0,float(load1 or 0.0))

    cpu_cap = max(1,min(8,cpu_count//2 if cpu_count > 1 else 1))
    cpu_reserve = max(1,(cpu_count+3)//4)
    cpu_headroom = max(1,int(cpu_count-load1-cpu_reserve))
    cpu_target = max(1,min(cpu_cap,cpu_headroom))

    if memory_total_mb:
        memory_reserve_mb = max(2048,memory_total_mb//4)
        memory_cap_mb = max(512,min(8192,memory_total_mb//8))
        memory_headroom_mb = max(0,memory_available_mb-memory_reserve_mb)
        memory_mb = max(512,min(memory_cap_mb,max(512,memory_headroom_mb//4)))
    else:
        memory_reserve_mb = 2048
        memory_cap_mb = 4096
        memory_mb = 2048

    return dict(
        cpu_cap=cpu_cap,
        cpu_target=cpu_target,
        cpu_reserve_cores=cpu_reserve,
        memory_mb=memory_mb,
        memory_cap_mb=memory_cap_mb,
        memory_reserve_mb=memory_reserve_mb,
        nice=5,
        ionice="best_effort",
    )


def disk_reserve_bytes(path):
    probe = os.path.abspath(os.path.dirname(path) or ".")
    while not os.path.exists(probe):
        parent = os.path.dirname(probe)
        if parent == probe:
            break
        probe = parent
    try:
        usage = shutil.disk_usage(probe)
    except OSError:
        return 4*1024**3
    return min(128*1024**3,max(4*1024**3,usage.total//20))


def read_resource_policy():
    cpu_ids = available_cpu_ids()
    affinity_count = max(1,len(cpu_ids))
    quota = cgroup_cpu_quota_cores()
    effective_count = min(affinity_count,quota) if quota else affinity_count
    frozen = os.environ.get("CDC_RELEASE_RESOURCE_FROZEN") == "1"
    frozen_detected = str(os.environ.get(
        "CDC_RELEASE_DETECTED_CPU_COUNT","")).strip() if frozen else ""
    # Capacity is captured before this program applies its own affinity. Release
    # children keep that baseline even if they inherit the parent's narrower mask.
    cpu_count = (
        max(1,int(frozen_detected)) if frozen_detected
        else max(1,int(effective_count))
    )
    memory = system_memory_stats()
    try:
        load1 = float(os.getloadavg()[0])
    except (AttributeError,OSError):
        load1 = 0.0
    defaults = resource_auto_defaults(
        cpu_count,memory["total_mb"],memory["available_mb"],load1)

    frozen_cpu_cap = str(os.environ.get(
        "CDC_RELEASE_CPU_CAP","")).strip() if frozen else ""
    if frozen_cpu_cap:
        cpu_cap = min(max(1,int(frozen_cpu_cap)),effective_count)
        cpu_source = str(os.environ.get(
            "CDC_RELEASE_CPU_SOURCE","auto")).strip().lower()
        if cpu_source not in ("auto","manual"):
            raise ValueError("invalid frozen CPU budget source")
    else:
        cpu_raw = str(env("CDC_RESOURCE_CPU_CORES","auto")).strip().lower()
        if cpu_raw in ("","auto"):
            cpu_cap = defaults["cpu_cap"]
            cpu_source = "auto"
        else:
            cpu_cap = int(cpu_raw)
            if cpu_cap < 1:
                raise ValueError("CDC_RESOURCE_CPU_CORES must be auto or a positive integer")
            cpu_cap = min(cpu_cap,effective_count)
            cpu_source = "manual"
    target_raw = (
        str(os.environ.get("CDC_RELEASE_CPU_TARGET","")).strip().lower()
        if frozen else ""
    )
    if target_raw:
        cpu_target = int(target_raw)
        if cpu_target < 1:
            raise ValueError("CDC_RESOURCE_CPU_TARGET must be a positive integer")
        cpu_target = min(cpu_cap,cpu_target)
    else:
        cpu_target = min(cpu_cap,defaults["cpu_target"])

    frozen_memory = str(os.environ.get(
        "CDC_RELEASE_MEMORY_MB","")).strip() if frozen else ""
    if frozen_memory:
        memory_mb = int(frozen_memory)
        if memory_mb < 512:
            raise ValueError("frozen CDC memory budget must be >= 512")
        if memory["total_mb"]:
            memory_mb = min(memory_mb,memory["total_mb"])
        memory_source = str(os.environ.get(
            "CDC_RELEASE_MEMORY_SOURCE","auto")).strip().lower()
        if memory_source not in ("auto","manual"):
            raise ValueError("invalid frozen memory budget source")
    else:
        memory_raw = str(env("CDC_RESOURCE_MEMORY_MB","auto")).strip().lower()
        if memory_raw in ("","auto"):
            memory_mb = defaults["memory_mb"]
            memory_source = "auto"
        else:
            memory_mb = int(memory_raw)
            if memory_mb < 512:
                raise ValueError("CDC_RESOURCE_MEMORY_MB must be auto or >= 512")
            if memory["total_mb"]:
                memory_mb = min(memory_mb,memory["total_mb"])
            memory_source = "manual"

    nice_raw = str(env("CDC_RESOURCE_NICE","auto")).strip().lower()
    nice = defaults["nice"] if nice_raw in ("","auto") else int(nice_raw)
    if not 0 <= nice <= 19:
        raise ValueError("CDC_RESOURCE_NICE must be auto or between 0 and 19")

    ionice = str(env("CDC_RESOURCE_IONICE","auto")).strip().lower()
    if ionice in ("","auto"):
        ionice = defaults["ionice"]
    if ionice not in ("off","idle","best_effort"):
        raise ValueError("CDC_RESOURCE_IONICE must be auto, off, idle or best_effort")

    return dict(
        cpu_ids=cpu_ids,
        detected_cpu_count=cpu_count,
        cpu_capacity_count=cpu_count,
        detected_affinity_cpus=affinity_count,
        detected_effective_cpu_count=effective_count,
        detected_cpu_quota=quota,
        detected_memory_mb=memory["total_mb"],
        detected_memory_available_mb=memory["available_mb"],
        detected_memory_used_mb=memory["used_mb"],
        detected_swap_total_mb=memory["swap_total_mb"],
        detected_swap_used_mb=memory["swap_used_mb"],
        detected_starrocks_be=starrocks_be_running(),
        detected_load1=load1,
        cpu_cap=cpu_cap,
        cpu_target=cpu_target,
        cpu_reserve_cores=(
            max(1,int(os.environ["CDC_RELEASE_CPU_RESERVE_CORES"]))
            if frozen and os.environ.get("CDC_RELEASE_CPU_RESERVE_CORES")
            else defaults["cpu_reserve_cores"]),
        cpu_source=cpu_source,
        memory_mb=memory_mb,
        memory_cap_mb=defaults["memory_cap_mb"],
        memory_reserve_mb=defaults["memory_reserve_mb"],
        memory_source=memory_source,
        nice=nice,
        ionice=ionice,
        legacy_shared_host=env("CDC_RESOURCE_SHARED_HOST"),
    )


def apply_resource_policy(policy):
    global _RESOURCE_POLICY_APPLIED
    current_ids = available_cpu_ids()
    cpu_cap = min(int(policy["cpu_cap"]),len(current_ids))
    selected = tuple(current_ids[:cpu_cap])
    memory_mb = int(policy["memory_mb"])
    key = (selected,memory_mb,int(policy["nice"]),policy["ionice"])
    if _RESOURCE_POLICY_APPLIED == key:
        return policy

    if hasattr(os,"sched_setaffinity"):
        try:
            os.sched_setaffinity(0,set(selected))
        except OSError as exc:
            log(f"RESOURCE affinity warning={exc}")

    try:
        pa.set_cpu_count(cpu_cap)
        pa.set_io_thread_count(max(1,min(2,cpu_cap)))
    except (AttributeError,ValueError):
        pass

    vms_mb = process_vms_mb()
    # RLIMIT_AS counts reserved address space, not resident memory. Python/C
    # worker stacks, allocator arenas and mmap can reserve gigabytes without
    # consuming the logical RSS budget. Leave virtual headroom for startup;
    # process-tree RSS monitoring and engine budgets enforce resident usage.
    virtual_headroom_mb = max(4096,memory_mb*2)
    rlimit_mb = max(memory_mb*4,int(vms_mb or 0)+virtual_headroom_mb)
    policy["memory_address_space_mb"] = rlimit_mb
    log(
        "RESOURCE address_space_limit_mb=%d rss_budget_mb=%d current_vms_mb=%.0f "
        "virtual_reservation_is_not_rss=1"
        % (rlimit_mb,memory_mb,vms_mb or 0)
    )

    if hasattr(resource,"RLIMIT_AS"):
        requested = rlimit_mb*1024**2
        soft,hard = resource.getrlimit(resource.RLIMIT_AS)
        target = requested
        if soft != resource.RLIM_INFINITY:
            target = min(target,soft)
        if hard != resource.RLIM_INFINITY:
            target = min(target,hard)
        if vms_mb and target <= int(vms_mb*1024**2):
            raise RuntimeError("existing address-space limit is below current CDC process size")
        resource.setrlimit(resource.RLIMIT_AS,(target,hard))
        policy["memory_applied_mb"] = target//1024**2
    else:
        policy["memory_applied_mb"] = None

    try:
        current_nice = os.getpriority(os.PRIO_PROCESS,0)
        if int(policy["nice"]) > current_nice:
            os.nice(int(policy["nice"])-current_nice)
    except (AttributeError,OSError):
        pass

    ionice_applied = False
    ionice_path = shutil.which("ionice")
    if policy["ionice"] != "off" and ionice_path:
        args = [ionice_path]
        if policy["ionice"] == "idle":
            args += ["-c","3"]
        else:
            args += ["-c","2","-n","7"]
        args += ["-p",str(os.getpid())]
        result = subprocess.run(args,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        ionice_applied = result.returncode == 0
    policy["ionice_applied"] = ionice_applied

    _RESOURCE_POLICY_APPLIED = key
    log(
        "RESOURCE LIMIT cpu_cap=%d/%d cpu_target=%d load1=%.2f cpu_source=%s "
        "memory_mb=%d/%s memory_available_mb=%s memory_source=%s "
        "swap_total_mb=%d swap_used_mb=%d swap_policy=ignored_never_enabled "
        "starrocks_be=%d diagnostic_only=1 nice=%d ionice=%s ionice_applied=%d"
        % (
            cpu_cap,policy["detected_cpu_count"],policy["cpu_target"],
            policy["detected_load1"],policy["cpu_source"],memory_mb,
            policy["detected_memory_mb"] or "unknown",
            policy["detected_memory_available_mb"] or "unknown",
            policy["memory_source"],policy["detected_swap_total_mb"],
            policy["detected_swap_used_mb"],int(policy["detected_starrocks_be"]),
            int(policy["nice"]),policy["ionice"],int(ionice_applied),
        )
    )
    if policy.get("legacy_shared_host") not in (None,"","auto"):
        log("RESOURCE CDC_RESOURCE_SHARED_HOST is deprecated and ignored; "
            "allocation is based on measured headroom")
    return policy


def memory_limit_bytes(value):
    text = str(value).strip().lower()
    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?)\s*(b|kb|kib|mb|mib|gb|gib|tb|tib)?",
        text)
    if not match:
        raise ValueError(
            "CDC_DUCKDB_MEMORY must be a fixed size such as 128MB, 1GB or 512MiB")
    number = float(match.group(1))
    if number <= 0:
        raise ValueError("CDC_DUCKDB_MEMORY must be positive")
    unit = match.group(2) or "b"
    scale = {
        "b":1,
        "kb":1024,"kib":1024,
        "mb":1024**2,"mib":1024**2,
        "gb":1024**3,"gib":1024**3,
        "tb":1024**4,"tib":1024**4,
    }[unit]
    return max(1,int(number*scale))


def duckdb_engine_slots(cfg, table_count=1):
    table_count = max(1,int(table_count or 1))
    if cfg.get("load_mode") == "transaction":
        loader_slots = table_count
    else:
        loader_slots = max(1,int(cfg["writer_max"]))*table_count
    # A loader can temporarily retain old+new plan engines while old durable
    # jobs drain. Reader routing uses one engine and active snapshot workers each
    # use one more.
    return max(
        1,
        1
        + max(0,int(cfg.get("snapshot_workers",0)))
        + loader_slots*2,
    )


def duckdb_merge_writer_cap(cfg, table_count=1, requested_bytes=None):
    table_count = max(1,int(table_count or 1))
    if cfg.get("load_mode") != "merge_async":
        return max(1,int(cfg.get("writer_max",1)))
    memory_bytes = int(cfg["resource"]["memory_mb"])*1024**2
    duckdb_budget = max(1,memory_bytes//2)
    if requested_bytes is None:
        requested_text = cfg.get(
            "_duckdb_memory_requested",str(cfg.get("duckdb_memory","256MB")))
        requested_bytes = memory_limit_bytes(requested_text)
    fixed_slots = 1 + max(0,int(cfg.get("snapshot_workers",0)))
    affordable_slots = max(1,duckdb_budget//max(1,int(requested_bytes)))
    return max(
        1,(affordable_slots-fixed_slots)//max(1,2*table_count))


def enforce_resource_config(cfg, table_count=1):
    policy = cfg["resource"]
    cpu_cap = max(1,int(policy["cpu_cap"]))
    cpu_target = max(1,min(cpu_cap,int(policy["cpu_target"])))
    table_count = max(1,int(table_count or 1))
    per_table_writer_cap = max(1,cpu_cap//table_count)
    per_table_writer_initial = max(1,cpu_target//table_count)
    cfg["writer_max"] = min(cfg["writer_max"],per_table_writer_cap)
    cfg["writer_initial"] = min(cfg["writer_initial"],cfg["writer_max"],per_table_writer_initial)
    cfg["writer_min"] = min(cfg["writer_min"],cfg["writer_initial"])
    snapshot_cap = max(1,min(2,cpu_target//2 if cpu_target > 1 else 1))
    cfg["snapshot_workers"] = min(cfg["snapshot_workers"],snapshot_cap)
    merge_cap = max(1,min(2,cpu_target//2 if cpu_target > 1 else 1))
    cfg["merge_commit_parallel"] = min(cfg["merge_commit_parallel"],merge_cap)
    cfg["max_inflight_deliveries"] = min(
        cfg["max_inflight_deliveries"],max(2,cpu_cap*2))
    memory_bytes = int(policy["memory_mb"])*1024**2
    cfg["max_prepared_bytes"] = min(
        cfg["max_prepared_bytes"],max(64*1024**2,memory_bytes//8))

    requested_text = cfg.setdefault(
        "_duckdb_memory_requested",str(cfg["duckdb_memory"]))
    requested_bytes = memory_limit_bytes(requested_text)
    # Reserve at least half of the logical process budget for Arrow/Python,
    # SQLite, compressed/staged payloads, native child overhead and allocator
    # fragmentation. Prefer preserving the requested per-engine DuckDB working
    # set and reduce merge-writer concurrency first. A too-small per-engine cap
    # can make one otherwise healthy wide transform fail even when the host has
    # abundant free memory.
    duckdb_budget = max(1,memory_bytes//2)
    if cfg.get("load_mode") == "merge_async":
        memory_writer_cap = duckdb_merge_writer_cap(
            cfg,table_count,requested_bytes=requested_bytes)
        cfg["writer_max"] = min(cfg["writer_max"],memory_writer_cap)
        cfg["writer_initial"] = min(cfg["writer_initial"],cfg["writer_max"])
        cfg["writer_min"] = min(cfg["writer_min"],cfg["writer_initial"])
        cfg["duckdb_memory_writer_cap"] = memory_writer_cap
    else:
        cfg["duckdb_memory_writer_cap"] = cfg["writer_max"]

    engine_slots = duckdb_engine_slots(cfg,table_count)
    per_engine_cap = max(1,duckdb_budget//engine_slots)
    if requested_bytes > per_engine_cap and per_engine_cap < 32*1024**2:
        raise RuntimeError(
            "CDC resource memory budget is too small for the configured table/"
            f"writer topology: memory_mb={policy['memory_mb']} "
            f"duckdb_engine_slots={engine_slots} "
            f"per_engine_cap_mb={per_engine_cap//1024**2}; "
            "reduce concurrent tables/writers or provide more memory")
    effective_bytes = min(requested_bytes,per_engine_cap)
    effective_mb = max(1,effective_bytes//1024**2)
    cfg["duckdb_memory"] = f"{effective_mb}MB"
    cfg["duckdb_engine_slots"] = engine_slots
    cfg["duckdb_memory_requested_bytes"] = requested_bytes
    cfg["duckdb_memory_cap_bytes"] = per_engine_cap
    return cfg


def resource_pressure_flags(memory_available, memory_known, memory_reserve,
                            disk_free, disk_known, disk_threshold,
                            tree_rss_mb, tree_known, tree_budget_mb):
    memory_pressure = (
        not memory_known
        or int(memory_available) <= int(memory_reserve)
        or not tree_known
        or float(tree_rss_mb) >= float(tree_budget_mb)*0.85
    )
    disk_pressure = (
        not disk_known
        or int(disk_free) <= int(disk_threshold)
    )
    return memory_pressure,disk_pressure


def runtime_cpu_target(capacity_count, cpu_cap, cpu_reserve, load1):
    return max(
        1,min(
            max(1,int(cpu_cap)),
            int(max(1,int(capacity_count))-max(0.0,float(load1))-max(1,int(cpu_reserve)))
        )
    )


def current_resource_headroom(cfg, table_count=1):
    policy = cfg["resource"]
    memory = system_memory_stats()
    tree = process_tree_rss_stats()
    try:
        load1 = float(os.getloadavg()[0])
    except (AttributeError,OSError):
        load1 = 0.0
    cpu_count = max(1,int(policy.get(
        "cpu_capacity_count",policy["detected_cpu_count"])))
    cpu_cap = max(1,int(policy["cpu_cap"]))
    cpu_reserve = max(1,int(policy["cpu_reserve_cores"]))
    cpu_target = runtime_cpu_target(cpu_count,cpu_cap,cpu_reserve,load1)
    table_count = max(1,int(table_count or 1))
    writer_cap = max(1,min(cfg["writer_max"],cpu_target//table_count))

    memory_available = int(memory["available_mb"] or 0)
    disk_free = 0
    disk_total = 0
    disk_known = False
    try:
        usage = shutil.disk_usage(os.path.dirname(cfg["state"]) or ".")
        disk_free,disk_total = int(usage.free),int(usage.total)
        disk_known = True
    except OSError:
        pass
    disk_threshold = max(int(cfg["min_free_bytes"])*2,4*1024**3)
    memory_pressure,disk_pressure = resource_pressure_flags(
        memory_available,bool(memory["available_known"]),int(policy["memory_reserve_mb"]),
        disk_free,disk_known,disk_threshold,
        tree["rss_mb"],bool(tree["known"]),int(policy["memory_mb"]))
    cpu_pressure = load1 >= max(1,cpu_count-cpu_reserve)
    memory_system_exhausted = bool(
        memory["available_known"] and memory_available <= 0)
    memory_hard_exceeded = bool(
        memory_system_exhausted
        or (tree["known"] and float(tree["rss_mb"]) >= float(policy["memory_mb"])))
    disk_hard_exceeded = bool(
        not disk_known or disk_free <= int(cfg["min_free_bytes"]))
    if memory_pressure or disk_pressure:
        writer_cap = 1
    pause_snapshot = memory_pressure or disk_pressure or cpu_pressure
    return dict(
        cpu_target=cpu_target,
        writer_cap=writer_cap,
        load1=load1,
        memory_available_mb=memory_available,
        memory_known=bool(memory["available_known"]),
        memory_pressure=memory_pressure,
        process_tree_rss_mb=float(tree["rss_mb"]),
        process_tree_processes=int(tree["processes"]),
        process_tree_known=bool(tree["known"]),
        memory_hard_exceeded=memory_hard_exceeded,
        disk_free_bytes=disk_free,
        disk_total_bytes=disk_total,
        disk_known=disk_known,
        disk_pressure=disk_pressure,
        disk_hard_exceeded=disk_hard_exceeded,
        cpu_pressure=cpu_pressure,
        pause_snapshot=pause_snapshot,
    )


def read_config():
    variables = catalog_variables()
    resource_policy = read_resource_policy()
    resource_cap = resource_policy["cpu_cap"]
    resource_target = resource_policy["cpu_target"]
    state_path = os.path.abspath(env(
        "CDC_STATE_FILE",
        os.path.join(os.path.dirname(os.path.abspath(__file__)),".cdc_v2.sqlite3")))
    catalog_paths = cdc_catalog.catalog_paths(
        __file__,state_path,variables=variables)
    snapshot_chunk_default = min(
        256*1024**2,max(32*1024**2,int(resource_policy["memory_mb"])*1024**2//8))
    transaction_spool_default = min(
        8*1024**3,max(512*1024**2,disk_reserve_bytes(state_path)//4))
    connection = cdc_catalog.connection_settings_values(variables)
    cfg = dict(
        resource=resource_policy,
        mysql=connection["mysql"],
        sr=connection["starrocks"],
        state=state_path,
        catalog=catalog_paths["catalog"],
        catalog_seed=catalog_paths["seed"],
        catalog_socket=catalog_paths["socket"],
        catalog_version=0,
        catalog_hash="",
        catalog_macros=[],
        catalog_udfs=[],
        catalog_published_version=0,
        catalog_config_revision=cdc_catalog.config_revision(
            catalog_paths["catalog"]),
        key_partitions=env_int("CDC_KEY_PARTITIONS", 16, maximum=64),
        writer_min=env_int("CDC_WRITE_WORKERS_MIN", 1, maximum=32),
        writer_initial=env_int("CDC_WRITE_WORKERS_INITIAL", min(4,resource_target), maximum=32),
        writer_max=env_int("CDC_WRITE_WORKERS_MAX", min(8,resource_cap), maximum=32),
        writer_ramp_seconds=env_int("CDC_WRITE_RAMP_SECONDS", 30, maximum=3600),
        writer_monitor_seconds=env_int("CDC_WRITE_MONITOR_SECONDS", 5, maximum=300),
        rowset_yellow=env_int("CDC_ROWSET_YELLOW", 500, maximum=10000),
        rowset_red=env_int("CDC_ROWSET_RED", 700, maximum=10000),
        version_recovery_checks=env_int("CDC_VERSION_RECOVERY_CHECKS", 2, maximum=10),
        snapshot_workers=env_int("CDC_SNAPSHOT_WORKERS", max(1,min(2,resource_target//2)), maximum=32),
        snapshot_rows=env_int("CDC_SNAPSHOT_ROWS", 50000, maximum=100000),
        snapshot_chunk_bytes=env_int(
            "CDC_SNAPSHOT_CHUNK_BYTES",snapshot_chunk_default,
            minimum=8*1024**2,maximum=2*1024**3),
        snapshot_bundle_max_lanes=env_int("CDC_SNAPSHOT_BUNDLE_MAX_LANES", 8, maximum=64),
        snapshot_read_ahead_groups=env_int(
            "CDC_SNAPSHOT_READ_AHEAD_GROUPS",2,minimum=1,maximum=32),
        batch_rows=env_int("CDC_BATCH_ROWS", 50000, maximum=1000000),
        native_event_group_events=env_int("CDC_NATIVE_EVENT_GROUP_EVENTS", 1, maximum=4096),
        batch_bytes=env_int("CDC_BATCH_BYTES", 16*1024*1024, maximum=64*1024*1024),
        batch_ms=env_int("CDC_BATCH_MS", 1000, minimum=0, maximum=3000),
        txn_rows=env_int("CDC_TXN_ROWS", 50000, maximum=1000000),
        txn_bytes=env_int("CDC_TXN_BYTES", 32*1024**2, maximum=256*1024**2),
        txn_spool_max_bytes=env_int(
            "CDC_TXN_SPOOL_MAX_BYTES",transaction_spool_default,
            minimum=64*1024**2,maximum=64*1024**3),
        commit_interval_ms=env_int("CDC_COMMIT_INTERVAL_MS", 2000, maximum=10000),
        pressure_max_seconds=env_int("CDC_PRESSURE_MAX_SECONDS", 60, maximum=600),
        load_mode=env("CDC_LOAD_MODE", "merge_async").strip().lower(),
        merge_commit_interval_ms=env_int("CDC_MERGE_COMMIT_INTERVAL_MS", 1000, maximum=60000),
        merge_commit_parallel=env_int("CDC_MERGE_COMMIT_PARALLEL", max(1,min(2,resource_target//2)), maximum=32),
        max_row_bytes=env_int("CDC_MAX_ROW_BYTES", 64*1024*1024, maximum=64*1024*1024),
        max_backlog_bytes=env_int("CDC_MAX_BACKLOG_BYTES", 2*1024**3),
        max_prepared_bytes=env_int("CDC_MAX_PREPARED_BYTES", 512*1024**2),
        max_inflight_deliveries=env_int("CDC_MAX_INFLIGHT_DELIVERIES", 8, maximum=256),
        min_free_bytes=env_int("CDC_MIN_FREE_BYTES", disk_reserve_bytes(state_path), minimum=0),
        freshness_seconds=float(env("CDC_FRESHNESS_SECONDS", "2")),
        query_timeout=env_int("CDC_QUERY_TIMEOUT", 30),
        load_timeout=env_int("CDC_LOAD_TIMEOUT", 600),
        retry_max=env_int("CDC_RETRY_MAX", 8),
        server_id=int(connection["runtime"]["server_id"]),
        compression=env("CDC_COMPRESSION", "gzip"),
        duckdb_memory=env("CDC_DUCKDB_MEMORY", "256MB"),
        resource_monitor_seconds=env_int("CDC_RESOURCE_MONITOR_SECONDS", 5, maximum=300),
        status_seconds=env_int("CDC_STATUS_SECONDS", 30, maximum=3600),
        idle_status_seconds=env_int("CDC_IDLE_STATUS_SECONDS", 300, maximum=86400),
        detail_logs=env_bool("CDC_DETAIL_LOGS", False),
        shared_source_state=env_bool("CDC_SHARED_SOURCE_STATE", False),
        stateful_share_mode=env(
            "CDC_STATEFUL_SHARE_MODE","compatible").strip().lower(),
        stateful_share_max_lag=env_int(
            "CDC_STATEFUL_SHARE_MAX_LAG",10000,minimum=0,maximum=1000000000),
        stateful_share_max_followers=env_int(
            "CDC_STATEFUL_SHARE_MAX_FOLLOWERS",1000,minimum=1,maximum=1000000),
        stateful_share_max_surplus=env_int(
            "CDC_STATEFUL_SHARE_MAX_SURPLUS",64,minimum=0,maximum=1000000),
        stateful_share_max_observed_visible_lag=env_int(
            "CDC_STATEFUL_SHARE_MAX_OBSERVED_VISIBLE_LAG",
            10000,minimum=0,maximum=1000000000),
        plan_retain=env_int("CDC_PLAN_RETAIN",32,minimum=4,maximum=10000),
        metrics_max_bytes=env_int(
            "CDC_METRICS_MAX_BYTES",64*1024**2,
            minimum=1024**2,maximum=4*1024**3),
        overflow_value_days=env_int(
            "CDC_OVERFLOW_VALUE_DAYS",90,minimum=1,maximum=3650),
        overflow_metadata_days=env_int(
            "CDC_OVERFLOW_METADATA_DAYS",365,minimum=1,maximum=36500),
        source_reader="native_c_v1",
        native_binlog_path=os.path.abspath(env(
            "CDC_NATIVE_BINLOG_PATH",
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "mysql_arrow_reader-linux-x86_64"))),
        run_seconds=env_int("CDC_RUN_SECONDS", 0, minimum=0),
    )
    if cfg["load_mode"] not in ("merge_async","transaction"):
        raise ValueError("CDC_LOAD_MODE must be merge_async or transaction")
    if cfg["stateful_share_mode"] not in stateful_share_policy.MODES:
        raise ValueError(
            "CDC_STATEFUL_SHARE_MODE must be one of "
            +",".join(sorted(stateful_share_policy.MODES)))
    if not (1 <= cfg["writer_min"] <= cfg["writer_initial"] <= cfg["writer_max"] <= cfg["key_partitions"]):
        raise ValueError("writer concurrency must satisfy 1 <= MIN <= INITIAL <= MAX <= CDC_KEY_PARTITIONS")
    if cfg["rowset_yellow"] >= cfg["rowset_red"]:
        raise ValueError("CDC_ROWSET_YELLOW must be lower than CDC_ROWSET_RED")
    if cfg["overflow_metadata_days"] < cfg["overflow_value_days"]:
        raise ValueError(
            "CDC_OVERFLOW_METADATA_DAYS must be >= CDC_OVERFLOW_VALUE_DAYS")
    return enforce_resource_config(cfg)


def sql_name(value, mysql=False):
    mark = chr(96) if mysql else '"'
    return mark + str(value).replace(mark, mark*2) + mark


def pk_columns(mapping):
    key = mapping.get("primary_key")
    if key is None:
        return []
    return [key] if isinstance(key, str) else list(key)


def mapping_key(mapping):
    return str(mapping.get("_catalog_sink") or mapping["src_table"])


def source_relation_key(cfg, mapping_or_table):
    table = (
        mapping_or_table["src_table"]
        if isinstance(mapping_or_table,dict)
        else str(mapping_or_table)
    )
    return str(cfg["mysql"]["database"])+"."+str(table)


def source_state_relation_exists(con, relation):
    try:
        source_state.relation_info(con,relation)
        return True
    except KeyError:
        return False


def source_base_catalog_spec(info):
    relation_identity = (
        str(info["source_epoch"]) + "::" + str(info["table_name"])
        + "::" + str(info["schema_hash"])
    )
    return incremental_contract.state_spec(
        "base",
        [relation_identity],
        [int(info["schema_epoch"])],
        key_exprs=list(info["pk_columns"]),
        value_exprs=list(info["columns"]),
        predicate="TRUE",
        collation="source-values-v1",
    )


def source_base_catalog_instance_id(info):
    identity = incremental_contract.state_identity(
        source_base_catalog_spec(info))
    return "source-base-" + identity[:40]


def sync_source_base_catalog(con):
    result = []
    # Multiple shared snapshot workers can call this concurrently through
    # separate SQLite connections. Keep source watermarks, existing physical
    # frontiers, monotonic advancement, health and owner refs in one IMMEDIATE
    # transaction so no worker can advance the physical frontier between
    # another worker's read and write.
    with physical_state_catalog.transaction(con):
        applied = source_state.base_applied_seq(con)
        physical_min = source_state.min_readable_seq(con)
        tables = [
            row[0] for row in con.execute(
                "SELECT table_name FROM source_relations ORDER BY table_name")
        ]
        for table_name in tables:
            info = source_state.relation_info(con,table_name)
            complete = info["complete_seq"]
            minimum = (
                applied if complete is None
                else max(int(complete),int(physical_min))
            )
            spec = source_base_catalog_spec(info)
            instance_id = source_base_catalog_instance_id(info)
            metadata = dict(
                source_relation=str(table_name),
                source_epoch=str(info["source_epoch"]),
                schema_hash=str(info["schema_hash"]),
                pin_authority="source_state",
            )
            state = physical_state_catalog.ensure_state(
                con,spec,"sqlite-source-state","source-state-v1",
                applied,min_readable_watermark=minimum,
                generation=1,
                health="ready" if complete is not None else "building",
                metadata=metadata,instance_id=instance_id)
            minimum = max(
                int(minimum),
                int(state["min_readable_watermark"]))
            state = physical_state_catalog.advance_state(
                con,instance_id,applied,
                min_readable_watermark=minimum)
            desired = "ready" if complete is not None else "building"
            if state["health"] != desired:
                state = physical_state_catalog.set_health(
                    con,instance_id,desired)
            physical_state_catalog.retain_state(
                con,instance_id,"source-state:"+str(table_name),"owner")
            result.append(state)
    return result


def source_mappings(prepared):
    by_source = {}
    for mapping in prepared:
        source = mapping["src_table"]
        previous = by_source.get(source)
        if previous is None:
            by_source[source] = mapping
            continue
        if (
            previous.get("_schema_signature") != mapping.get("_schema_signature")
            or pk_columns(previous) != pk_columns(mapping)
        ):
            raise RuntimeError(
                f"{source}: fan-out mappings disagree on checked source schema/primary key")
    return list(by_source.values())


def key_partition_count(cfg):
    return int(cfg["key_partitions"])


def pack(value):
    return pickle.dumps(value, protocol=5)


def unpack(value):
    return pickle.loads(value)


def position_key(position):
    name, offset = position
    match = re.fullmatch(r"(.*?)([0-9]+)", str(name))
    if not match:
        raise ValueError(f"unsupported binlog filename: {name!r}")
    return match[1], int(match[2]), int(offset)


def position_ge(left, right):
    a, b = position_key(left), position_key(right)
    if a[0] != b[0]:
        raise RuntimeError("binlog prefix changed; explicit source rebuild is required")
    return a[1:] >= b[1:]


def gtid_parse_set(value):
    parsed = {}
    text = str(value or "").strip()
    if not text:
        return parsed
    for member in text.split(","):
        pieces = [part.strip() for part in member.strip().split(":")]
        if len(pieces) < 2:
            raise ValueError(f"invalid GTID set member: {member!r}")
        sid = str(uuid.UUID(pieces[0])).lower()
        intervals = list(parsed.get(sid,()))
        for item in pieces[1:]:
            if not item:
                raise ValueError(f"invalid GTID interval in {member!r}")
            bounds = item.split("-",1)
            start = int(bounds[0])
            end = int(bounds[1]) if len(bounds) == 2 else start
            if start < 1 or end < start:
                raise ValueError(f"invalid GTID interval {item!r}")
            intervals.append((start,end+1))
        intervals.sort()
        merged = []
        for start,end in intervals:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0],max(merged[-1][1],end))
            else:
                merged.append((start,end))
        parsed[sid] = merged
    return parsed


def gtid_format_set(parsed):
    members = []
    for sid in sorted(parsed):
        values = []
        for start,end in parsed[sid]:
            last = end-1
            values.append(str(start) if start == last else f"{start}-{last}")
        members.append(sid+":"+":".join(values))
    return ",".join(members)


def gtid_add(gtid_set, gtid):
    parsed = gtid_parse_set(gtid_set)
    incoming = gtid_parse_set(gtid)
    for sid,intervals in incoming.items():
        combined = list(parsed.get(sid,()))+list(intervals)
        combined.sort()
        merged = []
        for start,end in combined:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0],max(merged[-1][1],end))
            else:
                merged.append((start,end))
        parsed[sid] = merged
    return gtid_format_set(parsed)


def gtid_contains(container, contained):
    available = gtid_parse_set(container)
    required = gtid_parse_set(contained)
    for sid,intervals in required.items():
        present = available.get(sid,())
        for start,end in intervals:
            if not any(start >= left and end <= right for left,right in present):
                return False
    return True


def gtid_encode_set(value):
    parsed = gtid_parse_set(value)
    out = bytearray(struct.pack("<Q",len(parsed)))
    for sid in sorted(parsed):
        out.extend(uuid.UUID(sid).bytes)
        intervals = parsed[sid]
        out.extend(struct.pack("<Q",len(intervals)))
        for start,end in intervals:
            out.extend(struct.pack("<QQ",start,end))
    return bytes(out)


STATE_WAL_CHECKPOINT_BYTES = 64*1024*1024
STATE_WAL_CHECKPOINT_INTERVAL = 0.5


def open_state(path):
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    # Commits are durable in WAL at FULL synchronous. Checkpointing is deliberately
    # detached from writer latency and handled by state_checkpoint_worker().
    con.execute("PRAGMA wal_autocheckpoint=0")
    con.execute("PRAGMA foreign_keys=ON")
    return con


@contextlib.contextmanager
def state_transaction(con):
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


STATE_FORMAT = 4


def init_state(path):
    con = open_state(path)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS table_state(
            name TEXT PRIMARY KEY, cursor BLOB, upper_key BLOB,
            upper_set INTEGER NOT NULL DEFAULT 0,
            snapshot_done INTEGER NOT NULL DEFAULT 0,
            staged_cursor BLOB,
            staged_done INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS touched(
            table_name TEXT NOT NULL, pk TEXT NOT NULL,
            PRIMARY KEY(table_name,pk)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS snapshot_groups(
            id TEXT PRIMARY KEY, table_name TEXT NOT NULL,
            cursor BLOB, is_last INTEGER NOT NULL,
            stage_seq INTEGER NOT NULL DEFAULT 0,
            plan_version INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS jobs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL, lane INTEGER NOT NULL, kind TEXT NOT NULL,
            payload BLOB NOT NULL, nrows INTEGER NOT NULL,
            logical_bytes INTEGER NOT NULL DEFAULT 0,
            plan_version INTEGER NOT NULL DEFAULT 0,
            source_file TEXT, source_pos INTEGER, source_seq INTEGER,
            source_time REAL, created REAL NOT NULL,
            group_id TEXT, delivery_id TEXT);
        CREATE INDEX IF NOT EXISTS jobs_lane ON jobs(table_name,lane,id);
        CREATE INDEX IF NOT EXISTS jobs_group ON jobs(group_id);
        CREATE INDEX IF NOT EXISTS jobs_table ON jobs(table_name,id);
        CREATE TABLE IF NOT EXISTS deliveries(
            id TEXT PRIMARY KEY, table_name TEXT NOT NULL, lane INTEGER NOT NULL,
            plan_version INTEGER NOT NULL DEFAULT 0,
            prepared INTEGER NOT NULL DEFAULT 0,
            UNIQUE(table_name,lane));
        CREATE TABLE IF NOT EXISTS job_assignments(
            job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
            delivery_id TEXT NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS aggregate_job_links(
            job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS aggregate_job_links_commit
            ON aggregate_job_links(consumer_id,source_seq,job_id);
        CREATE TABLE IF NOT EXISTS join_job_links(
            job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS join_job_links_commit
            ON join_job_links(consumer_id,source_seq,job_id);
        CREATE TABLE IF NOT EXISTS prepare_reservations(
            delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
            reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0));
        CREATE TABLE IF NOT EXISTS prepare_requirements(
            delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id) ON DELETE CASCADE,
            required_bytes INTEGER CHECK(required_bytes IS NULL OR required_bytes>=0),
            full_prepare_attempts INTEGER NOT NULL DEFAULT 0,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS job_assignments_delivery
            ON job_assignments(delivery_id,job_id);
        CREATE TABLE IF NOT EXISTS retired_jobs(
            job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE);
        CREATE VIEW IF NOT EXISTS active_jobs AS
            SELECT j.* FROM jobs j
            LEFT JOIN retired_jobs r ON r.job_id=j.id
            WHERE r.job_id IS NULL;
        CREATE TABLE IF NOT EXISTS load_parts(
            delivery_id TEXT NOT NULL, part INTEGER NOT NULL,
            label TEXT NOT NULL, payload BLOB NOT NULL, nrows INTEGER NOT NULL,
            visible INTEGER NOT NULL DEFAULT 0, txn_id INTEGER,
            json_bytes INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(delivery_id,part));
        CREATE TABLE IF NOT EXISTS applied(
            table_name TEXT NOT NULL, lane INTEGER NOT NULL,
            source_file TEXT, source_pos INTEGER, source_seq INTEGER,
            PRIMARY KEY(table_name,lane));
        CREATE TABLE IF NOT EXISTS load_transactions(
            delivery_id TEXT PRIMARY KEY REFERENCES deliveries(id),
            label TEXT NOT NULL, phase TEXT NOT NULL DEFAULT 'NEW', txn_id INTEGER,
            attempt INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
            retry_at REAL NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS merge_uncertain(
            delivery_id TEXT NOT NULL,
            part INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            lane INTEGER NOT NULL,
            label TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            reason TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL,
            PRIMARY KEY(delivery_id,part));
        CREATE TABLE IF NOT EXISTS field_overflow(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            delivery_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            row_order INTEGER NOT NULL,
            pk_json TEXT NOT NULL,
            column_name TEXT NOT NULL,
            target_type TEXT NOT NULL,
            actual_bytes INTEGER NOT NULL,
            raw_bytes INTEGER,
            limit_bytes INTEGER NOT NULL,
            value_encoding TEXT NOT NULL,
            value BLOB NOT NULL,
            value_sha256 TEXT NOT NULL,
            value_pruned INTEGER NOT NULL DEFAULT 0,
            action TEXT NOT NULL,
            source_file TEXT, source_pos INTEGER, source_time REAL,
            created REAL NOT NULL,
            UNIQUE(delivery_id,row_order,column_name));
        CREATE INDEX IF NOT EXISTS field_overflow_table
            ON field_overflow(table_name,column_name,id);
    """)
    source_state.install(con)
    aggregate_state.install(con)
    join_state.install(con)
    aggregate_outbox.install(con)
    join_outbox.install(con)
    join_task_catalog.install(con)
    aggregate_task_catalog.install(con)
    stateful_catalog_runtime.install(con)
    aggregate_shared_runtime.install(con)
    join_shared_runtime.install(con)
    stateful_share_policy.install(con)
    stateful_rebuild.install(con)
    physical_state_catalog.install(con)
    task_generation.install(con)
    existing_format = meta_get(con,"state_format")
    if existing_format is None:
        if con.execute("SELECT 1 FROM meta LIMIT 1").fetchone():
            con.close()
            raise RuntimeError(
                "legacy CDC state is unsupported by the Arrow/DuckDB routing format; "
                "delete the old state and start with an empty target or a new target table")
        with state_transaction(con):
            meta_set(con,"state_format",STATE_FORMAT)
        existing_format = STATE_FORMAT
    elif int(existing_format) not in (2,3,STATE_FORMAT):
        con.close()
        raise RuntimeError(
            f"CDC state format {existing_format!r} is incompatible with required format {STATE_FORMAT}; "
            "rebuild state explicitly")
    table_state_columns = {
        row[1] for row in con.execute("PRAGMA table_info(table_state)").fetchall()}
    if "staged_cursor" not in table_state_columns:
        with state_transaction(con):
            con.execute("ALTER TABLE table_state ADD COLUMN staged_cursor BLOB")
    if "staged_done" not in table_state_columns:
        with state_transaction(con):
            con.execute(
                "ALTER TABLE table_state "
                "ADD COLUMN staged_done INTEGER NOT NULL DEFAULT 0")
    snapshot_group_columns = {
        row[1] for row in con.execute(
            "PRAGMA table_info(snapshot_groups)").fetchall()}
    if "stage_seq" not in snapshot_group_columns:
        with state_transaction(con):
            con.execute(
                "ALTER TABLE snapshot_groups "
                "ADD COLUMN stage_seq INTEGER NOT NULL DEFAULT 0")
    if "plan_version" not in snapshot_group_columns:
        with state_transaction(con):
            con.execute(
                "ALTER TABLE snapshot_groups "
                "ADD COLUMN plan_version INTEGER NOT NULL DEFAULT 0")

    if meta_get(con,"snapshot_staged_cursor_v1",0) != 1:
        # Pre-read-ahead versions allowed at most one pending snapshot group per
        # table and advanced table_state.cursor only after that group became
        # visible. Promote that durable pending group's cursor to staged_cursor
        # so an in-place upgrade never rereads an already-journaled snapshot
        # range. cursor/snapshot_done retain their visible-only meaning.
        with state_transaction(con):
            con.execute("""
                UPDATE snapshot_groups
                SET stage_seq=rowid
                WHERE stage_seq=0
            """)
            for table,visible_cursor,visible_done in con.execute("""
                SELECT name,cursor,snapshot_done FROM table_state
            """).fetchall():
                pending = con.execute("""
                    SELECT cursor,is_last
                    FROM snapshot_groups
                    WHERE table_name=?
                    ORDER BY stage_seq DESC LIMIT 1
                """,(table,)).fetchone()
                staged_cursor = pending[0] if pending else visible_cursor
                staged_done = int(pending[1]) if pending else int(visible_done)
                con.execute("""
                    UPDATE table_state
                    SET staged_cursor=?,staged_done=?
                    WHERE name=?
                """,(staged_cursor,staged_done,table))
            meta_set(con,"snapshot_staged_cursor_v1",1)

    job_columns = {row[1] for row in con.execute("PRAGMA table_info(jobs)").fetchall()}
    if "logical_bytes" not in job_columns:
        with state_transaction(con):
            con.execute("ALTER TABLE jobs ADD COLUMN logical_bytes INTEGER NOT NULL DEFAULT 0")
    if "plan_version" not in job_columns:
        with state_transaction(con):
            con.execute("ALTER TABLE jobs ADD COLUMN plan_version INTEGER NOT NULL DEFAULT 0")
    if "source_seq" not in job_columns:
        with state_transaction(con):
            con.execute("ALTER TABLE jobs ADD COLUMN source_seq INTEGER")
    applied_columns = {
        row[1] for row in con.execute("PRAGMA table_info(applied)").fetchall()}
    if "source_seq" not in applied_columns:
        with state_transaction(con):
            con.execute("ALTER TABLE applied ADD COLUMN source_seq INTEGER")
    overflow_columns = {
        row[1] for row in con.execute("PRAGMA table_info(field_overflow)").fetchall()}
    if "value_pruned" not in overflow_columns:
        with state_transaction(con):
            con.execute(
                "ALTER TABLE field_overflow "
                "ADD COLUMN value_pruned INTEGER NOT NULL DEFAULT 0")
    delivery_columns = {
        row[1] for row in con.execute("PRAGMA table_info(deliveries)").fetchall()}
    if "plan_version" not in delivery_columns:
        with state_transaction(con):
            con.execute(
                "ALTER TABLE deliveries ADD COLUMN plan_version INTEGER NOT NULL DEFAULT 0")
    if meta_get(con,"logical_job_bytes_v1",0) != 1:
        last_id = 0
        while True:
            rows = con.execute("""
                SELECT id,payload FROM jobs
                WHERE id>? AND logical_bytes<=0 ORDER BY id LIMIT 32
            """,(last_id,)).fetchall()
            if not rows:
                break
            with state_transaction(con):
                for job_id,payload in rows:
                    con.execute(
                        "UPDATE jobs SET logical_bytes=? WHERE id=?",
                        (arrow_payload_logical_bytes(payload),job_id))
            last_id = int(rows[-1][0])
        with state_transaction(con):
            pending = con.execute("""
                SELECT COALESCE(SUM(j.logical_bytes),0)
                FROM jobs j LEFT JOIN retired_jobs r ON r.job_id=j.id
                WHERE r.job_id IS NULL
            """).fetchone()[0]
            meta_set(con,"pending_bytes",int(pending))
            meta_set(con,"logical_job_bytes_v1",1)
    if meta_get(con,"job_assignment_sidecar_v1",0) != 1:
        with state_transaction(con):
            con.execute("""
                INSERT OR IGNORE INTO job_assignments(job_id,delivery_id)
                SELECT id,delivery_id FROM jobs WHERE delivery_id IS NOT NULL
            """)
            meta_set(con,"job_assignment_sidecar_v1",1)
            con.execute("DROP INDEX IF EXISTS jobs_delivery")
    if int(existing_format) in (2,3):
        with state_transaction(con):
            meta_set(con,"state_format",STATE_FORMAT)
            meta_set(con,"state_migrated_from",int(existing_format))
    return con


def state_initialized(path):
    if not os.path.exists(path):
        return False
    con = sqlite3.connect(path)
    try:
        if not con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone():
            return False
        return con.execute("SELECT 1 FROM meta WHERE key='fingerprint'").fetchone() is not None
    finally:
        con.close()


def meta_get(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return unpack(row[0]) if row else default


def meta_set(con, key, value):
    con.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, pack(value)))


def bootstrap(con, fingerprint, source_uuid, position, table_names, gtid_set=None):
    with state_transaction(con):
        previous = meta_get(con, "fingerprint")
        if previous is not None:
            if previous != fingerprint or meta_get(con, "source_uuid") != source_uuid:
                raise RuntimeError("configuration/schema/source changed; preserve state and rebuild explicitly")
            return
        meta_set(con, "fingerprint", fingerprint)
        meta_set(con, "source_uuid", source_uuid)
        meta_set(con, "run_id", uuid.uuid4().hex)
        meta_set(con, "read_position", position)
        if gtid_set is not None:
            meta_set(con, "gtid_set", gtid_set)
        meta_set(con, "pending_bytes", 0)
        for table in table_names:
            con.execute("INSERT INTO table_state(name) VALUES(?)", (table,))


def migrate_sink_identity(con, catalog_path, current_version, prepared, fresh=False):
    if int(meta_get(con,"sink_identity_v1",0) or 0) == 1:
        return 0
    if fresh:
        with state_transaction(con):
            meta_set(con,"sink_identity_v1",1)
        return 0

    version = int(meta_get(con,"active_plan_version",0) or current_version or 0)
    if version:
        legacy = cdc_catalog.load_plan_version(catalog_path,version).get("mappings",())
    else:
        legacy = prepared

    source_to_sink = {}
    for mapping in legacy:
        source = str(mapping["src_table"])
        sink = mapping_key(mapping)
        previous = source_to_sink.get(source)
        if previous is not None and previous != sink:
            raise RuntimeError(
                "legacy durable state cannot be migrated to sink identity because "
                f"catalog plan {version} already has multiple sinks for source {source}")
        source_to_sink[source] = sink

    tables = (
        ("table_state","name"),
        ("touched","table_name"),
        ("snapshot_groups","table_name"),
        ("jobs","table_name"),
        ("deliveries","table_name"),
        ("applied","table_name"),
        ("merge_uncertain","table_name"),
        ("field_overflow","table_name"),
    )
    changed = 0
    with state_transaction(con):
        for source,sink in source_to_sink.items():
            if source == sink:
                continue
            for table,column in tables:
                source_count = int(con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column}=?",
                    (source,)).fetchone()[0])
                if not source_count:
                    continue
                target_count = int(con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column}=?",
                    (sink,)).fetchone()[0])
                if target_count:
                    raise RuntimeError(
                        "sink identity migration found both legacy and target keys "
                        f"table={table} source={source} sink={sink}; preserve state")
                con.execute(
                    f"UPDATE {table} SET {column}=? WHERE {column}=?",
                    (sink,source))
                changed += source_count
        meta_set(con,"sink_identity_v1",1)
    if changed:
        log(
            f"STATE MIGRATION sink_identity_v1 rows={changed} "
            f"active_plan_version={version}")
    return changed


def cursor_advance(con, position):
    old = meta_get(con, "read_position")
    if position != old and position_ge(position, old):
        meta_set(con, "read_position", position)


SPOOL_HEADER = ">HIIQQ"
SPOOL_HEADER_SIZE = struct.calcsize(SPOOL_HEADER)

_NATIVE_PARTITION_LIB = None
_NATIVE_PARTITION_TRIED = False
_NATIVE_PARTITION_FD = None
_NATIVE_PARTITION_ERRORS = []
_NATIVE_PARTITION_SOURCE = None
J4_NATIVE_ABI_VERSION = 3
J4_NATIVE_FEATURE_STABLE_PARTITION = 1 << 0
J4_NATIVE_FEATURE_NO_LIBC = 1 << 1
J4_NATIVE_FEATURE_JSON_ENCODER = 1 << 2
J4_NATIVE_BUNDLED_SHA256 = "d1035e7189dca3f73e42a1bdcdb9b8ae633bf7830c3ee89c2d951309fd88c49c"
J4_NATIVE_LOADER_REVISION = 3

J4_JSON_I8 = 1
J4_JSON_U8 = 2
J4_JSON_I16 = 3
J4_JSON_U16 = 4
J4_JSON_I32 = 5
J4_JSON_U32 = 6
J4_JSON_I64 = 7
J4_JSON_U64 = 8
J4_JSON_STRING = 11
J4_JSON_BINARY = 12
J4_JSON_FLAG_LARGE_OFFSETS = 1 << 0
J4_JSON_FLAG_BASE64 = 1 << 1
_NATIVE_JSON_LOGGED = False


class J4NativeJsonColumn(ctypes.Structure):
    _fields_ = [
        ("validity",ctypes.c_void_p),
        ("values",ctypes.c_void_p),
        ("data",ctypes.c_void_p),
        ("key",ctypes.c_void_p),
        ("offset",ctypes.c_uint64),
        ("key_len",ctypes.c_uint32),
        ("kind",ctypes.c_uint32),
        ("flags",ctypes.c_uint32),
        ("reserved",ctypes.c_uint32),
    ]


def load_bundled_native_library(root):
    global _NATIVE_PARTITION_FD
    encoded_path = os.path.join(
        root,"native","libj4_native.so.gz.b64")
    if not os.path.isfile(encoded_path):
        raise RuntimeError(
            f"bundled payload missing: {encoded_path}")
    if hasattr(os,"uname") and os.uname().machine != "x86_64":
        raise RuntimeError(
            f"bundled native library requires x86_64, got {os.uname().machine}")

    with open(encoded_path,"rb") as handle:
        compressed = base64.b64decode(handle.read(),validate=True)
    payload = gzip.decompress(compressed)
    actual = hashlib.sha256(payload).hexdigest()
    if actual != J4_NATIVE_BUNDLED_SHA256:
        raise RuntimeError(
            "bundled native library checksum mismatch "
            f"expected={J4_NATIVE_BUNDLED_SHA256} actual={actual}")

    errors = []
    if hasattr(os,"memfd_create"):
        fd = None
        try:
            flags = int(getattr(os,"MFD_CLOEXEC",0))
            fd = os.memfd_create("j4_native",flags)
            view = memoryview(payload)
            written = 0
            while written < len(view):
                count = os.write(fd,view[written:])
                if count <= 0:
                    raise RuntimeError(
                        "short write while materializing bundled native library")
                written += count
            lib = ctypes.CDLL(f"/proc/self/fd/{fd}")
            _NATIVE_PARTITION_FD = fd
            return lib,"bundled:memfd:x86_64-libc-free"
        except Exception as exc:
            errors.append(f"memfd_dlopen={type(exc).__name__}: {exc}")
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    # Some CentOS 8 / SELinux / procfs policies reject dlopen(/proc/self/fd/N)
    # even though memfd itself works. Fall back to a private verified ELF file.
    temp_path = None
    try:
        fd,temp_path = tempfile.mkstemp(
            prefix=".j4_native_",suffix=".so",dir=root)
        try:
            os.fchmod(fd,0o700)
            view = memoryview(payload)
            written = 0
            while written < len(view):
                count = os.write(fd,view[written:])
                if count <= 0:
                    raise RuntimeError(
                        "short write while materializing native tempfile")
                written += count
            os.fsync(fd)
        finally:
            os.close(fd)
        lib = ctypes.CDLL(temp_path)
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        return lib,"bundled:tempfile:x86_64-libc-free"
    except Exception as exc:
        errors.append(f"tempfile_dlopen={type(exc).__name__}: {exc}")
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        raise RuntimeError("; ".join(errors))

def native_partition_library():
    global _NATIVE_PARTITION_LIB,_NATIVE_PARTITION_TRIED
    global _NATIVE_PARTITION_ERRORS,_NATIVE_PARTITION_SOURCE
    mode = os.environ.get("CDC_NATIVE_PARTITION","auto").strip().lower()
    if mode not in ("auto","off","required"):
        raise ValueError(
            "CDC_NATIVE_PARTITION must be auto, off, or required")
    if mode == "off":
        _NATIVE_PARTITION_TRIED = True
        _NATIVE_PARTITION_LIB = None
        return None
    if _NATIVE_PARTITION_TRIED:
        if mode == "required" and _NATIVE_PARTITION_LIB is None:
            raise RuntimeError(
                "CDC_NATIVE_PARTITION=required but native library is unavailable")
        return _NATIVE_PARTITION_LIB
    _NATIVE_PARTITION_TRIED = True
    _NATIVE_PARTITION_ERRORS = []
    root = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        (path,None) for path in (
            os.path.join(root,"native","libj4_native.so"),
            os.path.join(root,"libj4_native.so"),
        ) if os.path.isfile(path)
    ]
    try:
        bundled_lib,bundled_name = load_bundled_native_library(root)
        if bundled_lib is not None:
            candidates.append((bundled_name,bundled_lib))
    except Exception as exc:
        reason = (
            "bundled loader failed "
            f"{type(exc).__name__}: {exc}")
        _NATIVE_PARTITION_ERRORS.append(reason)
        log(
            "NATIVE PARTITION bundled fallback=arrow "
            f"reason={reason}")

    for path,preloaded in candidates:
        try:
            lib = preloaded if preloaded is not None else ctypes.CDLL(path)
            abi = lib.j4_native_abi_version
            abi.argtypes = []
            abi.restype = ctypes.c_uint32
            features = lib.j4_native_feature_bits
            features.argtypes = []
            features.restype = ctypes.c_uint64
            actual_abi = int(abi())
            actual_features = int(features())
            if actual_abi != J4_NATIVE_ABI_VERSION:
                raise RuntimeError(
                    f"ABI mismatch expected={J4_NATIVE_ABI_VERSION} "
                    f"actual={actual_abi}")
            if not actual_features & J4_NATIVE_FEATURE_STABLE_PARTITION:
                raise RuntimeError(
                    f"stable partition feature missing bits=0x{actual_features:x}")
            fn = lib.j4_stable_partition_u16
            fn.argtypes = [
                ctypes.c_void_p,ctypes.c_uint64,ctypes.c_uint32,
                ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p]
            fn.restype = ctypes.c_int
            if actual_features & J4_NATIVE_FEATURE_JSON_ENCODER:
                measure = lib.j4_json_measure
                measure.argtypes = [
                    ctypes.POINTER(J4NativeJsonColumn),ctypes.c_uint32,
                    ctypes.c_uint64,ctypes.c_uint32,ctypes.c_uint64,
                    ctypes.POINTER(ctypes.c_uint64)]
                measure.restype = ctypes.c_int
                encode = lib.j4_json_encode
                encode.argtypes = [
                    ctypes.POINTER(J4NativeJsonColumn),ctypes.c_uint32,
                    ctypes.c_uint64,ctypes.c_uint32,ctypes.c_uint64,
                    ctypes.c_void_p,ctypes.c_void_p,ctypes.c_uint64,
                    ctypes.POINTER(ctypes.c_uint64)]
                encode.restype = ctypes.c_int
            _NATIVE_PARTITION_LIB = lib
            _NATIVE_PARTITION_SOURCE = path
            log(
                "NATIVE PARTITION enabled "
                f"library={path} abi={actual_abi} "
                f"features=0x{actual_features:x} "
                "algorithm=stable_count_scatter")
            break
        except Exception as exc:
            reason = (
                f"library={path} {type(exc).__name__}: {exc}")
            _NATIVE_PARTITION_ERRORS.append(reason)
            log(
                "NATIVE PARTITION fallback=arrow "
                f"reason={reason}")
    if mode == "required" and _NATIVE_PARTITION_LIB is None:
        details = "; ".join(_NATIVE_PARTITION_ERRORS) or "no candidate discovered"
        raise RuntimeError(
            "CDC_NATIVE_PARTITION=required but no compatible "
            f"native library was loaded: {details}")
    return _NATIVE_PARTITION_LIB


def native_stable_partition_order(routed, partitions):
    lib = native_partition_library()
    if lib is None or not routed.num_rows:
        return None
    lanes = routed.column("_sync_lane").combine_chunks()
    if not pa.types.is_uint16(lanes.type) or lanes.null_count:
        return None
    partitions = int(partitions)
    if partitions <= 0 or partitions > 65536:
        return None

    values = lanes.buffers()[1]
    if values is None:
        return None
    nrows = len(lanes)
    order_buffer = pa.allocate_buffer(nrows*8)
    counts = (ctypes.c_uint64*partitions)()
    cursor = (ctypes.c_uint64*partitions)()
    try:
        src_address = int(values.address) + int(lanes.offset)*2
        dst_address = int(order_buffer.address)
    except (AttributeError,TypeError,ValueError):
        # PyArrow's Python buffer surface is not part of our C ABI. If a future
        # PyArrow release changes it, keep correctness by falling back to the
        # validated Arrow sort path rather than making Python upgrades depend
        # on this optional accelerator.
        return None
    if values.size < (int(lanes.offset)+nrows)*2:
        return None
    rc = lib.j4_stable_partition_u16(
        ctypes.c_void_p(src_address),ctypes.c_uint64(nrows),
        ctypes.c_uint32(partitions),ctypes.c_void_p(dst_address),
        ctypes.cast(counts,ctypes.c_void_p),
        ctypes.cast(cursor,ctypes.c_void_p))
    if rc:
        raise RuntimeError(
            f"native stable partition failed rc={rc} "
            f"rows={nrows} partitions={partitions}")
    order = pa.Array.from_buffers(
        pa.uint64(),nrows,[None,order_buffer])
    return order,[
        (lane,int(counts[lane]))
        for lane in range(partitions) if counts[lane]]


def write_spool_record(spool, table, lane, payload, count, logical_bytes):
    table_bytes = table.encode("utf-8")
    if len(table_bytes) > 65535:
        raise ValueError("source table name is too long for the transaction spool")
    spool.write(struct.pack(
        SPOOL_HEADER,len(table_bytes),int(lane),int(count),
        int(logical_bytes),len(payload)))
    spool.write(table_bytes)
    spool.write(payload)


def read_spool_record(spool):
    header = spool.read(SPOOL_HEADER_SIZE)
    if not header:
        return None
    if len(header) != SPOOL_HEADER_SIZE:
        raise RuntimeError("truncated transaction spool header")
    table_len,lane,count,logical_bytes,payload_len = struct.unpack(SPOOL_HEADER,header)
    table_bytes = spool.read(table_len)
    payload = spool.read(payload_len)
    if len(table_bytes) != table_len or len(payload) != payload_len:
        raise RuntimeError("truncated transaction spool payload")
    return table_bytes.decode("utf-8"),lane,payload,count,int(logical_bytes)


def spool_routed(spool, mapping, routed, cfg):
    if not routed.num_rows:
        return
    # Prefer O(N) stable count/scatter when the optional native helper is
    # available. It returns only a permutation and lane counts; PyArrow still
    # owns/gathers the actual column buffers. Fallback preserves the validated
    # Arrow sort path exactly.
    partitions = key_partition_count(cfg)
    native = native_stable_partition_order(routed,partitions)
    if native is not None:
        order,counts = native
        grouped = routed.take(order)
    else:
        # _sync_order is only batch-local before spooling and can restart at
        # zero when one source transaction contains multiple decoded batches.
        # Preserve the exact routed row sequence with a transaction-local
        # ordinal, then rebuild dense per-lane _sync_order after partitioning.
        row_index = "_sync_partition_order"
        while row_index in routed.column_names:
            row_index = "_" + row_index
        indexed = routed.append_column(
            row_index,pa.array(range(routed.num_rows),type=pa.int64()))
        order = pc.sort_indices(
            indexed,
            sort_keys=[("_sync_lane","ascending"),(row_index,"ascending")])
        grouped = indexed.take(order).drop([row_index])
        counts = [
            (int(item["values"]),int(item["counts"]))
            for item in sorted(
                pc.value_counts(grouped.column("_sync_lane")).to_pylist(),
                key=lambda item:int(item["values"]))]

    start = 0
    for lane,count in counts:
        lane_table = grouped.slice(start,count)
        start += count
        order_index = lane_table.schema.get_field_index("_sync_order")
        lane_table = lane_table.set_column(
            order_index,"_sync_order",pa.array(range(count),type=pa.int64()))
        stack = [(0,count)]
        while stack:
            chunk_start,chunk_count = stack.pop()
            chunk = lane_table.slice(chunk_start,chunk_count)
            logical_bytes = int(chunk.nbytes)
            if logical_bytes > cfg["batch_bytes"] and chunk_count > 1:
                left = chunk_count//2
                stack.extend([
                    (chunk_start+left,chunk_count-left),(chunk_start,left)])
                continue
            payload = arrow_table_payload(chunk)
            logical_bytes = max(logical_bytes,len(payload))
            if logical_bytes > cfg["max_row_bytes"]:
                raise ValueError("single source row exceeds CDC_MAX_ROW_BYTES after Arrow IPC encoding")
            write_spool_record(
                spool,mapping_key(mapping),lane,payload,chunk_count,logical_bytes)


def spool_arrow(spool, mapping, raw, cfg, engine):
    routed = route_arrow(engine,mapping,raw,key_partition_count(cfg))
    spool_routed(spool,mapping,routed,cfg)


def transaction_batch_new():
    return {}


def transaction_batch_clear(batches):
    batches.clear()


def transaction_batch_flush_table(batches, table, cfg, engine, spool):
    entry = batches.pop(table,None)
    if not entry:
        return 0
    tables = entry["tables"]
    raw = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    spool_arrow(spool,entry["mapping"],raw,cfg,engine)
    return raw.num_rows


def transaction_batch_add(batches, mapping, raw, cfg, engine, spool):
    if raw.num_rows == 0:
        return 0
    table = mapping_key(mapping)
    entry = batches.get(table)
    if entry is None:
        entry = dict(mapping=mapping,tables=[],rows=0,bytes=0)
        batches[table] = entry
    entry["tables"].append(raw)
    entry["rows"] += raw.num_rows
    entry["bytes"] += raw.nbytes
    row_limit = int(cfg.get("batch_rows",50000))
    byte_limit = int(cfg.get("batch_bytes",16*1024*1024))
    if entry["rows"] >= row_limit or entry["bytes"] >= byte_limit:
        return transaction_batch_flush_table(batches,table,cfg,engine,spool)
    return 0


def transaction_batch_flush_all(batches, cfg, engine, spool):
    rows = 0
    for table in list(batches):
        rows += transaction_batch_flush_table(batches,table,cfg,engine,spool)
    return rows


def state_temp_dir(cfg):
    return os.path.dirname(cfg["state"]) or "."


def journal_disk_guard(cfg, extra_bytes=0):
    directory = state_temp_dir(cfg)
    try:
        free = int(shutil.disk_usage(directory).free)
    except OSError as exc:
        raise RuntimeError(
            "cannot verify CDC journal disk free space; fail-closed before advancing cursor"
        ) from exc
    required = int(cfg["min_free_bytes"])+max(0,int(extra_bytes))
    if free <= required:
        raise RuntimeError(
            "CDC journal disk headroom exhausted free=%d required=%d; "
            "uncommitted source transaction discarded and durable cursor retained"
            % (free,required)
        )
    return free


def transaction_spool_guard(spool, cfg, state=None, force=False):
    size = int(spool.tell())
    if size > int(cfg["txn_spool_max_bytes"]):
        raise RuntimeError(
            "source transaction spool exceeds CDC_TXN_SPOOL_MAX_BYTES "
            "bytes=%d limit=%d; durable cursor retained"
            % (size,int(cfg["txn_spool_max_bytes"]))
        )
    now = time.monotonic()
    if state is not None and not force:
        if (
            size-int(state.get("size",0)) < max(1,int(cfg["batch_bytes"]))
            and now-float(state.get("checked",0.0)) < 1.0
        ):
            return size
    if force:
        # The spool already occupies this filesystem. Before SQLite copies it into
        # WAL, reserve another spool-sized region plus bounded WAL/checkpoint slack.
        wal_slack = min(
            256*1024**2,max(64*1024**2,int(cfg["batch_bytes"])*2))
        journal_disk_guard(cfg,size+wal_slack)
    else:
        journal_disk_guard(cfg,min(int(cfg["batch_bytes"])*2,128*1024**2))
    if state is not None:
        state["size"] = size
        state["checked"] = now
    return size


def spool_rows(spool, mapping, mutations, cfg, engine=None):
    own_engine = engine is None
    engine = transform_engine(cfg) if own_engine else engine
    try:
        spool_arrow(spool,mapping,raw_arrow(mapping,mutations),cfg,engine)
    finally:
        if own_engine:
            engine.close()



def arrow_text_buffer(column):
    array = column.combine_chunks()
    if not (pa.types.is_string(array.type) or pa.types.is_large_string(array.type)):
        raise TypeError(f"expected Arrow string column, got {array.type}")
    if array.null_count:
        raise ValueError("routing key column contains NULL")
    offsets_buffer = array.buffers()[1]
    data_buffer = array.buffers()[2]
    offsets = memoryview(offsets_buffer).cast(
        "q" if pa.types.is_large_string(array.type) else "i")
    data = memoryview(data_buffer) if data_buffer is not None else memoryview(b"")
    return array,offsets,data


def insert_touched_column(con, table, column):
    array,offsets,data = arrow_text_buffer(column)
    base = array.offset
    # 250 rows = 500 host parameters, below SQLite's historical 999-variable limit.
    for start in range(0,len(array),250):
        stop = min(len(array),start+250)
        marks = ",".join(["(?,CAST(? AS TEXT))"]*(stop-start))
        params = []
        for row in range(start,stop):
            index = base+row
            params.extend((table,data[offsets[index]:offsets[index+1]]))
        con.execute(
            "INSERT OR IGNORE INTO touched(table_name,pk) VALUES "+marks,
            params)


def commit_spool(
        con, spool, position, source_time, mapping_by_name, gtid=None,
        plan_version=0, source_parts=None, source_epoch=None):
    """All rows of a committed source transaction and its read cursor commit together."""
    spool.seek(0)
    now = time.time()
    with state_transaction(con):
        old = meta_get(con, "read_position")
        if position_ge(old, position):
            return set()
        total, changed_tables = 0, set()
        source_seq = None
        if source_parts is not None:
            if not source_epoch:
                raise RuntimeError("source-state logging requires a source epoch")
            source_seq = source_state.log_commit_tx(
                con,source_epoch,position,gtid,source_parts)
        while True:
            record = read_spool_record(spool)
            if record is None:
                break
            table,lane,payload,count,logical_bytes = record
            if table not in mapping_by_name:
                raise ValueError(f"journal contains an unknown source table: {table}")
            con.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,logical_bytes,plan_version,
                    source_file,source_pos,source_seq,source_time,created)
                VALUES(?,?,'cdc',?,?,?,?,?,?,?,?,?)
            """,(table,lane,payload,count,logical_bytes,int(plan_version),
                 position[0],position[1],source_seq,source_time,now))
            total += logical_bytes
            changed_tables.add(table)
            if not con.execute("SELECT snapshot_done FROM table_state WHERE name=?", (table,)).fetchone()[0]:
                mapping = mapping_by_name[table]
                insert_touched_column(
                    con,table,arrow_job_table(mapping,payload).column("_sync_key"))
        meta_set(con, "pending_bytes", meta_get(con, "pending_bytes", 0) + total)
        cursor_advance(con, position)
        if gtid is not None:
            current = meta_get(con,"gtid_set")
            if current is None:
                raise RuntimeError("GTID transaction found without a durable initial GTID set")
            meta_set(con,"gtid_set",gtid_add(current,gtid))
    return changed_tables

def touched_keys(con, table, column):
    array,offsets,data = arrow_text_buffer(column)
    base = array.offset
    found = set()
    for start in range(0,len(array),500):
        stop = min(len(array),start+500)
        marks = ",".join(["CAST(? AS TEXT)"]*(stop-start))
        params = [table]
        for row in range(start,stop):
            index = base+row
            params.append(data[offsets[index]:offsets[index+1]])
        found.update(row[0] for row in con.execute(
            f"SELECT pk FROM touched WHERE table_name=? AND pk IN ({marks})",
            params).fetchall())
    return found


def stage_snapshot(con, mapping, rows, cursor, is_last, high, cfg, engine=None):
    """Caller captured high after the SELECT. Snapshot admission is rechecked atomically."""
    table = mapping_key(mapping)
    # Cheap precheck prevents repeated engine creation/Arrow routing while CDC catches up.
    if not position_ge(meta_get(con,"read_position"),high):
        return False
    if int(con.execute(
            "SELECT COUNT(*) FROM snapshot_groups WHERE table_name=?",
            (table,)).fetchone()[0]) >= int(cfg.get("snapshot_read_ahead_groups",1)):
        return False

    own_engine = engine is None
    engine = transform_engine(cfg) if own_engine else engine
    try:
        raw = snapshot_arrow(mapping,rows)
        routed = route_arrow(engine,mapping,raw,key_partition_count(cfg))

        # touched is monotonic until this table finishes snapshot. Build the expensive
        # compressed lane spool outside BEGIN IMMEDIATE, then verify the same key set
        # again after taking the SQLite write lock. A concurrent CDC commit therefore
        # causes a retry instead of allowing stale snapshot data to overtake it.
        seen_before = set()
        if routed.num_rows and con.execute(
                "SELECT 1 FROM touched WHERE table_name=? LIMIT 1",(table,)).fetchone():
            seen_before = touched_keys(con,table,routed.column("_sync_key"))
        filtered = routed
        if seen_before:
            filtered = routed.filter(pc.invert(pc.is_in(
                routed.column("_sync_key"),
                value_set=pa.array(tuple(seen_before),type=pa.string()))))

        with tempfile.SpooledTemporaryFile(
                max_size=1024**2,dir=state_temp_dir(cfg)) as spool:
            spool_routed(spool,mapping,filtered,cfg)
            spool.seek(0)
            with state_transaction(con):
                if not position_ge(meta_get(con,"read_position"),high):
                    return False
                if int(con.execute(
                        "SELECT COUNT(*) FROM snapshot_groups WHERE table_name=?",
                        (table,)).fetchone()[0]) >= int(
                            cfg.get("snapshot_read_ahead_groups",1)):
                    return False
                seen_now = set()
                if routed.num_rows and con.execute(
                        "SELECT 1 FROM touched WHERE table_name=? LIMIT 1",
                        (table,)).fetchone():
                    seen_now = touched_keys(con,table,routed.column("_sync_key"))
                if seen_now != seen_before:
                    return False

                group = uuid.uuid4().hex
                stage_seq = int(con.execute("""
                    SELECT COALESCE(MAX(stage_seq),0)+1
                    FROM snapshot_groups WHERE table_name=?
                """,(table,)).fetchone()[0])
                con.execute("""
                    INSERT INTO snapshot_groups(
                        id,table_name,cursor,is_last,stage_seq,plan_version)
                    VALUES(?,?,?,?,?,?)
                """,(
                    group,table,pack(cursor),int(is_last),stage_seq,
                    int(mapping.get("_plan_version",0))))
                total = 0
                while True:
                    record = read_spool_record(spool)
                    if record is None:
                        break
                    _,lane,payload,count,logical_bytes = record
                    con.execute("""
                        INSERT INTO jobs(
                            table_name,lane,kind,payload,nrows,logical_bytes,
                            plan_version,created,group_id)
                        VALUES(?,?,'snapshot',?,?,?,?,?,?)
                    """,(
                        table,lane,payload,count,logical_bytes,
                        int(mapping.get("_plan_version",0)),time.time(),group))
                    total += logical_bytes
                meta_set(
                    con,"pending_bytes",
                    meta_get(con,"pending_bytes",0)+total)
                con.execute("""
                    UPDATE table_state
                    SET staged_cursor=?,staged_done=?
                    WHERE name=?
                """,(pack(cursor),int(is_last),table))
                finish_snapshot_group(con,group)
        return True
    finally:
        if own_engine:
            engine.close()

def finish_snapshot_group(con, group):
    row = con.execute(
        "SELECT table_name FROM snapshot_groups WHERE id=?",(group,)).fetchone()
    if not row:
        return
    table = row[0]

    # Snapshot groups may now be staged ahead of visibility and different lane
    # bundles can finish out of order. Advance the public/visible cursor only
    # through the longest completed prefix of stage_seq.
    while True:
        head = con.execute("""
            SELECT id,cursor,is_last,plan_version
            FROM snapshot_groups
            WHERE table_name=?
            ORDER BY stage_seq,id
            LIMIT 1
        """,(table,)).fetchone()
        if not head:
            return
        head_id,cursor,is_last,plan_version = head
        if con.execute(
                "SELECT 1 FROM active_jobs WHERE group_id=?",
                (head_id,)).fetchone():
            return
        con.execute("""
            UPDATE table_state
            SET cursor=?,snapshot_done=?
            WHERE name=?
        """,(cursor,int(is_last),table))
        if is_last:
            con.execute("DELETE FROM touched WHERE table_name=?",(table,))
            task_generation.mark_ready_if_exists(
                con,table,int(plan_version))
        con.execute("DELETE FROM snapshot_groups WHERE id=?",(head_id,))


def assign_jobs(con, delivery, job_ids):
    con.executemany(
        "INSERT INTO job_assignments(job_id,delivery_id) VALUES(?,?)",
        ((int(job_id),delivery) for job_id in job_ids))


def prepared_budget_used(con, exclude_delivery=None):
    if exclude_delivery is None:
        payload = con.execute(
            "SELECT COALESCE(SUM(length(payload)),0) FROM load_parts WHERE visible=0"
        ).fetchone()[0]
        reserved = con.execute(
            "SELECT COALESCE(SUM(reserved_bytes),0) FROM prepare_reservations"
        ).fetchone()[0]
    else:
        payload = con.execute("""
            SELECT COALESCE(SUM(length(payload)),0) FROM load_parts
            WHERE visible=0 AND delivery_id<>?
        """,(exclude_delivery,)).fetchone()[0]
        reserved = con.execute("""
            SELECT COALESCE(SUM(reserved_bytes),0) FROM prepare_reservations
            WHERE delivery_id<>?
        """,(exclude_delivery,)).fetchone()[0]
    return int(payload or 0)+int(reserved or 0)


def prepare_reservation_estimate(cfg, logical_bytes):
    limit = int(cfg.get("max_prepared_bytes",2**63-1))
    return min(limit,max(1,int(cfg.get("batch_bytes",1)),int(logical_bytes or 0)))


def prepare_reservation_set_locked(con, delivery, reserved_bytes, cfg):
    reserved_bytes = max(0,int(reserved_bytes))
    limit = int(cfg.get("max_prepared_bytes",2**63-1))
    if reserved_bytes > limit:
        raise RuntimeError(
            "single prepared delivery exceeds CDC_MAX_PREPARED_BYTES "
            f"bytes={reserved_bytes} limit={limit}")
    if prepared_budget_used(con,delivery)+reserved_bytes > limit:
        return False
    con.execute("""
        INSERT INTO prepare_reservations(delivery_id,reserved_bytes) VALUES(?,?)
        ON CONFLICT(delivery_id) DO UPDATE SET reserved_bytes=excluded.reserved_bytes
    """,(delivery,reserved_bytes))
    return True


def prepare_reservation_release(con, delivery):
    with state_transaction(con):
        con.execute("DELETE FROM prepare_reservations WHERE delivery_id=?",(delivery,))


def prepare_requirement_get(con, delivery):
    row = con.execute("""
        SELECT required_bytes,full_prepare_attempts
        FROM prepare_requirements WHERE delivery_id=?
    """,(delivery,)).fetchone()
    if not row:
        return None,0
    return (int(row[0]) if row[0] is not None else None),int(row[1])


def prepare_requirement_attempt_locked(con, delivery):
    con.execute("""
        INSERT INTO prepare_requirements(
            delivery_id,required_bytes,full_prepare_attempts,updated)
        VALUES(?,NULL,1,?)
        ON CONFLICT(delivery_id) DO UPDATE SET
            full_prepare_attempts=prepare_requirements.full_prepare_attempts+1,
            updated=excluded.updated
    """,(delivery,time.time()))


def prepare_requirement_set_locked(con, delivery, required_bytes):
    required_bytes = max(0,int(required_bytes))
    con.execute("""
        INSERT INTO prepare_requirements(
            delivery_id,required_bytes,full_prepare_attempts,updated)
        VALUES(?,?,0,?)
        ON CONFLICT(delivery_id) DO UPDATE SET
            required_bytes=excluded.required_bytes,
            updated=excluded.updated
    """,(delivery,required_bytes,time.time()))


def claim_delivery(con, table, lane, cfg):
    with state_transaction(con):
        existing = con.execute("SELECT id FROM deliveries WHERE table_name=? AND lane=?", (table,lane)).fetchone()
        if existing:
            return existing[0]
        if con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] >= cfg.get("max_inflight_deliveries",2**31):
            return None
        if prepared_budget_used(con) >= cfg.get("max_prepared_bytes",2**63-1):
            return None
        selected, count, size, kind, plan_version = [], 0, 0, None, None
        for row in con.execute("""
            SELECT j.id,j.kind,j.nrows,j.logical_bytes,j.plan_version
            FROM active_jobs j LEFT JOIN job_assignments a ON a.job_id=j.id
            WHERE j.table_name=? AND j.lane=? AND a.job_id IS NULL
            ORDER BY j.id LIMIT 4096
        """, (table,lane)):
            job_id, job_kind, nrows, nbytes, job_version = row
            if selected and (
                    int(job_version) != int(plan_version) or job_kind != kind or
                    kind == "snapshot" or count+nrows > cfg["batch_rows"] or
                    size+nbytes > cfg["batch_bytes"]):
                break
            selected.append(job_id)
            count += nrows
            size += nbytes
            kind = job_kind
            plan_version = int(job_version)
        if not selected:
            return None
        delivery = uuid.uuid4().hex
        con.execute(
            "INSERT INTO deliveries(id,table_name,lane,plan_version) VALUES(?,?,?,?)",
            (delivery,table,lane,int(plan_version)))
        if not prepare_reservation_set_locked(
                con,delivery,prepare_reservation_estimate(cfg,size),cfg):
            con.execute("DELETE FROM deliveries WHERE id=?",(delivery,))
            return None
        assign_jobs(con,delivery,selected)
        return delivery


def pending_batch(con, table, lane, cfg):
    """Return the FIFO prefix size used to decide whether latency or size triggers a load."""
    rows = con.execute("""
        SELECT j.kind,j.created,j.nrows,j.logical_bytes,j.plan_version
        FROM active_jobs j LEFT JOIN job_assignments a ON a.job_id=j.id
        WHERE j.table_name=? AND j.lane=? AND a.job_id IS NULL
        ORDER BY j.id LIMIT 4096
    """, (table,lane)).fetchall()
    if not rows:
        return None
    kind,created,total_rows,total_bytes,plan_version = rows[0]
    if kind == "snapshot":
        return kind,created,total_rows,total_bytes,True,int(plan_version)
    full = total_rows >= cfg["batch_rows"] or total_bytes >= cfg["batch_bytes"]
    for next_kind,_,nrows,nbytes,next_version in rows[1:]:
        if int(next_version) != int(plan_version) or next_kind != kind or \
           total_rows+nrows > cfg["batch_rows"] or \
           total_bytes+nbytes > cfg["batch_bytes"]:
            full = True
            break
        total_rows += nrows
        total_bytes += nbytes
        full = full or total_rows >= cfg["batch_rows"] or total_bytes >= cfg["batch_bytes"]
    return kind,created,total_rows,total_bytes,full,int(plan_version)



def snapshot_bundle_width(cfg, runtime, table):
    partitions = key_partition_count(cfg)
    active = max(1,writer_target(runtime,table))
    # Rowset/resource pressure can temporarily lower active writers. Do not make
    # each DuckDB transform larger as a side effect; that creates a positive
    # feedback loop where memory pressure grows when concurrency is reduced.
    with runtime["control_lock"]:
        resource_cap = max(
            1,int(runtime.get("resource_writer_cap",cfg.get("writer_initial",1))))
    nominal = max(
        active,
        min(max(1,int(cfg.get("writer_initial",active))),resource_cap))
    automatic = max(1,(partitions+nominal-1)//nominal)
    return min(automatic,int(cfg.get("snapshot_bundle_max_lanes",8)))


def snapshot_transform_bytes_cap(cfg, runtime, table):
    with runtime["control_lock"]:
        return int(runtime.get("snapshot_transform_bytes_cap",{}).get(
            table,cfg["batch_bytes"]))


def snapshot_transform_oom_backoff(cfg, runtime, table, failed_bytes, retained_bytes):
    failed_bytes = max(1,int(failed_bytes))
    retained_bytes = max(1,int(retained_bytes))
    # Halve the failed logical working set but never below the retained owner
    # prefix. This adapts to row width: narrower future lanes can still bundle.
    candidate = max(retained_bytes,failed_bytes//2)
    with runtime["control_lock"]:
        caps = runtime.setdefault("snapshot_transform_bytes_cap",{})
        old = int(caps.get(table,cfg["batch_bytes"]))
        new = max(1,min(old,candidate))
        caps[table] = new
    if new < old:
        log(
            f"SNAPSHOT BUNDLE BACKOFF table={table} "
            f"logical_bytes_cap={old}->{new} failed_bytes={failed_bytes} "
            f"retained_bytes={retained_bytes}")
    return new


def cdc_bundle_width(cfg, runtime, table):
    partitions = key_partition_count(cfg)
    active = max(1,writer_target(runtime,table))
    automatic = max(1,(partitions+active-1)//active)
    return min(automatic,4)


def lane_blocking_delivery(con, table, lane):
    row = con.execute("""
        SELECT a.delivery_id
        FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
        WHERE j.table_name=? AND j.lane=?
        ORDER BY j.id LIMIT 1
    """,(table,lane)).fetchone()
    return row[0] if row else None


def claim_snapshot_bundle(con, table, primary_lane, cfg, runtime):
    """Coalesce head snapshot jobs from several logical lanes into one physical delivery.

    Logical lane identity is unchanged. Later CDC in every included lane remains blocked
    by the assigned head snapshot job until this delivery is VISIBLE and acknowledged.
    """
    with state_transaction(con):
        existing = con.execute(
            "SELECT id FROM deliveries WHERE table_name=? AND lane=?",(table,primary_lane)).fetchone()
        if existing:
            return existing[0]

        blocked = lane_blocking_delivery(con,table,primary_lane)
        if blocked is not None:
            owner = con.execute("SELECT lane FROM deliveries WHERE id=?",(blocked,)).fetchone()
            return blocked if owner and int(owner[0]) == int(primary_lane) else None

        if con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] >= cfg.get("max_inflight_deliveries",2**31):
            return None
        if prepared_budget_used(con) >= cfg.get("max_prepared_bytes",2**63-1):
            return None

        primary = con.execute("""
            SELECT id,kind,group_id,nrows,logical_bytes,plan_version
            FROM active_jobs WHERE table_name=? AND lane=? ORDER BY id LIMIT 1
        """,(table,primary_lane)).fetchone()
        if not primary or primary[1] != "snapshot" or primary[2] is None:
            return None
        primary_id,_,group_id,primary_rows,primary_bytes,plan_version = primary
        if con.execute("SELECT 1 FROM job_assignments WHERE job_id=?",(primary_id,)).fetchone():
            return None

        candidates = con.execute("""
            SELECT j.id,j.lane,j.nrows,j.logical_bytes
            FROM active_jobs j
            JOIN (
                SELECT lane,MIN(id) AS first_id
                FROM active_jobs WHERE table_name=? GROUP BY lane
            ) h ON h.lane=j.lane AND h.first_id=j.id
            LEFT JOIN job_assignments a ON a.job_id=j.id
            WHERE j.table_name=? AND j.kind='snapshot' AND j.group_id=?
              AND j.plan_version=? AND a.job_id IS NULL
        """,(table,table,group_id,int(plan_version))).fetchall()
        if not candidates:
            return None

        partitions = key_partition_count(cfg)
        candidates.sort(key=lambda r: ((int(r[1])-int(primary_lane)) % partitions, int(r[1])))
        width = snapshot_bundle_width(cfg,runtime,table)
        transform_bytes_cap = min(
            int(cfg["batch_bytes"]),
            snapshot_transform_bytes_cap(cfg,runtime,table))
        selected,rows,bytes_ = [],0,0
        for job_id,lane,nrows,nbytes in candidates:
            if len(selected) >= width:
                break
            if selected and (
                    rows+nrows > cfg["batch_rows"]
                    or bytes_+nbytes > transform_bytes_cap):
                break
            selected.append((job_id,int(lane)))
            rows += int(nrows)
            bytes_ += int(nbytes)
        if not selected or selected[0][1] != int(primary_lane):
            return None

        delivery = uuid.uuid4().hex
        con.execute(
            "INSERT INTO deliveries(id,table_name,lane,plan_version) VALUES(?,?,?,?)",
            (delivery,table,int(primary_lane),int(plan_version)))
        if not prepare_reservation_set_locked(
                con,delivery,prepare_reservation_estimate(cfg,bytes_),cfg):
            con.execute("DELETE FROM deliveries WHERE id=?",(delivery,))
            return None
        assign_jobs(con,delivery,(job_id for job_id,_ in selected))
        return delivery


def claim_cdc_bundle(con, table, primary_lane, cfg, runtime):
    """Coalesce CDC FIFO prefixes from several independent logical lanes.

    Member lanes stay durably assigned to one physical delivery, so their later jobs
    remain blocked across restart until the shared delivery is VISIBLE and acknowledged.
    """
    with state_transaction(con):
        existing = con.execute(
            "SELECT id FROM deliveries WHERE table_name=? AND lane=?",(table,primary_lane)).fetchone()
        if existing:
            return existing[0]

        blocked = lane_blocking_delivery(con,table,primary_lane)
        if blocked is not None:
            owner = con.execute("SELECT lane FROM deliveries WHERE id=?",(blocked,)).fetchone()
            return blocked if owner and int(owner[0]) == int(primary_lane) else None

        if con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] >= cfg.get("max_inflight_deliveries",2**31):
            return None
        if prepared_budget_used(con) >= cfg.get("max_prepared_bytes",2**63-1):
            return None

        heads = con.execute("""
            SELECT j.lane,j.kind,j.plan_version,a.delivery_id
            FROM active_jobs j
            JOIN (
                SELECT lane,MIN(id) AS first_id
                FROM active_jobs WHERE table_name=? GROUP BY lane
            ) h ON h.lane=j.lane AND h.first_id=j.id
            LEFT JOIN job_assignments a ON a.job_id=j.id
            WHERE j.table_name=?
        """,(table,table)).fetchall()
        partitions = key_partition_count(cfg)
        primary_head = next(
            (row for row in heads if int(row[0]) == int(primary_lane)),None)
        if not primary_head or primary_head[1] != "cdc" or primary_head[3] is not None:
            return None
        plan_version = int(primary_head[2])
        candidates = [
            int(lane) for lane,kind,version,assigned in heads
            if kind == "cdc" and int(version) == plan_version and assigned is None
        ]
        candidates.sort(key=lambda lane: ((lane-int(primary_lane)) % partitions,lane))
        if not candidates or candidates[0] != int(primary_lane):
            return None

        selected,selected_lanes,total_rows,total_bytes = [],[],0,0
        width = cdc_bundle_width(cfg,runtime,table)
        for lane in candidates:
            if len(selected_lanes) >= width:
                break
            lane_selected = []
            for job_id,kind,nrows,nbytes,job_version,assigned in con.execute("""
                SELECT j.id,j.kind,j.nrows,j.logical_bytes,j.plan_version,a.delivery_id
                FROM active_jobs j LEFT JOIN job_assignments a ON a.job_id=j.id
                WHERE j.table_name=? AND j.lane=?
                ORDER BY j.id LIMIT 4096
            """,(table,lane)):
                if assigned is not None or kind != "cdc" or int(job_version) != plan_version:
                    break
                nrows,nbytes = int(nrows),int(nbytes)
                if selected and (total_rows+nrows > cfg["batch_rows"] or
                                 total_bytes+nbytes > cfg["batch_bytes"]):
                    break
                lane_selected.append(int(job_id))
                total_rows += nrows
                total_bytes += nbytes
                if total_rows >= cfg["batch_rows"] or total_bytes >= cfg["batch_bytes"]:
                    break
            if lane_selected:
                selected.extend(lane_selected)
                selected_lanes.append(lane)
            if total_rows >= cfg["batch_rows"] or total_bytes >= cfg["batch_bytes"]:
                break

        if not selected or selected_lanes[0] != int(primary_lane):
            return None
        delivery = uuid.uuid4().hex
        con.execute(
            "INSERT INTO deliveries(id,table_name,lane,plan_version) VALUES(?,?,?,?)",
            (delivery,table,int(primary_lane),plan_version))
        if not prepare_reservation_set_locked(
                con,delivery,prepare_reservation_estimate(cfg,total_bytes),cfg):
            con.execute("DELETE FROM deliveries WHERE id=?",(delivery,))
            return None
        assign_jobs(con,delivery,selected)
        return delivery


def delivery_lanes(con, delivery):
    return [int(r[0]) for r in con.execute("""
        SELECT DISTINCT j.lane
        FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
        WHERE a.delivery_id=? ORDER BY j.lane
    """,(delivery,)).fetchall()]


def shrink_unprepared_delivery(con, delivery):
    """Reduce only the current unsent delivery after a local DuckDB OOM.

    Preserve the owner lane and FIFO prefixes. First remove bundled secondary
    lanes; if the delivery already owns one lane, halve that lane's assigned
    job prefix. Unassigned jobs remain durable and are claimed normally later.
    """
    with state_transaction(con):
        row = con.execute(
            "SELECT prepared,lane FROM deliveries WHERE id=?",(delivery,)).fetchone()
        if not row:
            raise RuntimeError(f"missing delivery {delivery} during OOM recovery")
        prepared,owner_lane = int(row[0]),int(row[1])
        if prepared:
            return dict(shrunk=False,reason="already_prepared")
        if con.execute(
                "SELECT 1 FROM load_parts WHERE delivery_id=? LIMIT 1",
                (delivery,)).fetchone():
            raise RuntimeError(
                f"cannot shrink delivery {delivery} after prepared load parts exist")

        jobs = [
            (int(job_id),int(lane),kind,int(nrows),int(nbytes))
            for job_id,lane,kind,nrows,nbytes in con.execute("""
                SELECT j.id,j.lane,j.kind,j.nrows,j.logical_bytes
                FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
                WHERE a.delivery_id=? ORDER BY j.id
            """,(delivery,)).fetchall()
        ]
        if not jobs:
            raise RuntimeError(
                f"delivery {delivery} has no active assigned jobs during OOM recovery")

        owner_jobs = [item for item in jobs if item[1] == owner_lane]
        if not owner_jobs:
            raise RuntimeError(
                f"delivery {delivery} owner lane {owner_lane} has no assigned jobs")

        secondary = [item for item in jobs if item[1] != owner_lane]
        if secondary:
            dropped = secondary
            mode = "drop_secondary_lanes"
        elif len(owner_jobs) > 1:
            keep = max(1,(len(owner_jobs)+1)//2)
            dropped = owner_jobs[keep:]
            mode = "halve_owner_prefix"
        else:
            return dict(
                shrunk=False,reason="single_job",mode="minimum",
                jobs_before=1,jobs_after=1,
                rows_before=jobs[0][3],bytes_before=jobs[0][4])

        con.executemany(
            "DELETE FROM job_assignments WHERE job_id=? AND delivery_id=?",
            ((item[0],delivery) for item in dropped))
        con.execute(
            "DELETE FROM prepare_reservations WHERE delivery_id=?",(delivery,))
        con.execute(
            "DELETE FROM prepare_requirements WHERE delivery_id=?",(delivery,))
        retained = [
            (int(job_id),int(lane),kind,int(nrows),int(nbytes))
            for job_id,lane,kind,nrows,nbytes in con.execute("""
                SELECT j.id,j.lane,j.kind,j.nrows,j.logical_bytes
                FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
                WHERE a.delivery_id=? ORDER BY j.id
            """,(delivery,)).fetchall()
        ]
        if not retained or retained[0][1] != owner_lane:
            raise RuntimeError(
                f"OOM shrink broke owner-lane FIFO delivery={delivery} "
                f"owner={owner_lane} retained={retained!r}")
        return dict(
            shrunk=True,mode=mode,
            kinds=sorted({item[2] for item in jobs}),
            jobs_before=len(jobs),jobs_after=len(retained),
            rows_before=sum(item[3] for item in jobs),
            rows_after=sum(item[3] for item in retained),
            bytes_before=sum(item[4] for item in jobs),
            bytes_after=sum(item[4] for item in retained),
            dropped_lanes=sorted({item[1] for item in dropped}),
        )


def duckdb_oom_recovery_memory_bytes(cfg):
    base = memory_limit_bytes(cfg["duckdb_memory"])
    duckdb_budget = max(
        1,int(cfg["resource"]["memory_mb"])*1024**2//2)
    # An emergency transform is short-lived and only runs after severe writer
    # backoff. Keep it bounded to one quarter of the aggregate DuckDB budget.
    return max(base,min(base*2,duckdb_budget//4))


def is_duckdb_oom(exc):
    oom_type = getattr(duckdb,"OutOfMemoryException",None)
    return (
        (oom_type is not None and isinstance(exc,oom_type))
        or exc.__class__.__name__ == "OutOfMemoryException"
    )


def acknowledge_delivery(con, delivery):
    with state_transaction(con):
        if con.execute("SELECT 1 FROM load_parts WHERE delivery_id=? AND visible=0", (delivery,)).fetchone():
            raise RuntimeError("attempt to acknowledge an invisible load")
        row = con.execute("SELECT prepared FROM deliveries WHERE id=?", (delivery,)).fetchone()
        if not row or not row[0]:
            raise RuntimeError("attempt to acknowledge an unprepared delivery")
        groups = [r[0] for r in con.execute("""
            SELECT DISTINCT j.group_id
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=? AND j.group_id IS NOT NULL
        """,(delivery,))]
        for table,lane,file_name,pos,source_seq in con.execute("""
            SELECT j.table_name,j.lane,j.source_file,j.source_pos,j.source_seq
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=? AND j.kind='cdc' ORDER BY j.id
        """, (delivery,)).fetchall():
            previous = con.execute("""
                SELECT source_seq FROM applied
                WHERE table_name=? AND lane=?
            """, (table,lane)).fetchone()
            if (
                previous and previous[0] is not None
                and source_seq is not None
                and int(source_seq) < int(previous[0])
            ):
                raise RuntimeError(
                    f"visible source sequence regression table={table} "
                    f"lane={lane} previous={previous[0]} next={source_seq}")
            con.execute("""
                INSERT INTO applied(
                    table_name,lane,source_file,source_pos,source_seq)
                VALUES(?,?,?,?,?)
                ON CONFLICT(table_name,lane) DO UPDATE SET
                    source_file=excluded.source_file,
                    source_pos=excluded.source_pos,
                    source_seq=COALESCE(excluded.source_seq,applied.source_seq)
            """, (table,lane,file_name,pos,source_seq))
        size = con.execute("""
            SELECT COALESCE(SUM(j.logical_bytes),0)
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=?
        """,(delivery,)).fetchone()[0]
        aggregate_commits = [
            (str(row[0]),int(row[1]))
            for row in con.execute("""
                SELECT DISTINCT l.consumer_id,l.source_seq
                FROM aggregate_job_links l
                JOIN job_assignments a ON a.job_id=l.job_id
                WHERE a.delivery_id=?
            """,(delivery,)).fetchall()
        ]
        join_commits = [
            (str(row[0]),int(row[1]))
            for row in con.execute("""
                SELECT DISTINCT l.consumer_id,l.source_seq
                FROM join_job_links l
                JOIN job_assignments a ON a.job_id=l.job_id
                WHERE a.delivery_id=?
            """,(delivery,)).fetchall()
        ]
        con.execute("""
            INSERT OR IGNORE INTO retired_jobs(job_id)
            SELECT job_id FROM job_assignments WHERE delivery_id=?
        """,(delivery,))
        for consumer_id,source_seq in aggregate_commits:
            if not con.execute("""
                SELECT 1
                FROM aggregate_job_links l
                LEFT JOIN retired_jobs r ON r.job_id=l.job_id
                WHERE l.consumer_id=? AND l.source_seq=?
                  AND r.job_id IS NULL
                LIMIT 1
            """,(consumer_id,source_seq)).fetchone():
                aggregate_outbox.mark_visible(
                    con,consumer_id,source_seq)
        for consumer_id,source_seq in join_commits:
            if not con.execute("""
                SELECT 1
                FROM join_job_links l
                LEFT JOIN retired_jobs r ON r.job_id=l.job_id
                WHERE l.consumer_id=? AND l.source_seq=?
                  AND r.job_id IS NULL
                LIMIT 1
            """,(consumer_id,source_seq)).fetchone():
                join_outbox.mark_visible(
                    con,consumer_id,source_seq)
        con.execute("DELETE FROM load_parts WHERE delivery_id=?", (delivery,))
        con.execute("DELETE FROM load_transactions WHERE delivery_id=?", (delivery,))
        con.execute("DELETE FROM deliveries WHERE id=?", (delivery,))
        meta_set(con, "pending_bytes", max(0, meta_get(con,"pending_bytes",0)-size))
        for group in groups:
            finish_snapshot_group(con, group)


def visible_frontiers(con, table):
    return [
        dict(
            lane=int(row[0]),
            source_file=row[1],
            source_pos=None if row[2] is None else int(row[2]),
            source_seq=None if row[3] is None else int(row[3]),
        )
        for row in con.execute("""
            SELECT lane,source_file,source_pos,source_seq
            FROM applied WHERE table_name=? ORDER BY lane
        """, (str(table),))
    ]


NATIVE_ARROW_I8 = 1
NATIVE_ARROW_U8 = 2
NATIVE_ARROW_I16 = 3
NATIVE_ARROW_U16 = 4
NATIVE_ARROW_I32 = 5
NATIVE_ARROW_U32 = 6
NATIVE_ARROW_I64 = 7
NATIVE_ARROW_U64 = 8
NATIVE_ARROW_F32 = 9
NATIVE_ARROW_F64 = 10
NATIVE_ARROW_STRING = 11
NATIVE_ARROW_BINARY = 12
NATIVE_ARROW_DATE32 = 13
NATIVE_ARROW_TIMESTAMP_US = 14
NATIVE_ARROW_DECIMAL128 = 15


def native_arrow_kind(dtype):
    if pa.types.is_int8(dtype): return NATIVE_ARROW_I8
    if pa.types.is_uint8(dtype): return NATIVE_ARROW_U8
    if pa.types.is_int16(dtype): return NATIVE_ARROW_I16
    if pa.types.is_uint16(dtype): return NATIVE_ARROW_U16
    if pa.types.is_int32(dtype): return NATIVE_ARROW_I32
    if pa.types.is_uint32(dtype): return NATIVE_ARROW_U32
    if pa.types.is_int64(dtype): return NATIVE_ARROW_I64
    if pa.types.is_uint64(dtype): return NATIVE_ARROW_U64
    if pa.types.is_float32(dtype): return NATIVE_ARROW_F32
    if pa.types.is_float64(dtype): return NATIVE_ARROW_F64
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype): return NATIVE_ARROW_STRING
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype): return NATIVE_ARROW_BINARY
    if pa.types.is_date32(dtype): return NATIVE_ARROW_DATE32
    if pa.types.is_timestamp(dtype) and dtype.unit == "us": return NATIVE_ARROW_TIMESTAMP_US
    if pa.types.is_decimal128(dtype): return NATIVE_ARROW_DECIMAL128
    raise ValueError(f"native binlog decoder has no Arrow kind for {dtype}")


def native_mapping_reason(mapping):
    unsupported = {"time","json","enum","set"}
    text_types = {
        "char","varchar","tinytext","text","mediumtext","longtext",
        "enum","set",
    }
    for column,(name,dtype) in zip(mapping["_schema_signature"],mapping["_schema"]):
        _,data_type,column_type,_,collation,_ = column[:6]
        data_type = str(data_type).lower()
        if data_type in unsupported:
            return f"{name}:{data_type} is not implemented by native decoder v1"
        if data_type == "timestamp":
            return f"{name}:timestamp is deferred until snapshot/CDC timezone normalization is proven identical"
        if data_type in ("decimal","numeric") and not pa.types.is_decimal128(dtype):
            return f"{name}:{column_type} exceeds native Decimal128 v1"
        if data_type in text_types and collation:
            charset = str(collation).lower()
            if not charset.startswith(("utf8mb4","utf8","ascii")):
                return f"{name}:collation {collation} is not UTF-8/ASCII"
        try:
            native_arrow_kind(dtype)
        except ValueError as exc:
            return f"{name}:{exc}"
    return None


def native_reader_plan(cfg, prepared):
    path = cfg.get("native_binlog_path","")
    if not path or not os.path.isfile(path) or not os.access(path,os.X_OK):
        raise RuntimeError(f"native binlog reader is required but binary is not executable: {path}")
    for mapping in source_mappings(prepared):
        why = native_mapping_reason(mapping)
        if why:
            raise RuntimeError(
                f"native binlog reader is required but {mapping['src_table']} is unsupported: {why}")
    cfg["source_reader"] = "native_c_v1"
    log(f"NATIVE BINLOG enabled decoder={path} transport=pymysql-wire->raw-event->ArrowIPC "
        f"event_group_events={cfg.get('native_event_group_events',1)}")
    return True


def native_config_payload(cfg, prepared):
    prepared = source_mappings(prepared)
    out = bytearray(struct.pack("<H",len(prepared)))
    def put_text(value):
        data = str(value).encode("utf-8")
        if len(data) > 65535:
            raise ValueError("native decoder metadata string is too long")
        out.extend(struct.pack("<H",len(data)))
        out.extend(data)
    for mapping in prepared:
        put_text(cfg["mysql"]["database"])
        put_text(mapping["src_table"])
        out.extend(struct.pack("<H",len(mapping["_schema"])))
        for column,(name,dtype) in zip(mapping["_schema_signature"],mapping["_schema"]):
            put_text(name)
            column_type = str(column[2]).lower()
            unsigned = int("unsigned" in column_type)
            precision = scale = 0
            if pa.types.is_decimal128(dtype):
                precision,scale = int(dtype.precision),int(dtype.scale)
            out.extend(bytes((native_arrow_kind(dtype),unsigned,precision,scale)))
    return bytes(out)


def source_type(data_type, column_type):
    name = data_type.lower()
    unsigned = "unsigned" in column_type.lower()
    integers = {"tinyint":(pa.int8,pa.uint8), "smallint":(pa.int16,pa.uint16),
                "mediumint":(pa.int32,pa.uint32), "int":(pa.int32,pa.uint32),
                "bigint":(pa.int64,pa.uint64)}
    if name in integers:
        return integers[name][int(unsigned)]()
    if name == "float":
        return pa.float32()
    if name in ("double","real"):
        return pa.float64()
    if name in ("decimal","numeric"):
        match = re.search(r"\((\d+),\s*(\d+)\)", column_type)
        precision, scale = map(int, match.groups()) if match else (65,0)
        return pa.decimal128(precision,scale) if precision <= 38 else pa.large_string()
    if name in ("datetime","timestamp"):
        return pa.timestamp("us")
    if name == "date":
        return pa.date32()
    if name == "year":
        return pa.int16()
    if name in ("tinyblob","blob","mediumblob","longblob","binary","varbinary","bit",
                "geometry","point","linestring","polygon","multipoint","multilinestring",
                "multipolygon","geometrycollection"):
        return pa.large_binary()
    return pa.large_string()


def string_value(value):
    if isinstance(value, (dict,list)):
        return orjson.dumps(json_value(value)).decode()
    if isinstance(value, (set,frozenset)):
        return ",".join(sorted(value))
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, datetime.timedelta):
        micros = value // datetime.timedelta(microseconds=1)
        sign = "-" if micros < 0 else ""
        seconds, frac = divmod(abs(micros),1000000)
        hours, rest = divmod(seconds,3600)
        minutes, seconds = divmod(rest,60)
        return f"{sign}{hours:02}:{minutes:02}:{seconds:02}" + (f".{frac:06}" if frac else "")
    return value


def json_value(value):
    if isinstance(value,bytes):
        return value.decode("utf-8")
    if isinstance(value,dict):
        return {json_value(k):json_value(v) for k,v in value.items()}
    if isinstance(value,list):
        return [json_value(v) for v in value]
    return value


ARROW_JOB_MAGIC = b"ARW2IPC0"
# Arrow IPC stores the codec in stream metadata, so new LZ4 jobs remain readable
# beside existing ZSTD jobs. LZ4 materially reduces both journal encode and
# delivery decode CPU for wide CDC rows while preserving compression.
ARROW_JOB_COMPRESSION = (
    "lz4" if pa.Codec.is_available("lz4_frame") else "zstd")
if ARROW_JOB_COMPRESSION == "zstd" and not pa.Codec.is_available("zstd"):
    raise RuntimeError("PyArrow requires LZ4 or ZSTD IPC compression")
ARROW_JOB_WRITE_OPTIONS = pa.ipc.IpcWriteOptions(compression=ARROW_JOB_COMPRESSION)


def source_arrow_schema(mapping):
    schema = mapping.get("_source_arrow_schema")
    if schema is None:
        schema = pa.schema([pa.field(name,dtype) for name,dtype in mapping["_schema"]])
        mapping["_source_arrow_schema"] = schema
    return schema


def raw_arrow(mapping, mutations):
    if not mutations:
        fields = list(source_arrow_schema(mapping))
        fields += [pa.field("_sync_op",pa.int8()),pa.field("_sync_order",pa.int64())]
        return pa.Table.from_batches([],schema=pa.schema(fields))
    ops,rows = zip(*mutations)
    try:
        # Common CDC path: let Arrow's native row builder consume Python dictionaries once,
        # instead of scanning the same batch once per source column in Python.
        table = pa.Table.from_pylist(list(rows),schema=source_arrow_schema(mapping))
    except (TypeError,ValueError):
        # Rare source values such as MySQL TIME, SET or JSON objects need textual
        # normalization before Arrow can honor a large_string source schema.
        string_columns = [
            name for name,dtype in mapping["_schema"] if pa.types.is_large_string(dtype)]
        normalized = []
        for row in rows:
            copy = dict(row)
            for name in string_columns:
                if copy[name] is not None:
                    copy[name] = string_value(copy[name])
            normalized.append(copy)
        table = pa.Table.from_pylist(normalized,schema=source_arrow_schema(mapping))
    return table.append_column("_sync_op",pa.array(ops,type=pa.int8())).append_column(
        "_sync_order",pa.array(range(len(rows)),type=pa.int64()))


def snapshot_arrow(mapping, rows):
    if isinstance(rows,pa.Table):
        return rows
    if not rows:
        return raw_arrow(mapping,[])
    if isinstance(rows[0],dict):
        return raw_arrow(mapping,[(0,row) for row in rows])
    columns = list(zip(*rows))
    arrays = []
    for index,(name,dtype) in enumerate(mapping["_schema"]):
        values = columns[index]
        if pa.types.is_large_string(dtype):
            values = [string_value(value) for value in values]
        arrays.append(pa.array(values,type=dtype))
    arrays += [
        pa.array([0]*len(rows),type=pa.int8()),
        pa.array(range(len(rows)),type=pa.int64()),
    ]
    return pa.Table.from_arrays(
        arrays,names=[name for name,_ in mapping["_schema"]]+["_sync_op","_sync_order"])


def routing_key_sql(mapping):
    cached = mapping.get("_routing_key_sql")
    if cached:
        return cached
    types = dict(mapping["_schema"])
    parts = []
    for name in pk_columns(mapping):
        dtype = types[name]
        column = sql_name(name)
        if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
            value = f"hex({column})"
            tag = "binary"
        else:
            value = f"CAST({column} AS VARCHAR)"
            tag = str(dtype)
        parts.append(
            f"concat({sql_text(tag)},':',CAST(octet_length(encode({value})) AS VARCHAR),':',{value})")
    mapping["_routing_key_sql"] = "concat_ws(chr(31),"+",".join(parts)+")"
    return mapping["_routing_key_sql"]


def route_arrow(engine, mapping, raw, partitions):
    keys = pk_columns(mapping)
    for name in keys:
        if raw.column(name).null_count:
            raise ValueError(f"{mapping['src_table']}: NULL primary key")
    key_sql = routing_key_sql(mapping)

    # For narrow rows DuckDB's single pass is cheaper than building/sorting a
    # sidecar. The measured crossover is near 100 B/row; keep a conservative
    # margin so ordinary narrow tables never pay the wide-row optimization cost.
    if not raw.num_rows or raw.nbytes < 128*raw.num_rows:
        engine.register("_sync_route",raw)
        try:
            return engine.execute(f"""
                SELECT *,CAST(hash("_sync_key") % {int(partitions)} AS USMALLINT) AS "_sync_lane"
                FROM (
                    SELECT *,{key_sql} AS "_sync_key"
                    FROM _sync_route
                )
            """).to_arrow_table()
        finally:
            engine.unregister("_sync_route")

    # Routing depends only on the source PK. Keep multi-KiB payload columns in Arrow
    # instead of copying them through DuckDB just to calculate key/lane metadata.
    row_index = "_sync_route_row_index"
    while row_index in raw.column_names:
        row_index = "_" + row_index
    narrow = raw.select(keys).append_column(
        row_index,pa.array(range(raw.num_rows),type=pa.int64()))
    engine.register("_sync_route",narrow)
    try:
        routed = engine.execute(f"""
            SELECT {sql_name(row_index)},"_sync_key",
                   CAST(hash("_sync_key") % {int(partitions)} AS USMALLINT) AS "_sync_lane"
            FROM (
                SELECT *,{key_sql} AS "_sync_key"
                FROM _sync_route
            )
        """).to_arrow_table()
    finally:
        engine.unregister("_sync_route")

    # SQL row order is not part of the contract. Restore the Arrow input order
    # explicitly before appending metadata so row events stay aligned.
    routed = routed.sort_by([(row_index,"ascending")])
    return raw.append_column("_sync_key",routed.column("_sync_key")).append_column(
        "_sync_lane",routed.column("_sync_lane"))


def journal_columns(mapping):
    return [name for name,_ in mapping["_schema"]]+[
        "_sync_op","_sync_order","_sync_key","_sync_lane"]


def arrow_table_payload(table):
    sink = pa.BufferOutputStream()
    sink.write(ARROW_JOB_MAGIC)
    with pa.ipc.new_stream(
            sink,table.schema,options=ARROW_JOB_WRITE_OPTIONS) as writer:
        writer.write_table(table)
    # ZSTD-compressed Arrow IPC shrinks the durable SQLite/WAL journal while
    # remaining transparently readable alongside older uncompressed ARW2IPC0 jobs.
    return memoryview(sink.getvalue())


def arrow_job_payload(mapping, mutations, cfg, engine):
    return arrow_table_payload(
        route_arrow(engine,mapping,raw_arrow(mapping,mutations),key_partition_count(cfg)))


def arrow_job_table(mapping, payload):
    view = memoryview(payload)
    if len(view) < len(ARROW_JOB_MAGIC) or view[:len(ARROW_JOB_MAGIC)].tobytes() != ARROW_JOB_MAGIC:
        raise RuntimeError(
            f"{mapping['src_table']}: invalid Arrow journal payload; legacy pickle jobs are unsupported")
    buffer = pa.py_buffer(view).slice(len(ARROW_JOB_MAGIC))
    table = pa.ipc.open_stream(pa.BufferReader(buffer)).read_all()
    if table.column_names != journal_columns(mapping):
        raise RuntimeError(
            f"{mapping['src_table']}: Arrow journal columns differ from the checked source schema")
    if not pa.types.is_integer(table.schema.field("_sync_op").type) or \
       not pa.types.is_integer(table.schema.field("_sync_order").type) or \
       not pa.types.is_string(table.schema.field("_sync_key").type) or \
       not pa.types.is_integer(table.schema.field("_sync_lane").type):
        raise RuntimeError(f"{mapping['src_table']}: invalid Arrow routing metadata types")
    return table


def arrow_payload_logical_bytes(payload):
    view = memoryview(payload)
    if len(view) < len(ARROW_JOB_MAGIC) or view[:len(ARROW_JOB_MAGIC)].tobytes() != ARROW_JOB_MAGIC:
        return len(view)
    buffer = pa.py_buffer(view).slice(len(ARROW_JOB_MAGIC))
    with pa.ipc.open_stream(buffer) as reader:
        table = reader.read_all()
    return max(int(table.nbytes),len(view))


def concat_job_tables(mapping, payloads):
    tables = []
    offset = 0
    for payload in payloads:
        table = arrow_job_table(mapping,payload)
        if table.num_rows:
            order_index = table.schema.get_field_index("_sync_order")
            # Job id is the durable FIFO. Rebuild one dense delivery-local sequence so
            # source-batch/lane-local sparse order values can never cross transaction boundaries.
            order = pa.array(range(offset,offset+table.num_rows),type=pa.int64())
            table = table.set_column(order_index,"_sync_order",order)
            offset += table.num_rows
        tables.append(table)
    if not tables:
        raise RuntimeError(f"{mapping['src_table']}: delivery has no durable jobs")
    return pa.concat_tables(tables)



def validate_mapping(mapping):
    keys = pk_columns(mapping)
    if not keys or len(set(keys)) != len(keys):
        raise ValueError("primary_key must be a column name or a nonempty list")
    tree = sqlglot.parse_one(mapping.get("sql") or "SELECT * FROM arrow_batch", read="duckdb")
    if not isinstance(tree, exp.Select):
        raise ValueError("only one SELECT from arrow_batch is supported")
    forbidden = (
        (exp.Join, exp.Union, exp.AggFunc, exp.Window, exp.Group,
         exp.Having, exp.Limit, exp.Offset, exp.Distinct, exp.With,
         exp.Unnest, exp.Explode)
        if mapping.get("_catalog_compiled")
        else
        (exp.Join, exp.Subquery, exp.Union, exp.AggFunc, exp.Window, exp.Group,
         exp.Having, exp.Limit, exp.Offset, exp.Distinct, exp.With,
         exp.Unnest, exp.Explode)
    )
    if any(isinstance(node,forbidden) for node in tree.walk()):
        raise ValueError("phase 1 supports deterministic row projection/filter only; stateful SQL is rejected")
    if mapping.get("_catalog_compiled"):
        # Only the catalog compiler may introduce nested SELECTs. Every physical
        # table reference must still resolve to exactly one Arrow source.
        tables = list(tree.find_all(exp.Table))
        if len(tables) != 1 or tables[0].name.lower() != "arrow_batch":
            raise ValueError("compiled catalog plan must resolve to exactly one arrow_batch source")
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1 or tables[0].name.lower() != "arrow_batch":
        raise ValueError("SQL must read only arrow_batch")
    if any(isinstance(p,exp.Star) for p in tree.expressions):
        projections = []
        for projection in tree.expressions:
            if isinstance(projection,exp.Star):
                if any(projection.args.values()):
                    raise ValueError("star modifiers are unsupported; list the columns explicitly")
                projections.extend(exp.column(name,quoted=True) for name,_ in mapping["_schema"])
            else:
                projections.append(projection)
        tree.set("expressions",projections)
    for name in keys:
        if not any(isinstance(p,exp.Star) or
                   isinstance(p,exp.Column) and p.name == name for p in tree.expressions):
            raise ValueError(f"primary key {name!r} must be projected unchanged")
    sql = tree.sql(dialect="duckdb")
    if re.search(r"\b(random|uuid|now|current_timestamp|current_date|current_time|read_\w+|query|"
                 r"unnest|explode|generate_series|json_each|regexp_split_to_table)\b",
                 sql, re.I):
        raise ValueError("non-deterministic or external SQL functions are not supported")
    condition = (mapping.get("full_filter") or "").strip()
    condition = re.sub(r"^(where|and)\s+","",condition,flags=re.I)
    if condition:
        predicate = sqlglot.parse_one(condition,read="mysql")
        if any(isinstance(n,(exp.Subquery,exp.Select)) for n in predicate.walk()):
            raise ValueError("full_filter must be a row predicate")
        if re.search(r"\b(now|current_timestamp|current_date|current_time|rand|uuid)\b",condition,re.I):
            raise ValueError("full_filter must be deterministic")
        condition = predicate.sql(dialect="duckdb")
    mapping["_filter_sql"] = condition
    mapping["_sql"] = sql
    mapping["_direct_output_sources"] = {}
    for projection in tree.expressions:
        if isinstance(projection,exp.Column):
            mapping["_direct_output_sources"][str(projection.alias_or_name)] = projection.name
        elif isinstance(projection,exp.Alias) and isinstance(projection.this,exp.Column):
            mapping["_direct_output_sources"][str(projection.alias_or_name)] = projection.this.name
    mapping["_arrow_columns"] = (
        [p.name for p in tree.expressions]
        if all(isinstance(p,exp.Column) for p in tree.expressions) and
           not tree.args.get("where") and not condition and not tree.args.get("order") else None)
    # Metadata follows each row through the supported, cardinality-preserving projection.
    tree.select(exp.column("_sync_op"),exp.column("_sync_order"),copy=False)
    mapping["_delta_sql"] = tree.sql(dialect="duckdb")
    return mapping


def sql_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def target_byte_limit(type_sql):
    value = str(type_sql or "").strip().lower()
    match = re.match(r"^(?:char|varchar|string|binary|varbinary)\s*\((\d+)\)",value)
    if match:
        return int(match.group(1))
    if value == "string":
        return 65533
    if value.startswith("json"):
        return 16*1024*1024
    return None


def compile_target_constraints(mapping, target_columns):
    keys = set(pk_columns(mapping))
    constraints = {}
    binary_outputs = set(mapping.get("_binary_output_columns",()))
    for name in mapping["_output_columns"]:
        row = target_columns[name]
        type_sql = str(row[1])
        limit = target_byte_limit(type_sql)
        if limit is None:
            continue
        constraints[name] = dict(
            target_type=type_sql,
            limit_bytes=int(limit),
            nullable=str(row[2]).upper() == "YES",
            primary_key=name in keys,
            value_encoding=("base64" if name in binary_outputs else
                            "json" if str(type_sql).lower().startswith("json") else "utf8"),
        )
    return constraints


def output_wire_expr(name, json_columns, binary_columns):
    column = sql_name(name)
    if name in binary_columns:
        return f"to_base64({column})"
    if name in json_columns:
        return f"json({column})"
    return column


def guarded_output_sql(mapping, names, json_columns, binary_columns):
    constraints = mapping.get("_target_constraints",{})
    size_columns = mapping.get("_size_columns",{})
    fields, overflow_entries, overflow_conditions = [], [], []
    for name in names:
        column = sql_name(name)
        wire = output_wire_expr(name,json_columns,binary_columns)
        wire_size = f"utf8len(CAST({wire} AS VARCHAR))"
        raw_size = f"bloblen({column})" if name in binary_columns else wire_size
        constraint = constraints.get(name)
        if not constraint:
            fields.append(f"{column} := {wire}")
        else:
            limit = int(constraint["limit_bytes"])
            condition = f"{column} IS NOT NULL AND {wire_size}>{limit}"
            overflow_conditions.append(condition)
            fatal = bool(constraint["primary_key"] or not constraint["nullable"])
            safe = wire if fatal else f"CASE WHEN {condition} THEN NULL ELSE {wire} END"
            fields.append(f"{column} := {safe}")
            overflow_entries.append(
                f"CASE WHEN {condition} THEN struct_pack("
                f"column_name := {sql_text(name)},"
                f"target_type := {sql_text(constraint['target_type'])},"
                f"actual_bytes := CAST({wire_size} AS BIGINT),"
                f"raw_bytes := CAST({raw_size} AS BIGINT),"
                f"limit_bytes := CAST({limit} AS BIGINT),"
                f"value_encoding := {sql_text(constraint['value_encoding'])},"
                f"value_text := CAST({wire} AS VARCHAR),"
                f"fatal := {'TRUE' if fatal else 'FALSE'}) ELSE NULL END"
            )
        size_name = size_columns.get(name)
        if size_name:
            fields.append(
                f"{sql_name(size_name)} := CASE WHEN {column} IS NULL THEN NULL "
                f"ELSE CAST({raw_size} AS BIGINT) END")
    overflow_json = ("to_json(list_filter(["+",".join(overflow_entries)+
                     "], x -> x IS NOT NULL))" if overflow_entries else "'[]'")
    overflow_condition = (
        " OR ".join(f"({condition})" for condition in overflow_conditions)
        if overflow_conditions else None
    )
    return fields,overflow_json,overflow_condition


def transform_engine(cfg, macros=None, udfs=None):
    con = duckdb.connect(config={"threads":1,"memory_limit":cfg["duckdb_memory"]})
    con.execute("CREATE MACRO bloblen(col) AS octet_length(col)")
    con.execute("CREATE MACRO utf8len(col) AS octet_length(encode(CAST(col AS VARCHAR)))")
    con.execute("""CREATE MACRO truncate_exceed_byte_limit_str(col,limit_len:=1048576)
                   AS CASE WHEN utf8len(col)<=limit_len THEN col ELSE NULL END""")
    con.execute("""CREATE MACRO truncate_exceed_byte_limit_json(col,limit_len:=16777216)
                   AS truncate_exceed_byte_limit_str(col,limit_len)""")
    con.execute("""CREATE MACRO format_json(col) AS
                   CASE WHEN json_valid(truncate_exceed_byte_limit_json(col))
                   THEN json(col) ELSE NULL END""")
    for statement in (
            cfg.get("catalog_macros",()) if macros is None else macros):
        con.execute(statement)
    for spec in (cfg.get("catalog_udfs",()) if udfs is None else udfs):
        namespace = {"__builtins__":__builtins__}
        exec(compile(
            spec["source"],spec.get("filename") or "<cdc-arrow-udf>","exec"),
            namespace,namespace)
        function = namespace.get(spec["function"])
        if not callable(function):
            raise ValueError(
                f"Arrow UDF {spec['name']}: function {spec['function']!r} not found")
        parameters = [
            duckdb.sqltype(item) for item in spec.get("parameters",())]
        return_type = duckdb.sqltype(spec["return_type"])
        con.create_function(
            spec["name"],function,parameters,return_type,
            type="arrow",side_effects=False)
    return con


def native_json_mode():
    mode = os.environ.get("CDC_NATIVE_JSON","auto").strip().lower()
    if mode not in ("auto","off","required"):
        raise ValueError("CDC_NATIVE_JSON must be auto, off, or required")
    return mode


def native_json_kind(dtype):
    if pa.types.is_int8(dtype): return J4_JSON_I8,0
    if pa.types.is_uint8(dtype): return J4_JSON_U8,0
    if pa.types.is_int16(dtype): return J4_JSON_I16,0
    if pa.types.is_uint16(dtype): return J4_JSON_U16,0
    if pa.types.is_int32(dtype): return J4_JSON_I32,0
    if pa.types.is_uint32(dtype): return J4_JSON_U32,0
    if pa.types.is_int64(dtype): return J4_JSON_I64,0
    if pa.types.is_uint64(dtype): return J4_JSON_U64,0
    if pa.types.is_string(dtype): return J4_JSON_STRING,0
    if pa.types.is_large_string(dtype):
        return J4_JSON_STRING,J4_JSON_FLAG_LARGE_OFFSETS
    if pa.types.is_binary(dtype): return J4_JSON_BINARY,0
    if pa.types.is_large_binary(dtype):
        return J4_JSON_BINARY,J4_JSON_FLAG_LARGE_OFFSETS
    return None


def native_json_fast_path_reason(
        mapping, output, names, json_columns, binary_columns, winner_ready):
    if native_json_mode() == "off":
        return "disabled"
    if not winner_ready:
        return "winner selection is not available outside DuckDB"
    if mapping.get("_arrow_columns") is None:
        return "projection requires DuckDB"
    if mapping.get("_target_constraints"):
        return "target overflow guards require DuckDB"
    if mapping.get("_size_columns"):
        return "synthetic size columns require DuckDB"
    if json_columns:
        return "native StarRocks JSON columns are deferred"
    for name in names:
        dtype = output.schema.field(name).type
        spec = native_json_kind(dtype)
        if spec is None:
            return f"{name}:{dtype} is not supported by native JSON v1"
        if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
            if name not in binary_columns:
                return f"{name}:binary output is not marked for Base64 transport"
        elif name in binary_columns:
            return f"{name}:non-binary column is marked as binary output"
    if not pa.types.is_int8(output.schema.field("_sync_op").type):
        return "_sync_op must be int8"
    lib = native_partition_library()
    if lib is None:
        return "native library is unavailable"
    features = int(lib.j4_native_feature_bits())
    if not features & J4_NATIVE_FEATURE_JSON_ENCODER:
        return f"native JSON feature missing bits=0x{features:x}"
    return None


def native_json_column(array, output_name, base64_binary=False):
    spec = native_json_kind(array.type)
    if spec is None:
        raise ValueError(f"native JSON unsupported Arrow type {array.type}")
    kind,flags = spec
    if kind == J4_JSON_BINARY:
        if not base64_binary:
            raise ValueError(
                f"native JSON binary column {output_name!r} requires Base64")
        flags |= J4_JSON_FLAG_BASE64

    buffers = array.buffers()
    validity = buffers[0]
    values = buffers[1] if len(buffers) > 1 else None
    data = buffers[2] if len(buffers) > 2 else None
    if values is None:
        raise ValueError(
            f"native JSON column {output_name!r} has no values buffer")

    key_bytes = orjson.dumps(output_name)
    key_buffer = ctypes.create_string_buffer(key_bytes)
    descriptor = J4NativeJsonColumn(
        ctypes.c_void_p(int(validity.address)) if validity is not None else None,
        ctypes.c_void_p(int(values.address)),
        ctypes.c_void_p(int(data.address)) if data is not None and data.size else None,
        ctypes.c_void_p(ctypes.addressof(key_buffer)),
        ctypes.c_uint64(int(array.offset)),
        ctypes.c_uint32(len(key_bytes)),
        ctypes.c_uint32(kind),
        ctypes.c_uint32(flags),
        ctypes.c_uint32(0),
    )
    return descriptor,key_buffer,buffers


def native_json_lines(batch, names, binary_columns, sequence, include_sequence):
    global _NATIVE_JSON_LOGGED
    lib = native_partition_library()
    if lib is None:
        raise RuntimeError("native JSON library is unavailable")
    descriptors = []
    keepalive = []
    for name in names:
        array = batch.column(batch.schema.get_field_index(name))
        descriptor,key_buffer,buffers = native_json_column(
            array,name,name in binary_columns)
        descriptors.append(descriptor)
        keepalive.extend((array,key_buffer,buffers))
    op = batch.column(batch.schema.get_field_index("_sync_op"))
    descriptor,key_buffer,buffers = native_json_column(op,"__op",False)
    descriptors.append(descriptor)
    keepalive.extend((op,key_buffer,buffers))

    native_columns = (J4NativeJsonColumn*len(descriptors))(*descriptors)
    nrows = int(batch.num_rows)
    measured = ctypes.c_uint64()
    rc = lib.j4_json_measure(
        native_columns,ctypes.c_uint32(len(descriptors)),
        ctypes.c_uint64(nrows),ctypes.c_uint32(bool(include_sequence)),
        ctypes.c_uint64(int(sequence)),ctypes.byref(measured))
    if rc:
        raise RuntimeError(f"native JSON measure failed rc={rc}")

    offsets = pa.allocate_buffer((nrows+1)*8)
    data = pa.allocate_buffer(max(1,int(measured.value)))
    used = ctypes.c_uint64()
    rc = lib.j4_json_encode(
        native_columns,ctypes.c_uint32(len(descriptors)),
        ctypes.c_uint64(nrows),ctypes.c_uint32(bool(include_sequence)),
        ctypes.c_uint64(int(sequence)),ctypes.c_void_p(int(offsets.address)),
        ctypes.c_void_p(int(data.address)),ctypes.c_uint64(int(measured.value)),
        ctypes.byref(used))
    if rc:
        raise RuntimeError(
            f"native JSON encode failed rc={rc} capacity={measured.value}")
    if int(used.value) != int(measured.value):
        raise RuntimeError(
            "native JSON measure/encode byte mismatch "
            f"measured={measured.value} used={used.value}")
    if not _NATIVE_JSON_LOGGED:
        _NATIVE_JSON_LOGGED = True
        log(
            "NATIVE JSON enabled abi=3 "
            "types=integer,string,binary-base64 wire=starrocks-json-lines")
    return pa.Array.from_buffers(
        pa.large_string(),nrows,[None,offsets,data])


def transformed_line_batches(
        con, mapping, batch, sequence=0, collect_overflow=False,
        delivery_dense_order=False, batch_rows=32768):
    """Yield encoded JSON line arrays in bounded Arrow RecordBatches.

    The final DuckDB JSON projection is consumed with RecordBatchReader instead
    of materializing one full Arrow table. Upstream typed transforms and winner
    selection keep their existing semantics; only the final wire-format result
    is streamed.
    """
    raw = batch if isinstance(batch,pa.Table) else raw_arrow(mapping,batch)
    if raw.num_rows == 0:
        return
    raw_registered = False
    condition = mapping["_filter_sql"]
    cte = "WITH arrow_batch AS (SELECT * FROM _sync_raw" + (
        f" WHERE {condition}" if condition else "") + ") "
    try:
        json_columns = set(mapping.get("_target_json_columns",()))
        binary_columns = set(mapping.get("_binary_output_columns",()))
        if mapping["_arrow_columns"] is not None:
            output = raw.select(mapping["_arrow_columns"]+["_sync_op","_sync_order"])
        else:
            con.register("_sync_raw",raw)
            raw_registered = True
            result = con.execute(cte + mapping["_delta_sql"])
            json_columns.update(
                d[0] for d in result.description if str(d[1]) == "JSON")
            output = result.to_arrow_table()

        names = [
            n for n in output.column_names
            if n not in ("_sync_op","_sync_order")]
        if len(set(names)) != len(names) or any(
                n.startswith("_sync_") for n in names):
            raise ValueError("duplicate or reserved output column name")

        keys = ", ".join(sql_name(k) for k in pk_columns(mapping))
        wide_plan = output.nbytes >= 192*output.num_rows
        direct_arrow_winners = (
            delivery_dense_order
            and mapping["_arrow_columns"] is not None
            and "_sync_key" in raw.column_names)
        winner_ready = False

        if direct_arrow_winners:
            winner_keys = pk_columns(mapping)
            grouped = raw.select(
                winner_keys+["_sync_order"]).group_by(winner_keys).aggregate(
                    [("_sync_order","max")])
            if grouped.num_rows == raw.num_rows:
                output = raw.select(
                    mapping["_arrow_columns"]+["_sync_op","_sync_order"])
            else:
                order_name = next(
                    name for name in grouped.column_names
                    if name not in winner_keys)
                winners = grouped.column(order_name).combine_chunks()
                output = raw.take(winners).select(
                    mapping["_arrow_columns"]+["_sync_op","_sync_order"])
            winner_ready = True
        elif wide_plan:
            row_index = "_sync_row_index"
            output = output.append_column(
                row_index,pa.array(range(output.num_rows),type=pa.int64()))
            con.register("_sync_output",output)
            winners = con.execute(f"""
                SELECT {sql_name(row_index)}
                FROM _sync_output
                QUALIFY row_number() OVER(
                    PARTITION BY {keys} ORDER BY "_sync_order" DESC
                )=1
            """).to_arrow_table().column(row_index).combine_chunks()
            con.unregister("_sync_output")
            output = output.drop([row_index]).take(winners)
            winner_ready = True

        native_reason = native_json_fast_path_reason(
            mapping,output,names,json_columns,binary_columns,winner_ready)
        if native_reason is None:
            for output_batch in output.to_batches(
                    max_chunksize=max(1,int(batch_rows))):
                yield native_json_lines(
                    output_batch,names,binary_columns,sequence,
                    bool(mapping.get("_target_sequence"))),[]
            return
        if native_json_mode() == "required":
            raise RuntimeError(
                f"{mapping['src_table']}: native JSON required but unavailable: "
                f"{native_reason}")

        con.register("_sync_output",output)
        guarded_fields,overflow_json,overflow_condition = guarded_output_sql(
            mapping,names,json_columns,binary_columns)
        fields = ", ".join(guarded_fields)
        fields += ', "__op" := "_sync_op"'
        if mapping.get("_target_sequence"):
            fields += f', "_cdc_seq" := {int(sequence)}'
        pk_fields = ", ".join(
            f"{sql_name(k)} := {sql_name(k)}" for k in pk_columns(mapping))

        overflow_rules = overflow_condition is not None
        line_sql = f"to_json(struct_pack({fields})) || chr(10) AS line"
        overflow_sql = (
            f", CASE WHEN {overflow_condition} THEN {overflow_json} ELSE NULL END AS overflow_json, "
            f"CASE WHEN {overflow_condition} THEN "
            f"to_json(struct_pack({pk_fields})) ELSE NULL END AS overflow_pk, "
            f'CASE WHEN {overflow_condition} THEN "_sync_order" ELSE NULL END AS overflow_order'
            if collect_overflow and overflow_rules else "")
        if wide_plan:
            query = f"SELECT {line_sql}{overflow_sql} FROM _sync_output"
        else:
            query = f"""
                SELECT {line_sql}{overflow_sql}
                FROM _sync_output
                QUALIFY row_number() OVER(
                    PARTITION BY {keys} ORDER BY "_sync_order" DESC
                )=1
            """

        reader = con.execute(query).to_arrow_reader(max(1,int(batch_rows)))
        for result_batch in reader:
            line_index = result_batch.schema.get_field_index("line")
            lines = result_batch.column(line_index)
            if not collect_overflow or not overflow_rules:
                yield lines,[]
                continue

            table = pa.Table.from_batches([result_batch])
            mask = pc.is_valid(table.column("overflow_json"))
            exceptional = table.filter(mask).select(
                ["overflow_json","overflow_pk","overflow_order"]).to_pylist()
            overflows = []
            for row in exceptional:
                for item in orjson.loads(row["overflow_json"]):
                    item["pk_json"] = row["overflow_pk"]
                    item["row_order"] = int(row["overflow_order"])
                    overflows.append(item)
            yield lines,overflows
    finally:
        if raw_registered:
            con.unregister("_sync_raw")
        try:
            con.unregister("_sync_output")
        except duckdb.InvalidInputException:
            pass


def transformed_lines(
        con, mapping, batch, sequence=0, collect_overflow=False,
        delivery_dense_order=False):
    """Compatibility wrapper that materializes streamed line batches.

    Production prepare_delivery() consumes transformed_line_batches() directly.
    Tests and callers that need the historical ChunkedArray API keep identical
    behavior through this wrapper.
    """
    chunks = []
    overflows = []
    stream = transformed_line_batches(
        con,mapping,batch,sequence=sequence,
        collect_overflow=collect_overflow,
        delivery_dense_order=delivery_dense_order)
    try:
        for lines,batch_overflows in stream:
            chunks.append(lines)
            if collect_overflow and batch_overflows:
                overflows.extend(batch_overflows)
    finally:
        stream.close()

    result = (
        pa.chunked_array(chunks)
        if chunks else pa.chunked_array([],type=pa.string()))
    return (result,overflows) if collect_overflow else result


def persist_field_overflows(con, mapping, delivery, overflows, jobs):
    source = next((job for job in reversed(jobs) if job[2] is not None),None)
    source_file = source[2] if source else None
    source_pos = source[3] if source else None
    source_time = source[4] if source else None
    now = time.time()

    with state_transaction(con):
        con.execute("DELETE FROM field_overflow WHERE delivery_id=?",(delivery,))
        for item in overflows:
            value = str(item["value_text"]).encode("utf-8")
            action = "stop" if item["fatal"] else "null"
            con.execute("""
                INSERT INTO field_overflow(
                    delivery_id,table_name,row_order,pk_json,column_name,target_type,
                    actual_bytes,raw_bytes,limit_bytes,value_encoding,value,value_sha256,
                    action,source_file,source_pos,source_time,created
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,(
                delivery,mapping_key(mapping),int(item["row_order"]),str(item["pk_json"]),
                str(item["column_name"]),str(item["target_type"]),int(item["actual_bytes"]),
                int(item["raw_bytes"]) if item.get("raw_bytes") is not None else None,
                int(item["limit_bytes"]),str(item["value_encoding"]),value,
                hashlib.sha256(value).hexdigest(),action,source_file,source_pos,source_time,now
            ))

    for item in overflows[:5]:
        log(
            f"FIELD OVERFLOW table={mapping_key(mapping)} pk={item['pk_json']} "
            f"column={item['column_name']} target={item['target_type']} "
            f"actual_bytes={item['actual_bytes']} raw_bytes={item.get('raw_bytes')} "
            f"limit_bytes={item['limit_bytes']} action={'stop' if item['fatal'] else 'null'} "
            f"overflow_saved=1"
        )
    if len(overflows) > 5:
        log(f"FIELD OVERFLOW SUMMARY table={mapping_key(mapping)} delivery={delivery} "
            f"count={len(overflows)} details_logged=5 overflow_saved={len(overflows)}")

    fatal = [item for item in overflows if item["fatal"]]
    if fatal:
        first = fatal[0]
        raise ValueError(
            f"{mapping['src_table']}: target field overflow on required column "
            f"{first['column_name']} pk={first['pk_json']} actual_bytes={first['actual_bytes']} "
            f"limit_bytes={first['limit_bytes']}; overflow was saved but delivery is stopped"
        )


def payload_chunks(lines, cfg):
    chunks = lines.chunks if isinstance(lines,pa.ChunkedArray) else (lines,)
    for chunk in chunks:
        if not len(chunk):
            continue
        offsets = memoryview(chunk.buffers()[1]).cast(
            "q" if pa.types.is_large_string(chunk.type) else "i")
        data = chunk.buffers()[2]
        start, stop = chunk.offset, chunk.offset+len(chunk)
        while start < stop:
            end = bisect.bisect_right(
                offsets,offsets[start]+cfg["batch_bytes"],
                start+1,stop+1)-1
            end = max(start+1,end)
            length = offsets[end]-offsets[start]
            if length > cfg["max_row_bytes"]:
                raise ValueError(
                    f"encoded row exceeds CDC_MAX_ROW_BYTES: {length}")
            yield memoryview(data)[
                offsets[start]:offsets[start]+length],end-start
            start = end


def prepare_delivery(con, engine, mapping, delivery, cfg):
    row = con.execute("SELECT prepared FROM deliveries WHERE id=?", (delivery,)).fetchone()
    if not row:
        raise RuntimeError(f"missing delivery {delivery}")
    if row[0]:
        return True

    limit = int(cfg.get("max_prepared_bytes",2**63-1))
    # Unprepared parts were never submitted. A known exact wire size lets a
    # waiting delivery test capacity before repeating Arrow/DuckDB/JSON/gzip.
    with state_transaction(con):
        con.execute("DELETE FROM load_parts WHERE delivery_id=?",(delivery,))
        required,_ = prepare_requirement_get(con,delivery)
        logical = con.execute("""
            SELECT COALESCE(SUM(j.logical_bytes),0)
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=?
        """,(delivery,)).fetchone()[0]
        reserve = (
            required if required is not None
            else prepare_reservation_estimate(cfg,logical))
        if reserve > limit:
            raise RuntimeError(
                "single prepared delivery exceeds CDC_MAX_PREPARED_BYTES "
                f"delivery={delivery} required_bytes={reserve} limit={limit}")
        if not prepare_reservation_set_locked(con,delivery,reserve,cfg):
            con.execute(
                "DELETE FROM prepare_reservations WHERE delivery_id=?",(delivery,))
            return False
        prepare_requirement_attempt_locked(con,delivery)

    jobs = []
    job_cursor = con.execute("""
        SELECT j.id,j.payload,j.source_file,j.source_pos,j.source_time
        FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
        WHERE a.delivery_id=? ORDER BY j.id
    """,(delivery,))
    def delivery_payloads():
        for job_id,payload,source_file,source_pos,source_time in job_cursor:
            jobs.append((job_id,None,source_file,source_pos,source_time))
            yield payload
    raw = concat_job_tables(mapping,delivery_payloads())
    if not jobs:
        raise RuntimeError(f"delivery {delivery} has no active assigned jobs")
    total_wire = 0
    staged_complete = True
    known_required = required is not None
    reserved_capacity = int(reserve)
    overflows = []
    line_stream = transformed_line_batches(
        engine,mapping,raw,sequence=jobs[-1][0],collect_overflow=True,
        delivery_dense_order=True)
    with tempfile.SpooledTemporaryFile(
            max_size=1024**2,dir=state_temp_dir(cfg)) as staged:
        try:
            for lines,batch_overflows in line_stream:
                if batch_overflows:
                    overflows.extend(batch_overflows)
                for payload,nrows in payload_chunks(lines,cfg):
                    wire = (
                        gzip.compress(payload,compresslevel=1,mtime=0)
                        if cfg["compression"] == "gzip" else bytes(payload))
                    total_wire += len(wire)
                    if total_wire > limit:
                        with state_transaction(con):
                            prepare_requirement_set_locked(
                                con,delivery,total_wire)
                            con.execute(
                                "DELETE FROM prepare_reservations WHERE delivery_id=?",
                                (delivery,))
                        raise RuntimeError(
                            "single prepared delivery exceeds CDC_MAX_PREPARED_BYTES "
                            f"delivery={delivery} bytes={total_wire} limit={limit}")

                    if known_required:
                        # The exact prior probe owns enough capacity until
                        # deterministic bytes prove otherwise.
                        if total_wire > required:
                            staged_complete = False
                    elif staged_complete and total_wire > reserved_capacity:
                        # Grow the reservation only when staged bytes cross the
                        # durable claim; doubling avoids per-part SQLite writes.
                        wanted = min(
                            limit,max(
                                total_wire,max(1,reserved_capacity)*2))
                        with state_transaction(con):
                            if prepare_reservation_set_locked(
                                    con,delivery,wanted,cfg):
                                reserved_capacity = wanted
                            else:
                                con.execute(
                                    "DELETE FROM prepare_reservations "
                                    "WHERE delivery_id=?",(delivery,))
                                staged_complete = False
                        if not staged_complete:
                            staged.seek(0)
                            staged.truncate()

                    if staged_complete:
                        staged.write(struct.pack(
                            "<III",len(wire),int(nrows),len(payload)))
                        staged.write(wire)
        finally:
            line_stream.close()

        fatal_overflows = [
            item for item in overflows if item["fatal"]]
        if fatal_overflows and mapping.get("_target_constraints"):
            # Fatal data is independent of capacity and must remain fail-closed.
            # No load_parts have been committed yet; only local staging exists.
            prepare_reservation_release(con,delivery)
            persist_field_overflows(
                con,mapping,delivery,overflows,jobs)

        if not staged_complete:
            with state_transaction(con):
                prepare_requirement_set_locked(con,delivery,total_wire)
                con.execute(
                    "DELETE FROM prepare_reservations WHERE delivery_id=?",(delivery,))
            staged.seek(0)
            staged.truncate()
            return False

        # Nonfatal overflow evidence is persisted only once the delivery already
        # owns enough exact/over-reserved capacity to finish. Capacity probes have
        # no durable overflow side effects.
        if mapping.get("_target_constraints"):
            persist_field_overflows(con,mapping,delivery,overflows,jobs)

        # Finalize all prepared parts atomically. This converts one durable
        # reservation into load_parts without a fsync/BEGIN IMMEDIATE per part:
        # on crash SQLite exposes either the old unprepared delivery or the fully
        # prepared one, never a partially converted reservation.
        staged.seek(0)
        with state_transaction(con):
            prepare_requirement_set_locked(con,delivery,total_wire)
            held = con.execute(
                "SELECT reserved_bytes FROM prepare_reservations WHERE delivery_id=?",
                (delivery,)).fetchone()
            if not held or int(held[0]) < total_wire:
                raise RuntimeError(
                    "prepared byte reservation is smaller than staged payload "
                    f"delivery={delivery} held={held} bytes={total_wire}")
            con.execute(
                "UPDATE prepare_reservations SET reserved_bytes=? WHERE delivery_id=?",
                (total_wire,delivery))

            part = 0
            while True:
                header = staged.read(12)
                if not header:
                    break
                if len(header) != 12:
                    raise RuntimeError("truncated prepared payload staging header")
                wire_bytes,nrows,json_bytes = struct.unpack("<III",header)
                wire = staged.read(wire_bytes)
                if len(wire) != wire_bytes:
                    raise RuntimeError("truncated prepared payload staging body")
                label = "cdc_" + delivery + "_" + str(part)
                con.execute("""
                    INSERT INTO load_parts(
                        delivery_id,part,label,payload,nrows,json_bytes)
                    VALUES(?,?,?,?,?,?)
                """,(delivery,part,label,wire,nrows,json_bytes))
                con.execute("""
                    UPDATE prepare_reservations
                    SET reserved_bytes=reserved_bytes-?
                    WHERE delivery_id=? AND reserved_bytes>=?
                """,(wire_bytes,delivery,wire_bytes))
                if con.execute("SELECT changes()").fetchone()[0] != 1:
                    raise RuntimeError(
                        "prepared byte reservation accounting underflow")
                part += 1

            remaining = con.execute(
                "SELECT reserved_bytes FROM prepare_reservations WHERE delivery_id=?",
                (delivery,)).fetchone()
            if not remaining or int(remaining[0]) != 0:
                raise RuntimeError(
                    "prepared byte reservation did not drain to zero")
            con.execute(
                "DELETE FROM prepare_reservations WHERE delivery_id=?",(delivery,))
            con.execute(
                "UPDATE deliveries SET prepared=1 WHERE id=?",(delivery,))

    return True


def mysql_connect(cfg, target=False):
    options = dict(cfg["sr"] if target else cfg["mysql"])
    options.pop("http_port",None)
    con = pymysql.connect(**options,charset="utf8mb4",autocommit=True,
                          connect_timeout=10,read_timeout=cfg["query_timeout"],
                          write_timeout=cfg["query_timeout"])
    with con.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    return con


def binlog_checkpoint(con):
    with con.cursor() as cur:
        try:
            cur.execute("SHOW BINARY LOG STATUS")
        except pymysql.err.ProgrammingError as exc:
            if exc.args[0] != 1064:
                raise
            cur.execute("SHOW MASTER STATUS")
        row = cur.fetchone()
        if not row:
            raise RuntimeError("MySQL binary logging is disabled")
        gtid_set = str(row[4]).strip() if len(row) > 4 and row[4] is not None else None
        return (str(row[0]),int(row[1])),gtid_set


def binlog_position(con):
    return binlog_checkpoint(con)[0]


def config_fingerprint(cfg, prepared, state_format=STATE_FORMAT):
    source = {k:v for k,v in cfg["mysql"].items() if k != "password"}
    target = {k:v for k,v in cfg["sr"].items() if k != "password"}
    spec = dict(
        protocol=2,state_format=int(state_format),source=source,target=target,
        key_partitions=cfg["key_partitions"],compression=cfg["compression"],
        duckdb_version=duckdb.__version__,routing="duckdb_hash_canonical_v1",
        source_reader=cfg.get("source_reader","native_c_v1"),tables=[])
    if cfg.get("catalog_macros"):
        spec["catalog_macros"] = list(cfg["catalog_macros"])
    if cfg.get("catalog_udfs"):
        spec["catalog_udfs"] = [
            (item["name"],item.get("source_sha256",""),
             tuple(item.get("parameters",())),item.get("return_type",""))
            for item in cfg["catalog_udfs"]
        ]
    for mapping in prepared:
        spec["tables"].append({
            "source":mapping["src_table"],"target":mapping["sr_table"],
            "keys":pk_columns(mapping),"sql":mapping["_sql"],
            "filter":mapping["_filter_sql"],"schema":mapping["_schema_signature"],
            "target_sequence":mapping["_target_sequence"],
            "target_ddl":mapping["_target_ddl"],
        })
    return hashlib.sha256(orjson.dumps(spec,option=orjson.OPT_SORT_KEYS)).hexdigest()


def online_config_available():
    paths = cdc_catalog.catalog_paths(
        __file__,variables=catalog_variables())
    return cdc_catalog.connection_configured(paths["catalog"])


def mapping_output_description(mapping, cfg, macros=None, udfs=None):
    engine = transform_engine(cfg,macros=macros,udfs=udfs)
    try:
        empty = raw_arrow(mapping,[])
        engine.register("arrow_batch",empty)
        result = engine.execute(mapping["_sql"])
        description = [(str(d[0]),str(d[1]).upper()) for d in result.description]
    finally:
        engine.close()
    names = [name for name,_ in description]
    if len(names) != len(set(names)):
        raise ValueError(f"{mapping['src_table']}: mapping output column names must be unique")
    if any(name.startswith("_sync_") or name == "__op" for name in names):
        raise ValueError(f"{mapping['src_table']}: mapping output uses a reserved column name")
    missing_keys = [name for name in pk_columns(mapping) if name not in names]
    if missing_keys:
        raise ValueError(f"{mapping['src_table']}: mapping output is missing primary key columns {missing_keys!r}")
    return description


def starrocks_output_type(duck_type):
    value = str(duck_type).upper()
    exact = {
        "BOOLEAN":"BOOLEAN",
        "TINYINT":"TINYINT",
        "SMALLINT":"SMALLINT",
        "INTEGER":"INT",
        "INT":"INT",
        "BIGINT":"BIGINT",
        "HUGEINT":"LARGEINT",
        "UTINYINT":"SMALLINT",
        "USMALLINT":"INT",
        "UINTEGER":"BIGINT",
        "UBIGINT":"LARGEINT",
        "FLOAT":"FLOAT",
        "REAL":"FLOAT",
        "DOUBLE":"DOUBLE",
        "DATE":"DATE",
        "JSON":"JSON",
        # JSON Stream Load cannot directly load BINARY/VARBINARY in StarRocks 4.1.1.
        # Direct DuckDB BLOB outputs are encoded to Base64 before JSON serialization.
        "BLOB":"VARCHAR(1048576)",
        "VARCHAR":"VARCHAR(1048576)",
        "UUID":"VARCHAR(36)",
    }
    if value in exact:
        return exact[value]
    if value.startswith("DECIMAL("):
        match = re.match(r"DECIMAL\((\d+),(\d+)\)",value)
        if match and int(match.group(1)) <= 38:
            return f"DECIMAL({int(match.group(1))},{int(match.group(2))})"
    if value.startswith("TIMESTAMP"):
        return "DATETIME"
    if value.startswith("TIME"):
        return "VARCHAR(64)"
    raise ValueError(f"automatic StarRocks table creation does not support DuckDB output type {duck_type!r}; "
                     "create the target table explicitly")


def mysql_charset_width(collation):
    value = str(collation or "").lower()
    if value.startswith("utf8mb4"):
        return 4
    if value.startswith("utf8"):
        return 3
    if value.startswith(("ucs2","gbk","gb2312","big5")):
        return 2
    if value.startswith(("utf16","utf16le","utf32")):
        return 4
    return 1


def mysql_pk_target_type(column):
    name,data_type,column_type,_,collation,_ = column[:6]
    data_type = str(data_type).lower()
    column_type = str(column_type).lower()
    unsigned = "unsigned" in column_type
    if data_type == "tinyint":
        return "SMALLINT" if unsigned else "TINYINT"
    if data_type == "smallint":
        return "INT" if unsigned else "SMALLINT"
    if data_type == "mediumint":
        return "INT"
    if data_type == "int":
        return "BIGINT" if unsigned else "INT"
    if data_type == "bigint":
        return "LARGEINT" if unsigned else "BIGINT"
    if data_type in ("decimal","numeric"):
        match = re.search(r"\((\d+),\s*(\d+)\)",column_type)
        if not match or int(match.group(1)) > 38:
            raise ValueError(f"{name}: StarRocks Primary Key DECIMAL precision must be <= 38")
        return f"DECIMAL({int(match.group(1))},{int(match.group(2))})"
    if data_type in ("char","varchar"):
        match = re.search(r"\((\d+)\)",column_type)
        if not match:
            raise ValueError(f"{name}: cannot determine MySQL key string length from {column_type!r}")
        byte_limit = max(1,int(match.group(1))*mysql_charset_width(collation))
        if byte_limit > 1048576:
            raise ValueError(f"{name}: MySQL Primary Key can exceed StarRocks VARCHAR(1048576)")
        return f"VARCHAR({byte_limit})"
    if data_type == "date":
        return "DATE"
    if data_type in ("datetime","timestamp"):
        return "DATETIME"
    if data_type == "year":
        return "SMALLINT"
    raise ValueError(f"{name}: automatic target creation does not support MySQL Primary Key type "
                     f"{column_type!r}; create the StarRocks table explicitly")


def mysql_output_target_type(column, duck_type):
    _,data_type,column_type,_,collation,_ = column[:6]
    data_type = str(data_type).lower()
    column_type = str(column_type).lower()
    unsigned = "unsigned" in column_type
    integer_types = {
        "tinyint":("TINYINT","SMALLINT"),
        "smallint":("SMALLINT","INT"),
        "mediumint":("INT","INT"),
        "int":("INT","BIGINT"),
        "bigint":("BIGINT","LARGEINT"),
    }
    if data_type in integer_types:
        return integer_types[data_type][int(unsigned)],False
    if data_type == "float":
        return "FLOAT",False
    if data_type in ("double","real"):
        return "DOUBLE",False
    if data_type in ("decimal","numeric"):
        match = re.search(r"\((\d+),\s*(\d+)\)",column_type)
        if match:
            precision,scale = int(match.group(1)),int(match.group(2))
            if precision <= 38:
                return f"DECIMAL({precision},{scale})",False
            return f"VARCHAR({min(1048576,max(8,precision+3))})",False
        return "VARCHAR(128)",False
    if data_type == "date":
        return "DATE",False
    if data_type in ("datetime","timestamp"):
        return "DATETIME",False
    if data_type == "year":
        return "SMALLINT",False
    if data_type == "time":
        return "VARCHAR(32)",False
    if data_type in ("char","varchar"):
        match = re.search(r"\((\d+)\)",column_type)
        if match:
            maximum = max(1,int(match.group(1))*mysql_charset_width(collation))
            return f"VARCHAR({min(1048576,maximum)})",maximum > 1048576
    text_max = {
        "tinytext":255,
        "text":65535,
        "mediumtext":16777215,
        "longtext":4294967295,
    }.get(data_type)
    if text_max is not None:
        return f"VARCHAR({min(1048576,text_max)})",text_max > 1048576
    if data_type in ("enum","set"):
        return "VARCHAR(65535)",False
    if data_type == "json":
        return "JSON",True

    raw_binary_max = {
        "tinyblob":255,
        "blob":65535,
        "mediumblob":16777215,
        "longblob":4294967295,
    }.get(data_type)
    if data_type in ("binary","varbinary"):
        match = re.search(r"\((\d+)\)",column_type)
        raw_binary_max = int(match.group(1)) if match else None
    elif data_type == "bit":
        match = re.search(r"\((\d+)\)",column_type)
        raw_binary_max = (int(match.group(1))+7)//8 if match else 1
    elif data_type in ("geometry","point","linestring","polygon","multipoint",
                       "multilinestring","multipolygon","geometrycollection"):
        raw_binary_max = 4294967295
    if raw_binary_max is not None:
        encoded_max = 4*((int(raw_binary_max)+2)//3)
        return f"VARCHAR({min(1048576,max(1,encoded_max))})",encoded_max > 1048576

    fallback = starrocks_output_type(duck_type)
    return fallback,target_byte_limit(fallback) is not None and str(duck_type).upper() in ("VARCHAR","BLOB","JSON")


def automatic_target_layout(mapping, output_description, source_columns):
    source_by_name = {str(column[0]):column for column in source_columns}
    direct = dict(mapping.get("_direct_output_sources",{}))
    if not direct:
        direct = {name:name for name,_ in output_description if name in source_by_name}
    keys = set(pk_columns(mapping))
    target_types,size_columns = {},{}
    outputs = [name for name,_ in output_description]
    output_types = dict(output_description)
    for name in outputs:
        source_name = direct.get(name)
        source = source_by_name.get(source_name)
        if name in keys:
            if source is None:
                raise ValueError(f"{mapping['src_table']}: no source metadata for primary key {name!r}")
            sr_type,track_size = mysql_pk_target_type(source),False
        elif source is not None:
            sr_type,track_size = mysql_output_target_type(source,output_types[name])
        else:
            sr_type = starrocks_output_type(output_types[name])
            track_size = target_byte_limit(sr_type) is not None and \
                         str(output_types[name]).upper() in ("VARCHAR","BLOB","JSON")
        target_types[name] = sr_type
        if track_size:
            size_name = "size_"+name
            if size_name in outputs:
                raise ValueError(
                    f"{mapping['src_table']}: automatic overflow metadata column {size_name!r} "
                    "collides with a mapping output column")
            size_columns[name] = size_name
    return dict(types=target_types,size_columns=size_columns,sources=direct)


def starrocks_comment_text(value):
    return '"' + str(value).replace("\\","\\\\").replace('"','\\"').replace("\x00","") + '"'


def target_create_ddl(mapping, output_description, source_columns, column_comments=None, table_comment=""):
    layout = automatic_target_layout(mapping,output_description,source_columns)
    mapping["_auto_target_types"] = layout["types"]
    mapping["_auto_size_columns"] = layout["size_columns"]
    column_comments = column_comments or {}
    keys = pk_columns(mapping)
    ordered = keys+[name for name,_ in output_description if name not in keys]
    definitions = []
    for name in ordered:
        sr_type = layout["types"][name]
        if name in keys and sr_type.startswith(("VARBINARY","JSON","FLOAT","DOUBLE")):
            raise ValueError(f"{name}: unsupported automatic Primary Key type {sr_type}")
        null_sql = "NOT NULL" if name in keys else "NULL"
        source_name = layout["sources"].get(name)
        comment = str(column_comments.get(source_name,"") or "")
        comment_sql = " COMMENT "+starrocks_comment_text(comment) if comment else ""
        definitions.append(f"  {sql_name(name,True)} {sr_type} {null_sql}{comment_sql}")
        size_name = layout["size_columns"].get(name)
        if size_name:
            size_comment = f"Original byte size of {name}; value is NULL when it exceeds the StarRocks field limit"
            definitions.append(
                f"  {sql_name(size_name,True)} BIGINT NULL COMMENT {starrocks_comment_text(size_comment)}")
    bucket_keys = keys[:3]
    table_comment_sql = ("\nCOMMENT "+starrocks_comment_text(table_comment)) if table_comment else ""
    return (
        "CREATE TABLE IF NOT EXISTS "+sql_name(mapping["sr_table"],True)+" (\n"
        +",\n".join(definitions)+"\n) ENGINE=OLAP\nPRIMARY KEY("
        +",".join(sql_name(k,True) for k in keys)+")"
        +table_comment_sql+"\nDISTRIBUTED BY HASH("
        +",".join(sql_name(k,True) for k in bucket_keys)+")"
    )


def target_table_exists(cur, cfg, table):
    cur.execute("""
        SELECT 1 FROM information_schema.TABLES
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s LIMIT 1
    """,(cfg["sr"]["database"],table))
    return cur.fetchone() is not None


def durable_sink_state_exists(state_path, sink_key):
    if not os.path.exists(state_path):
        return False
    con = open_state(state_path)
    try:
        return con.execute(
            "SELECT 1 FROM table_state WHERE name=? LIMIT 1",
            (str(sink_key),)).fetchone() is not None
    finally:
        con.close()


def preflight(
        cfg, create_missing=True, mapping_defs=None, plan_macros=None,
        plan_udfs=None, allow_missing_targets=False, hot_add_sinks=()):
    if cfg["compression"] not in ("gzip",""):
        raise ValueError("CDC_COMPRESSION must be gzip or empty")
    if cfg["freshness_seconds"] <= 0:
        raise ValueError("CDC_FRESHNESS_SECONDS must be positive")
    mapping_defs = mappings if mapping_defs is None else mapping_defs
    prepared = [dict(m) for m in mapping_defs]
    hot_add_sinks = {str(item) for item in (hot_add_sinks or ())}
    state_ready = state_initialized(cfg["state"])
    if len({m["sr_table"] for m in prepared}) != len(prepared):
        raise ValueError("one target table must have exactly one source owner")
    with mysql_connect(cfg) as source, mysql_connect(cfg,target=True) as target:
        with source.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.server_uuid,@@GLOBAL.binlog_format,@@GLOBAL.binlog_row_image,"
                        "@@GLOBAL.gtid_mode")
            source_uuid,binlog_format,row_image,gtid_mode = cur.fetchone()
            if str(binlog_format).upper() != "ROW" or str(row_image).upper() != "FULL":
                raise RuntimeError("MySQL requires binlog_format=ROW and binlog_row_image=FULL")
            if str(gtid_mode).upper() not in ("ON","OFF"):
                raise RuntimeError("gtid_mode must be ON or OFF; transitional modes are unsupported")
            cfg["gtid_enabled"] = str(gtid_mode).upper() == "ON"
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'binlog_transaction_compression'")
            compression = cur.fetchone()
            if compression and str(compression[1]).upper() not in ("OFF","0"):
                raise RuntimeError("binlog_transaction_compression must be OFF")
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'binlog_row_value_options'")
            value_options = cur.fetchone()
            if value_options and value_options[1]:
                raise RuntimeError("partial JSON binlog values are unsupported; clear binlog_row_value_options")
            cur.execute("SHOW BINARY LOGS")
            available_logs = {str(row[0]) for row in cur.fetchall()}
        with target.cursor() as cur:
            cur.execute("SELECT current_version()")
            sr_version = str(cur.fetchone()[0])
            if not re.match(r"^4\.1\.1(?:\D|$)",sr_version):
                raise RuntimeError(f"this phase targets StarRocks 4.1.1; found {sr_version}")
            if create_missing or allow_missing_targets or hot_add_sinks:
                for candidate in prepared:
                    key = mapping_key(candidate)
                    if state_ready and key not in hot_add_sinks:
                        continue
                    if not target_table_exists(cur,cfg,candidate["sr_table"]):
                        continue
                    cur.execute("SELECT 1 FROM "+sql_name(candidate["sr_table"],True)+" LIMIT 1")
                    if cur.fetchone():
                        scope = "new hot-add sink" if key in hot_add_sinks else "new state"
                        raise RuntimeError(
                            f"{candidate['sr_table']}: {scope} requires a pre-existing "
                            "target table to be empty")
        for mapping in prepared:
            table = mapping["src_table"]
            with source.cursor() as cur:
                cur.execute("""SELECT ENGINE,TABLE_COMMENT FROM information_schema.TABLES
                               WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s""", (cfg["mysql"]["database"],table))
                row = cur.fetchone()
                if not row or str(row[0]).upper() != "INNODB":
                    raise ValueError(f"{table}: source must be an InnoDB table")
                table_comment = str(row[1] or "")
                cur.execute("""SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,IS_NULLABLE,COLLATION_NAME,EXTRA,COLUMN_COMMENT
                               FROM information_schema.COLUMNS
                               WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION""",
                            (cfg["mysql"]["database"],table))
                column_rows = cur.fetchall()
                columns = [tuple(c[:6]) for c in column_rows]
                column_comments = {str(c[0]):str(c[6] or "") for c in column_rows}
                if any(re.search(r"(?:VIRTUAL|STORED) GENERATED",str(c[5]).upper()) for c in columns):
                    raise ValueError(f"{table}: generated columns need an explicit CDC mapping")
                if any(str(c[0]).startswith("_sync_") or c[0] == "__op" for c in columns):
                    raise ValueError(f"{table}: reserved source column name")
                cur.execute("""SELECT COLUMN_NAME FROM information_schema.STATISTICS
                               WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME='PRIMARY'
                               ORDER BY SEQ_IN_INDEX""", (cfg["mysql"]["database"],table))
                source_pk = [r[0] for r in cur.fetchall()]
                if not source_pk:
                    raise ValueError(f"{table}: source must have a PRIMARY KEY")
                if mapping.get("primary_key") is None:
                    mapping["primary_key"] = source_pk[0] if len(source_pk) == 1 else list(source_pk)
                    log(f"PRIMARY KEY inferred {table}: {source_pk}")
                elif pk_columns(mapping) != source_pk:
                    raise ValueError(f"{table}: mapping primary_key must exactly match source PK {source_pk!r}")
            mapping["_schema"] = [(c[0],source_type(c[1],c[2])) for c in columns]
            mapping["_source_name_set"] = frozenset(name for name,_ in mapping["_schema"])
            mapping["_source_index"] = {name:index for index,(name,_) in enumerate(mapping["_schema"])}
            mapping["_json_columns"] = {c[0] for c in columns if str(c[1]).lower() == "json"}
            mapping["_schema_signature"] = columns
            mapping["_source_table_comment"] = table_comment
            mapping["_source_column_comments"] = column_comments
            validate_mapping(mapping)
            output_description = mapping_output_description(
                mapping,cfg,macros=plan_macros,udfs=plan_udfs)
            outputs = [name for name,_ in output_description]
            mapping["_binary_output_columns"] = {
                name for name,duck_type in output_description if str(duck_type).upper() == "BLOB"
            }
            layout = automatic_target_layout(mapping,output_description,columns)
            mapping["_auto_target_types"] = layout["types"]
            mapping["_auto_size_columns"] = layout["size_columns"]
            if mapping["_binary_output_columns"]:
                log(f"BINARY JSON ENCODING {table}: columns={sorted(mapping['_binary_output_columns'])} mode=base64")
            if "_cdc_seq" in outputs:
                raise ValueError("_cdc_seq is reserved for the compatibility sequence")
            with target.cursor() as cur:
                exists = target_table_exists(cur,cfg,mapping["sr_table"])
                target_columns = None
                mapping["_target_missing"] = not exists
                if not exists:
                    key = mapping_key(mapping)
                    hot_add = key in hot_add_sinks
                    if state_ready and not hot_add:
                        raise RuntimeError(
                            f"{mapping['sr_table']}: target table is missing but durable CDC state already exists; "
                            "refuse automatic recreation because historical target data may have been lost")
                    if hot_add and durable_sink_state_exists(cfg["state"],key):
                        raise RuntimeError(
                            f"{mapping['sr_table']}: hot-add sink {key} already has durable state; "
                            "refuse target recreation")
                    create_sql = target_create_ddl(
                        mapping,output_description,columns,column_comments,table_comment)
                    if create_missing:
                        try:
                            cur.execute(create_sql)
                        except Exception as exc:
                            raise RuntimeError(
                                f"{mapping['sr_table']}: automatic target creation failed: {exc}; ddl={create_sql}") from exc
                        log(f"CREATE TARGET {mapping['sr_table']}: primary_key={source_pk} "
                            f"columns={len(outputs)} size_columns={len(mapping.get('_auto_size_columns',{}))} "
                            f"distribution_keys={source_pk[:3]}")
                    elif allow_missing_targets or hot_add:
                        ddl = create_sql
                        keys = set(source_pk)
                        target_columns = {
                            name:(
                                name,layout["types"][name],
                                "NO" if name in keys else "YES",
                                "PRI" if name in keys else "",None,"")
                            for name in outputs
                        }
                        for size_name in layout["size_columns"].values():
                            target_columns[size_name] = (
                                size_name,"BIGINT","YES","",None,"")
                        log(
                            f"VALIDATE TARGET {mapping['sr_table']}: missing table "
                            +("will be created after catalog commit" if hot_add
                              else "will be created after catalog commit"))
                    else:
                        raise RuntimeError(
                            f"{mapping['sr_table']}: target table does not exist")
                if target_columns is None:
                    cur.execute("SHOW CREATE TABLE " + sql_name(mapping["sr_table"],True))
                    ddl = str(cur.fetchone()[1])
                    cur.execute("SHOW COLUMNS FROM " + sql_name(mapping["sr_table"],True))
                    target_columns = {r[0]:r for r in cur.fetchall()}
                key_match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)",ddl,re.I)
                if not key_match:
                    raise ValueError(f"{mapping['sr_table']}: target must be a Primary Key table")
                target_pk = [s.strip().strip(chr(96)) for s in key_match[1].split(",")]
                if target_pk != source_pk:
                    raise ValueError(f"{mapping['sr_table']}: target PK must match source PK")
                mapping["_target_schema"] = {
                    name: dict(
                        type=str(row[1]),
                        nullable=str(row[2]).upper() == "YES",
                        key=str(row[3] or ""),
                        default=row[4],
                        extra=str(row[5] or "") if len(row) > 5 else "",
                    )
                    for name,row in target_columns.items()
                }
                mapping["_target_sequence"] = "_cdc_seq" in target_columns
                mapping["_target_ddl"] = ddl
                missing = set(outputs)-set(target_columns)
                if missing:
                    raise ValueError(f"{table}: missing target columns: {sorted(missing)}")
                active_size_columns = {
                    name:size_name for name,size_name in mapping.get("_auto_size_columns",{}).items()
                    if size_name in target_columns
                }
                for name,size_name in active_size_columns.items():
                    size_type = str(target_columns[size_name][1]).lower()
                    if not size_type.startswith(("bigint","largeint")):
                        raise ValueError(
                            f"{mapping['sr_table']}: overflow size column {size_name} must be BIGINT/LARGEINT")
                if exists:
                    skipped = sorted(set(mapping.get("_auto_size_columns",{}).values())-set(target_columns))
                    if skipped:
                        log(f"SIZE COLUMNS EXISTING TARGET {mapping['sr_table']}: missing={skipped} "
                            "automatic ALTER disabled; overflow remains protected by NULL+journal")
                mapping["_size_columns"] = active_size_columns
                mapping["_target_json_columns"] = {
                    name for name in outputs
                    if str(target_columns[name][1]).lower().startswith("json")
                }
                binary_targets = [
                    name for name in outputs
                    if str(target_columns[name][1]).lower().startswith(("binary","varbinary"))
                ]
                if binary_targets:
                    raise ValueError(
                        f"{mapping['sr_table']}: mapped target columns {binary_targets!r} use BINARY/VARBINARY, "
                        "but this CDC protocol uses JSON Stream Load and StarRocks 4.1.1 supports BINARY "
                        "Stream Load only with CSV. Use a VARCHAR target and Base64 representation, or "
                        "change the transport explicitly."
                    )
                mapping["_output_columns"] = outputs
                mapping["_target_constraints"] = compile_target_constraints(mapping,target_columns)
            constrained = sorted(mapping["_target_constraints"])
            if constrained:
                log(f"FIELD LIMITS {table}: columns={constrained} policy=nullable->null+overflow required->stop")
            log(f"CHECK {table} -> {mapping['sr_table']}: primary key={source_pk}, "
                f"source_columns={len(columns)} output_columns={len(outputs)}")
        native_reader_plan(cfg,prepared)
        start,start_gtid = binlog_checkpoint(source)
        if cfg["gtid_enabled"] and start_gtid is None:
            raise RuntimeError("MySQL GTID is ON but SHOW BINARY LOG STATUS returned no GTID set")
        if not cfg["gtid_enabled"]:
            start_gtid = None
    return prepared,str(source_uuid),start,start_gtid,available_logs,config_fingerprint(cfg,prepared)



def runtime_plan_entry(version, prepared, macros=(), udfs=(), fingerprint=""):
    prepared = [dict(item) for item in prepared]
    by_table,by_source = {},{}
    for mapping in prepared:
        mapping["_plan_version"] = int(version)
        mapping["_relational_ir"] = relational_ir.mapping_ir(
            mapping, macros=macros, udfs=udfs)
        mapping["_relational_ir_id"] = relational_ir.semantic_id(
            mapping["_relational_ir"])
        mapping["_incremental_ir"] = incremental_ir.compile_ir(
            mapping["_relational_ir"])
        mapping["_incremental_ir_id"] = incremental_ir.semantic_id(
            mapping["_incremental_ir"])
        key = mapping_key(mapping)
        if key in by_table:
            raise RuntimeError(f"duplicate durable sink identity: {key}")
        by_table[key] = mapping
        by_source.setdefault(mapping["src_table"],[]).append(mapping)
    return dict(
        version=int(version),prepared=prepared,
        by_table=by_table,by_source=by_source,
        source_prepared=source_mappings(prepared),
        macros=list(macros or ()),udfs=list(udfs or ()),
        fingerprint=str(fingerprint or ""))


def prepare_runtime_catalog_plan(cfg, plan, hot_add_sinks=()):
    local = dict(cfg)
    local["catalog_macros"] = list(plan.get("macros",()))
    local["catalog_udfs"] = list(plan.get("udfs",()))
    prepared,_,_,_,_,fingerprint = preflight(
        local,create_missing=False,mapping_defs=plan["mappings"],
        plan_macros=local["catalog_macros"],plan_udfs=local["catalog_udfs"],
        hot_add_sinks=hot_add_sinks)
    return runtime_plan_entry(
        int(plan["version"]),prepared,local["catalog_macros"],
        local["catalog_udfs"],fingerprint)


def runtime_plan_topology(current, candidate):
    current_tables = set(current["by_table"])
    candidate_tables = set(candidate["by_table"])
    return dict(
        added=sorted(candidate_tables-current_tables),
        dropped=sorted(current_tables-candidate_tables),
        retained=sorted(current_tables&candidate_tables),
    )


def runtime_plan_compatible(current, candidate):
    change = runtime_plan_topology(current,candidate)
    for table in change["retained"]:
        old = current["by_table"][table]
        new = candidate["by_table"][table]
        if old["src_table"] != new["src_table"]:
            return False,f"{table}: source table changed"
        if old["sr_table"] != new["sr_table"]:
            return False,f"{table}: target table changed"
        if pk_columns(old) != pk_columns(new):
            return False,f"{table}: primary key changed"
        if old.get("_schema_signature") != new.get("_schema_signature"):
            return False,f"{table}: source schema changed"
        if old.get("_relational_ir_id") != new.get("_relational_ir_id"):
            return False,(
                f"{table}: retained sink relational semantics changed; "
                "rebuild/new generation is required")
        if old.get("_incremental_ir_id") != new.get("_incremental_ir_id"):
            return False,(
                f"{table}: retained sink incremental semantics changed; "
                "rebuild/new generation is required")
    if change["added"]:
        return True,(
            "compatible hot-add sinks require snapshot+live CDC bootstrap: "
            +",".join(change["added"]))
    if change["dropped"]:
        return True,(
            "compatible drop-only sink plan; target tables are preserved: "
            +",".join(change["dropped"]))
    return True,"compatible transform/macro/UDF plan"


def hot_add_worker_resource_check(cfg, runtime, added_keys):
    added_keys={str(item) for item in (added_keys or ())}
    if not added_keys:
        return
    existing_keys=set(runtime.get("worker_keys",()))
    physical_sinks=max(
        1,len(existing_keys|added_keys))
    if cfg["load_mode"]=="merge_async":
        per_sink_writers=max(
            1,min(
                int(cfg["writer_max"]),
                int(cfg["resource"]["cpu_target"])//physical_sinks))
        loader_slots=per_sink_writers*physical_sinks
    else:
        per_sink_writers=1
        loader_slots=physical_sinks
    slots=max(
        1,1+max(0,int(cfg.get("snapshot_workers",0)))
        +loader_slots*2)
    memory_bytes=int(cfg["resource"]["memory_mb"])*1024**2
    per_engine_cap=max(1,(memory_bytes//2)//slots)
    current_limit=memory_limit_bytes(cfg["duckdb_memory"])
    if current_limit>per_engine_cap:
        raise RuntimeError(
            "online sink add would exceed the active-engine DuckDB memory "
            f"budget: physical_sinks={physical_sinks} "
            f"writers_per_sink={per_sink_writers} engine_slots={slots} "
            f"current_per_engine_mb={current_limit//1024**2} "
            f"required_cap_mb={per_engine_cap//1024**2}; restart with the "
            "published topology so resource sizing can be recomputed safely")


def hot_add_resource_check(cfg, runtime, current, candidate):
    change=runtime_plan_topology(current,candidate)
    hot_add_worker_resource_check(
        cfg,runtime,change["added"])


def runtime_active_version(runtime):
    with runtime["plan_lock"]:
        return int(runtime["active_plan_version"])


def runtime_plan(runtime, version):
    version = int(version)
    with runtime["plan_lock"]:
        plan = runtime["plans"].get(version)
    if plan is None:
        loader = runtime.get("plan_loader")
        if loader is None:
            raise RuntimeError(f"runtime catalog plan version {version} is unavailable")
        loaded = loader(version)
        with runtime["plan_lock"]:
            plan = runtime["plans"].setdefault(version,loaded)
    return plan


def runtime_mapping(runtime, version, table):
    version = int(version)
    table = str(table)
    stateful = runtime.get("stateful_mappings",{}).get((version,table))
    if stateful is not None:
        return stateful
    plan = runtime_plan(runtime,version)
    mapping = plan["by_table"].get(table)
    if mapping is None:
        raise RuntimeError(
            f"catalog plan {version} has no mapping for durable table {table}")
    return mapping


def active_snapshot_status(con, runtime):
    tables = sorted(runtime_plan(
        runtime,runtime_active_version(runtime))["by_table"])
    if not tables:
        return 0,0
    marks = ",".join("?" for _ in tables)
    done = con.execute(
        f"SELECT COUNT(*) FROM table_state "
        f"WHERE snapshot_done=1 AND name IN ({marks})",
        tables).fetchone()[0]
    return int(done),len(tables)


def delivery_plan_version(con, delivery):
    row = con.execute(
        "SELECT plan_version FROM deliveries WHERE id=?",(delivery,)).fetchone()
    if not row:
        raise RuntimeError(f"missing delivery {delivery}")
    return int(row[0])


def transform_engine_for_version(runtime, cfg, version, table=None):
    version=int(version)
    stateful=(
        None if table is None else
        runtime.get("stateful_mappings",{}).get((version,str(table)))
    )
    if stateful is not None:
        return transform_engine(cfg)
    plan=runtime_plan(runtime,version)
    return transform_engine(
        cfg,macros=plan.get("macros",()),udfs=plan.get("udfs",()))


def plan_engine(engine_cache, runtime, cfg, version, table=None):
    version = int(version)
    stateful = (
        None if table is None else
        runtime.get("stateful_mappings",{}).get((version,str(table)))
    )
    key = ("stateful",version,str(table)) if stateful is not None else version
    engine = engine_cache.get(key)
    if engine is None:
        engine=transform_engine_for_version(
            runtime,cfg,version,table)
        engine_cache[key] = engine
    return engine


def close_plan_engines(engine_cache):
    for engine in engine_cache.values():
        with contextlib.suppress(Exception):
            engine.close()
    engine_cache.clear()


def durable_draining_plan_versions(con, active_version):
    active_version = int(active_version)
    return sorted({
        int(row[0]) for row in con.execute("""
            SELECT DISTINCT j.plan_version
            FROM active_jobs j
            LEFT JOIN aggregate_job_links a ON a.job_id=j.id
            LEFT JOIN join_job_links q ON q.job_id=j.id
            WHERE a.job_id IS NULL AND q.job_id IS NULL
            UNION
            SELECT DISTINCT d.plan_version
            FROM deliveries d
            WHERE EXISTS(
                SELECT 1
                FROM job_assignments x
                JOIN jobs j ON j.id=x.job_id
                LEFT JOIN aggregate_job_links a ON a.job_id=j.id
                LEFT JOIN join_job_links q ON q.job_id=j.id
                WHERE x.delivery_id=d.id
                  AND a.job_id IS NULL AND q.job_id IS NULL
            )
        """).fetchall()
        if row[0] is not None and int(row[0]) != active_version
    })


def live_plan_versions(con, runtime):
    versions = {
        int(row[0]) for row in con.execute("""
            SELECT DISTINCT plan_version FROM active_jobs
            UNION
            SELECT DISTINCT plan_version FROM deliveries
        """).fetchall()
        if row[0] is not None
    }
    versions.add(runtime_active_version(runtime))
    with runtime["plan_lock"]:
        for name in ("pending_plan","deferred_plan"):
            plan = runtime.get(name)
            if plan is not None:
                versions.add(int(plan["version"]))
    return versions


def prune_plan_engines(engine_cache, con, runtime):
    live = live_plan_versions(con,runtime)
    for key in list(engine_cache):
        version = (
            int(key[1])
            if isinstance(key,tuple) and key and key[0]=="stateful"
            else int(key)
        )
        if version not in live:
            engine = engine_cache.pop(key)
            with contextlib.suppress(Exception):
                engine.close()
    if len(runtime.get("plans",{})) > max(4,len(live)+1):
        with runtime["plan_lock"]:
            for version in list(runtime["plans"]):
                if int(version) not in live:
                    runtime["plans"].pop(version,None)


def _catalog_plan_payload(cfg, publish_result):
    if publish_result.get("mappings") is not None:
        return dict(
            version=int(publish_result["version"]),
            revision=int(publish_result.get("revision",0)),
            plan_hash=str(publish_result.get("plan_hash","")),
            mappings=list(publish_result.get("mappings",())),
            stateful_tasks=list(publish_result.get("stateful_tasks",())),
            macros=list(publish_result.get("macros",())),
            udfs=list(publish_result.get("udfs",())))
    return cdc_catalog.load_plan_version(
        cfg["catalog"],int(publish_result["version"]))


def validate_stateful_catalog_plan(
        cfg,plan,prepared,create_missing=False,allow_missing=False
):
    tasks=list(plan.get("stateful_tasks",()) or ())
    if not tasks:
        return []
    if not cfg.get("shared_source_state",False):
        raise RuntimeError(
            "stateful catalog tasks require CDC_SHARED_SOURCE_STATE=1")
    scope=stateful_catalog_runtime.source_scope(
        cfg,tasks,prepared)
    compiled=stateful_catalog_runtime.compile_catalog_tasks(
        cfg,int(plan.get("version",0)),tasks,
        scope["source_metadata"],
        create_missing=create_missing,
        allow_missing=allow_missing)
    if os.path.exists(cfg["state"]):
        probe=open_state(cfg["state"])
        try:
            stateful_catalog_runtime.ensure_registration_safe(
                probe,cfg,compiled)
        finally:
            probe.close()
    return compiled

def durable_plan_hot_add_sinks(cfg, plan):
    state_path = cfg["state"]
    if not os.path.exists(state_path):
        return []
    con = open_state(state_path)
    try:
        active_version = meta_get(con,"active_plan_version")
    finally:
        con.close()
    if active_version is None:
        return []
    active_version = int(active_version)
    if active_version == int(plan.get("version",0)):
        return []
    current = cdc_catalog.load_plan_version(
        cfg["catalog"],active_version)
    current_keys = {
        mapping_key(item) for item in current.get("mappings",())}
    candidate_keys = {
        mapping_key(item) for item in plan.get("mappings",())}
    return sorted(candidate_keys-current_keys)


def validate_local_catalog_publish(publish_result, phase):
    if phase not in ("validate","validate_config","install","install_config"):
        raise ValueError(f"unknown catalog publish phase: {phase}")

    if phase in ("install","install_config"):
        if phase == "install_config" and not online_config_available():
            return dict(
                status="config_incomplete",
                version=int(publish_result.get("version",0)),
                note="persistent connection settings are incomplete; target creation skipped")
        local_cfg = read_config()
        plan = _catalog_plan_payload(local_cfg,publish_result)
        local_cfg["catalog_macros"] = list(plan.get("macros",()))
        local_cfg["catalog_udfs"] = list(plan.get("udfs",()))
        hot_add_sinks = durable_plan_hot_add_sinks(local_cfg,plan)
        prepared,_,_,_,_,_ = preflight(
            local_cfg,create_missing=True,
            mapping_defs=plan.get("mappings",()),
            plan_macros=local_cfg["catalog_macros"],
            plan_udfs=local_cfg["catalog_udfs"],
            hot_add_sinks=hot_add_sinks)
        stateful=validate_stateful_catalog_plan(
            local_cfg,plan,prepared,
            create_missing=True)
        created = sorted(
            mapping["sr_table"] for mapping in prepared
            if mapping.get("_target_missing"))
        if stateful:
            return dict(
                status="restart_required",
                version=int(publish_result.get("version",0)),
                target_creation="stateless_created_stateful_verified",
                created_targets=created,
                stateful_tasks=len(stateful),
                reason=(
                    "stateful task plan was committed and validated; restart "
                    "the daemon to expand shared source capture and activate "
                    "durable stateful workers"))
        return dict(
            status="installed_offline",
            version=int(publish_result.get("version",0)),
            target_creation="created_or_verified",
            created_targets=created)

    mappings = publish_result.get("mappings")
    variables = publish_result.get("_variables")
    if variables is None:
        raise RuntimeError(
            "local publish validation is missing the transactional configuration snapshot")
    if (
        phase == "validate"
        and mappings == []
        and not publish_result.get("stateful_tasks")
    ):
        return dict(
            status="validated_offline",version=int(publish_result["version"]),
            note="empty plan requires no online source/target validation")
    if (
        phase == "validate_config"
        and cdc_catalog.connection_settings_values(
            variables,require=False) is None
    ):
        return dict(
            status="config_incomplete",
            version=int(publish_result.get("version",0)),
            note="persistent connection settings are incomplete; online validation skipped")
    with catalog_variable_scope(variables):
        local_cfg = read_config()
        plan = _catalog_plan_payload(local_cfg,publish_result)
        local_cfg["catalog_macros"] = list(plan.get("macros",()))
        local_cfg["catalog_udfs"] = list(plan.get("udfs",()))
        hot_add_sinks = durable_plan_hot_add_sinks(local_cfg,plan)
        prepared,_,_,_,_,_ = preflight(
            local_cfg,create_missing=False,
            mapping_defs=plan.get("mappings",()),
            plan_macros=local_cfg["catalog_macros"],
            plan_udfs=local_cfg["catalog_udfs"],
            allow_missing_targets=True,
            hot_add_sinks=hot_add_sinks)
        stateful=validate_stateful_catalog_plan(
            local_cfg,plan,prepared,
            allow_missing=True)
    return dict(
        status=(
            "validated_config"
            if phase == "validate_config"
            else "validated_offline"),
        version=int(publish_result.get("version",0)),
        stateful_tasks=len(stateful),
        target_creation="create_after_catalog_commit")

def validate_hot_catalog_plan(cfg, runtime, publish_result):
    version = int(publish_result["version"])
    if version == runtime_active_version(runtime):
        return dict(status="active",version=version)

    plan=_catalog_plan_payload(cfg,publish_result)
    current_catalog=cdc_catalog.load_plan_version(
        cfg["catalog"],runtime_active_version(runtime))
    current_stateful={
        str(item.get("sink")):dict(item)
        for item in current_catalog.get("stateful_tasks",())
    }
    candidate_stateful={
        str(item.get("sink")):dict(item)
        for item in plan.get("stateful_tasks",())
    }
    stateful_added=sorted(
        set(candidate_stateful)-set(current_stateful))
    stateful_dropped=sorted(
        set(current_stateful)-set(candidate_stateful))
    stateful_changed=sorted(
        sink for sink in set(current_stateful)&set(candidate_stateful)
        if current_stateful[sink]!=candidate_stateful[sink])
    if stateful_changed and (
        len(stateful_changed)!=1
        or stateful_added
        or stateful_dropped
    ):
        return dict(
            status="rebuild_required",
            version=version,
            reason=(
                "online stateful semantic rebuild currently requires exactly "
                "one retained sink replacement per catalog publish and cannot "
                "be combined with add/drop: changed=%s added=%s dropped=%s"
                % (
                    stateful_changed,
                    stateful_added,
                    stateful_dropped,
                )),
            stateful_changed_sinks=stateful_changed)

    if stateful_dropped:
        live_by_sink=stateful_catalog_runtime.compiled_by_sink(
            runtime.get("stateful_tasks",()))
        probe=open_state(cfg["state"])
        try:
            for sink in stateful_dropped:
                item=live_by_sink.get(sink)
                if item is None:
                    return dict(
                        status="restart_required",version=version,
                        reason=(
                            "stateful drop cannot bind the live durable task: "
                            +sink),
                        stateful_dropped_sinks=stateful_dropped)
                durable=stateful_durable_task(
                    probe,item["kind"],item["task"]["task_id"])
                generation=task_generation.maybe_info(
                    probe,durable["sink_key"],
                    durable["plan_version"])
                if (
                    durable["status"]!="active"
                    or generation is None
                    or generation["status"]!="ready"
                    or not generation["source_pin_released"]
                ):
                    return dict(
                        status="restart_required",version=version,
                        reason=(
                            "stateful drop is online-safe only after the "
                            "generation is active/ready; retry after readiness "
                            "or restart: "+sink),
                        stateful_dropped_sinks=stateful_dropped)
                try:
                    source_state.consumer_info(
                        probe,durable["consumer_id"])
                except KeyError:
                    return dict(
                        status="restart_required",version=version,
                        reason=(
                            "stateful drop has no durable source consumer: "
                            +sink),
                        stateful_dropped_sinks=stateful_dropped)
        finally:
            probe.close()

    candidate_config_revision = int(
        publish_result.get(
            "config_revision",
            cdc_catalog.config_revision(cfg["catalog"])))
    if candidate_config_revision != int(cfg.get("catalog_config_revision",0)):
        # A mixed SET + plan deployment still has to prove the candidate is
        # valid against its uncommitted configuration before published_version
        # can move. It cannot hot-apply because runtime resources/connections
        # were fixed at daemon startup, so successful validation still requires
        # a restart.
        validate_local_catalog_publish(publish_result,"validate")
        return dict(
            status="restart_required",version=version,
            reason=(
                "persistent runtime configuration changed since daemon startup; "
                "candidate plan validated against the new configuration and will "
                "be used after restart"))

    current = runtime_plan(runtime,runtime_active_version(runtime))
    current_keys = set(current["by_table"])
    candidate_keys = {mapping_key(item) for item in plan.get("mappings",())}
    hot_add_sinks = sorted(candidate_keys-current_keys)
    candidate = prepare_runtime_catalog_plan(
        cfg,plan,hot_add_sinks=hot_add_sinks)
    stateful_additions=[]
    if stateful_changed:
        compatible,reason=runtime_plan_compatible(
            current,candidate)
        if not compatible:
            return dict(
                status="rebuild_required",
                version=version,
                reason=reason,
                stateful_changed_sinks=stateful_changed)
        sink=stateful_changed[0]
        live_by_sink=stateful_catalog_runtime.compiled_by_sink(
            runtime.get("stateful_tasks",()))
        old_item=live_by_sink.get(sink)
        if old_item is None:
            return dict(
                status="restart_required",
                version=version,
                reason=(
                    "stateful semantic rebuild cannot bind live owner: "
                    +sink),
                stateful_changed_sinks=stateful_changed)
        manifest=candidate_stateful[sink]
        scope=stateful_catalog_runtime.source_scope(
            cfg,[manifest],candidate["prepared"])
        probe=open_state(cfg["state"])
        try:
            old_task=stateful_durable_task(
                probe,old_item["kind"],
                old_item["task"]["task_id"])
            generation=task_generation.maybe_info(
                probe,old_task["sink_key"],
                old_task["plan_version"])
            if (
                old_task["status"]!="active"
                or generation is None
                or generation["status"]!="ready"
                or not generation["source_pin_released"]
            ):
                return dict(
                    status="restart_required",
                    version=version,
                    reason=(
                        "stateful semantic rebuild requires the current "
                        "generation to be active/ready: "+sink),
                    stateful_changed_sinks=stateful_changed)
            for mapping in scope["capture_mappings"]:
                source=str(mapping["src_table"])
                if source not in set(scope["required_sources"]):
                    continue
                relation=source_relation_key(
                    cfg,mapping)
                try:
                    info=source_state.relation_info(
                        probe,relation)
                except KeyError:
                    return dict(
                        status="restart_required",
                        version=version,
                        reason=(
                            "stateful semantic rebuild references a relation "
                            "outside the live shared capture scope: "
                            +relation),
                        stateful_changed_sinks=stateful_changed)
                if list(info["pk_columns"])!=pk_columns(mapping):
                    return dict(
                        status="rebuild_required",
                        version=version,
                        reason=(
                            "stateful semantic rebuild source primary key "
                            "differs from durable shared state: "+relation),
                        stateful_changed_sinks=stateful_changed)
                if not info["schema"].equals(
                    source_arrow_schema(mapping),
                    check_metadata=False
                ):
                    return dict(
                        status="rebuild_required",
                        version=version,
                        reason=(
                            "stateful semantic rebuild source schema differs "
                            "from durable shared state: "+relation),
                        stateful_changed_sinks=stateful_changed)
            base=stateful_task_plan.compile_ir(
                manifest,cfg["mysql"]["database"],
                scope["source_metadata"])
            if str(base["kind"])!=str(old_item["kind"]):
                return dict(
                    status="rebuild_required",
                    version=version,
                    reason=(
                        "online semantic rebuild does not yet change "
                        "stateful operator kind: "+sink),
                    stateful_changed_sinks=stateful_changed)
            inferred=stateful_catalog_runtime.infer_target_schema(
                base["kind"],base["ir"])
            provisional=stateful_task_plan.compile_task(
                manifest,version,cfg["mysql"]["database"],
                scope["source_metadata"],inferred)
            shadow=stateful_rebuild.shadow_target(
                manifest["target_table"],
                provisional["task"]["task_id"])
            new_item=stateful_catalog_runtime.compile_rebuild_task(
                cfg,version,manifest,
                scope["source_metadata"],shadow)
            if list(new_item["task"]["target_schema"])!=list(
                old_task["target_schema"]
            ):
                return dict(
                    status="rebuild_required",
                    version=version,
                    reason=(
                        "online stateful semantic rebuild currently requires "
                        "an unchanged target schema; publish a new sink or "
                        "restart for schema migration: "+sink),
                    stateful_changed_sinks=stateful_changed)
            if mapping_key(new_item["mapping"]) in set(
                candidate["by_table"]
            ):
                return dict(
                    status="rebuild_required",
                    version=version,
                    reason=(
                        "stateful/stateless sink identity collision during "
                        "semantic rebuild: "+sink),
                    stateful_changed_sinks=stateful_changed)
            existing=stateful_rebuild.maybe_info(
                probe,sink)
            if existing is not None and existing["phase"] not in {
                "complete","failed"
            } and (
                existing["old_task_id"]!=old_task["task_id"]
                or existing["new_task_id"]
                !=new_item["task"]["task_id"]
            ):
                return dict(
                    status="restart_required",
                    version=version,
                    reason=(
                        "another durable semantic rebuild is already active "
                        "for sink "+sink),
                    stateful_changed_sinks=stateful_changed)
        finally:
            probe.close()
        current_compiled=stateful_catalog_runtime.compiled_by_sink(
            runtime.get("stateful_tasks",()))
        candidate_compiled=[]
        for candidate_sink in candidate_stateful:
            if candidate_sink==sink:
                candidate_compiled.append(
                    new_item)
            else:
                retained=current_compiled.get(
                    candidate_sink)
                if retained is None:
                    return dict(
                        status="restart_required",
                        version=version,
                        reason=(
                            "live runtime lacks retained stateful task "
                            +candidate_sink))
                candidate_compiled.append(
                    retained)
        candidate["stateful_candidate_tasks"]=candidate_compiled
        candidate["stateful_rebuilds"]=[dict(
            sink=sink,
            old=old_item,
            new=new_item,
            shadow_target=shadow,
            logical_target=str(
                manifest["target_table"]),
        )]
        validation=dict(
            status="rebuild_pending",
            version=version,
            reason=(
                "semantic replacement will build an isolated shadow "
                "generation and atomically swap after a common visible "
                "source frontier"),
            history_mode="stateful_shadow_rebuild",
            stateful_changed_sinks=[sink],
            shadow_target=shadow)
        with runtime["plan_lock"]:
            runtime.setdefault(
                "validated_catalog_plans",{})[
                    str(
                        publish_result.get("plan_hash")
                        or plan.get("plan_hash"))
                ]=(candidate,dict(validation))
        return validation

    if stateful_added:
        manifests=[
            candidate_stateful[sink]
            for sink in stateful_added
        ]
        scope=stateful_catalog_runtime.source_scope(
            cfg,manifests,candidate["prepared"])
        probe=open_state(cfg["state"])
        try:
            for mapping in scope["capture_mappings"]:
                source=str(mapping["src_table"])
                if source not in set(scope["required_sources"]):
                    continue
                relation=source_relation_key(cfg,mapping)
                try:
                    info=source_state.relation_info(
                        probe,relation)
                except KeyError:
                    return dict(
                        status="restart_required",version=version,
                        reason=(
                            "online stateful add references a relation outside "
                            "the live shared capture scope; restart is required "
                            "to expand the native decoder/source mirror: "
                            +relation),
                        stateful_added_sinks=stateful_added)
                if list(info["pk_columns"])!=pk_columns(mapping):
                    return dict(
                        status="rebuild_required",version=version,
                        reason=(
                            "online stateful add source primary key differs "
                            "from durable shared source state: "+relation),
                        stateful_added_sinks=stateful_added)
                if not info["schema"].equals(
                    source_arrow_schema(mapping),
                    check_metadata=False):
                    return dict(
                        status="rebuild_required",version=version,
                        reason=(
                            "online stateful add source schema differs from "
                            "durable shared source state: "+relation),
                        stateful_added_sinks=stateful_added)
            stateful_additions=(
                stateful_catalog_runtime.compile_catalog_tasks(
                    cfg,version,manifests,
                    scope["source_metadata"],
                    allow_missing=True))
            ensure_stateful_hot_add_targets_empty(
                cfg,stateful_additions,allow_missing=True)
            stateful_catalog_runtime.ensure_registration_safe(
                probe,cfg,stateful_additions)
        finally:
            probe.close()
        stateless_keys=set(candidate["by_table"])
        collision=sorted(
            mapping_key(item["mapping"])
            for item in stateful_additions
            if mapping_key(item["mapping"]) in stateless_keys
        )
        if collision:
            return dict(
                status="rebuild_required",version=version,
                reason=(
                    "stateful/stateless sink identity collision: "
                    +",".join(collision)),
                stateful_added_sinks=stateful_added)
        candidate["stateful_additions"]=stateful_additions
        candidate["stateful_added_manifests"]=manifests
        candidate["stateful_source_metadata"]=scope["source_metadata"]
        try:
            hot_add_worker_resource_check(
                cfg,runtime,
                [mapping_key(item["mapping"])
                 for item in stateful_additions])
        except RuntimeError as exc:
            return dict(
                status="restart_required",version=version,
                reason=str(exc),
                stateful_added_sinks=stateful_added)

    if stateful_added or stateful_dropped:
        current_compiled=stateful_catalog_runtime.compiled_by_sink(
            runtime.get("stateful_tasks",()))
        added_compiled=stateful_catalog_runtime.compiled_by_sink(
            stateful_additions)
        candidate_compiled=[]
        for sink in candidate_stateful:
            if sink in added_compiled:
                candidate_compiled.append(
                    added_compiled[sink])
                continue
            item=current_compiled.get(sink)
            if item is None:
                return dict(
                    status="restart_required",version=version,
                    reason=(
                        "live runtime lacks the compiled stateful task needed "
                        "for online cutover: "+sink))
            candidate_compiled.append(item)
        candidate["stateful_candidate_tasks"]=candidate_compiled
        candidate["stateful_dropped_sinks"]=list(
            stateful_dropped)
    compatible,reason = runtime_plan_compatible(current,candidate)
    if not compatible:
        return dict(status="rebuild_required",version=version,reason=reason)
    change = runtime_plan_topology(current,candidate)
    if cfg.get("shared_source_state",False) and change["added"]:
        probe = open_state(cfg["state"])
        try:
            missing_sources = sorted({
                source_relation_key(cfg,candidate["by_table"][key])
                for key in change["added"]
                if not source_state_relation_exists(
                    probe,source_relation_key(cfg,candidate["by_table"][key]))
            })
        finally:
            probe.close()
        if missing_sources:
            return dict(
                status="rebuild_required",version=version,
                reason=(
                    "shared source scope does not contain newly referenced "
                    "relations; restart/rebuild is required to expand capture: "
                    + ",".join(missing_sources)),
                added_sinks=list(change["added"]))
    try:
        hot_add_resource_check(cfg,runtime,current,candidate)
    except RuntimeError as exc:
        return dict(
            status="restart_required",version=version,
            reason=str(exc),added_sinks=hot_add_sinks)
    con = open_state(cfg["state"])
    try:
        if change["added"]:
            marks = ",".join("?" for _ in change["added"])
            prior_state = [
                row[0] for row in con.execute(
                    f"SELECT name FROM table_state "
                    f"WHERE name IN ({marks})",
                    list(change["added"])).fetchall()
            ]
            if prior_state:
                return dict(
                    status="rebuild_required",version=version,
                    reason=(
                        "re-adding a sink with durable prior table_state is "
                        "not yet an online-safe operation; explicit generation "
                        "reset/rebuild is required: " + ",".join(sorted(prior_state))),
                    added_sinks=list(change["added"]))
        current_tables = sorted(current["by_table"])
        if current_tables:
            marks = ",".join("?" for _ in current_tables)
            incomplete = [
                row[0] for row in con.execute(
                    f"SELECT name FROM table_state "
                    f"WHERE snapshot_done=0 AND name IN ({marks})",
                    current_tables).fetchall()]
        else:
            incomplete = []
        draining = durable_draining_plan_versions(
            con,runtime_active_version(runtime))
    finally:
        con.close()

    status = (
        "deferred_until_snapshot_done" if incomplete
        else "deferred_until_previous_plan_drained" if draining
        else "hot_pending")
    validation = dict(
        status=status,version=version,reason=reason,
        history_mode=(
            "snapshot_plus_live_cdc"
            if change["added"] else
            "stateful_fixed_w"
            if stateful_added else
            "stateful_drop_drain"
            if stateful_dropped else "forward"))
    if stateful_added:
        validation["stateful_added_sinks"]=list(
            stateful_added)
    if stateful_dropped:
        validation["stateful_dropped_sinks"]=list(
            stateful_dropped)
    if change["added"]:
        validation["added_sinks"] = list(change["added"])
    if change["dropped"]:
        validation["dropped_sinks"] = list(change["dropped"])
    if incomplete:
        validation["tables"] = incomplete
    if draining:
        validation["draining_versions"] = draining
    if status == "hot_pending":
        validation["cutover"] = "next safe MySQL transaction boundary"
        validation["note"] = (
            "new sinks start an independent historical snapshot while live CDC "
            "is captured from the cutover boundary; retained sinks remain forward-hot"
            if change["added"] else
            "existing StarRocks rows are not rewritten; new source transactions "
            "use the new plan after cutover")
    with runtime["plan_lock"]:
        runtime.setdefault("validated_catalog_plans",{})[
            str(publish_result.get("plan_hash") or plan.get("plan_hash"))] = (
                candidate,dict(validation))
    return validation


def ensure_hot_add_targets(cfg, candidate, added_sinks):
    added_sinks = [str(item) for item in (added_sinks or ())]
    if not added_sinks:
        return
    with mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            for key in added_sinks:
                mapping = candidate["by_table"].get(key)
                if mapping is None:
                    raise RuntimeError(
                        f"validated hot-add sink disappeared before install: {key}")
                exists = target_table_exists(cur,cfg,mapping["sr_table"])
                if exists:
                    cur.execute(
                        "SELECT 1 FROM "+sql_name(mapping["sr_table"],True)+" LIMIT 1")
                    if cur.fetchone():
                        raise RuntimeError(
                            f"{mapping['sr_table']}: hot-add target became non-empty "
                            "before activation; refuse to merge unknown history")
                    continue
                ddl = str(mapping.get("_target_ddl") or "")
                if not ddl:
                    raise RuntimeError(
                        f"{mapping['sr_table']}: validated hot-add target has no CREATE DDL")
                try:
                    cur.execute(ddl)
                except Exception as exc:
                    raise RuntimeError(
                        f"{mapping['sr_table']}: hot-add target creation failed: "
                        f"{exc}; ddl={ddl}") from exc
                if not target_table_exists(cur,cfg,mapping["sr_table"]):
                    raise RuntimeError(
                        f"{mapping['sr_table']}: CREATE returned without a visible target table")
                log(
                    f"HOT ADD CREATE TARGET sink={key} table={mapping['sr_table']} "
                    f"primary_key={pk_columns(mapping)}")


def ensure_stateful_hot_add_targets_empty(cfg, additions, allow_missing=False):
    additions=list(additions or ())
    if not additions:
        return
    with mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            for item in additions:
                task=item["task"]
                table=str(task["target_table"])
                if not target_table_exists(cur,cfg,table):
                    if allow_missing:
                        continue
                    raise RuntimeError(
                        "stateful hot-add target disappeared before activation: "
                        +table)
                cur.execute(
                    "SELECT 1 FROM "+sql_name(table,True)+" LIMIT 1")
                if cur.fetchone():
                    raise RuntimeError(
                        "stateful hot-add target is non-empty; refuse to merge "
                        "unknown history: "+table)


def prepare_hot_stateful_additions(cfg, runtime, candidate):
    additions=list(candidate.get("stateful_additions",()) or ())
    if not additions:
        return []
    manifests=list(candidate.get("stateful_added_manifests",()) or ())
    metadata=dict(candidate.get("stateful_source_metadata",{}) or {})
    ensure_stateful_hot_add_targets_empty(
        cfg,additions,allow_missing=True)
    compiled=stateful_catalog_runtime.compile_catalog_tasks(
        cfg,int(candidate["version"]),manifests,metadata,
        create_missing=True,allow_missing=False)
    expected={
        item["task"]["sink_key"]:item["task"]["descriptor_hash"]
        for item in additions
    }
    actual={
        item["task"]["sink_key"]:item["task"]["descriptor_hash"]
        for item in compiled
    }
    if actual!=expected:
        raise RuntimeError(
            "stateful hot-add target binding changed after CREATE/verify "
            "expected=%r actual=%r" % (expected,actual))
    ensure_stateful_hot_add_targets_empty(
        cfg,compiled,allow_missing=False)
    con=open_state(cfg["state"])
    try:
        stateful_catalog_runtime.ensure_registration_safe(
            con,cfg,compiled)
        registered=stateful_catalog_runtime.register_compiled(
            con,compiled)
        with runtime["plan_lock"]:
            existing_tasks=list(
                runtime.get("stateful_tasks",()))
        preferences=stateful_share_policy.plan_graph(
            con,existing_tasks+list(registered),cfg=cfg)
    finally:
        con.close()
    candidate["stateful_additions"]=registered
    candidate["stateful_share_preferences"]=preferences
    if "stateful_candidate_tasks" in candidate:
        replacements=stateful_catalog_runtime.compiled_by_sink(
            registered)
        candidate["stateful_candidate_tasks"]=[
            replacements.get(
                item["task"]["sink_key"],item)
            for item in candidate["stateful_candidate_tasks"]
        ]
    return registered


def activate_stateful_additions(cfg, runtime, candidate):
    additions=list(candidate.get("stateful_additions",()) or ())
    if not additions:
        return []
    ensure_stateful_hot_add_targets_empty(
        cfg,additions,allow_missing=False)
    activated=[]
    for item in additions:
        task=item["task"]
        mapping=item["mapping"]
        key=mapping_key(mapping)
        version=stateful_task_plan.writer_plan_version(
            task["plan_version"])
        identity=(version,key)
        with runtime["plan_lock"]:
            previous=runtime.setdefault(
                "stateful_mappings",{}).get(identity)
            if previous is not None and (
                previous.get("_output_columns")!=mapping.get("_output_columns")
                or previous.get("sr_table")!=mapping.get("sr_table")
            ):
                raise RuntimeError(
                    "stateful hot-add writer identity changed: %r"
                    % (identity,))
            runtime["stateful_mappings"][identity]=mapping
            runtime.setdefault(
                "stateful_active_task_ids",set()).add(
                    task["task_id"])
            existing={
                entry["task"]["task_id"]
                for entry in runtime.setdefault(
                    "stateful_tasks",[])
            }
            if task["task_id"] not in existing:
                runtime["stateful_tasks"].append(item)

        runtime_add_sink(
            mapping,cfg,runtime,historical_snapshot=False)
        thread=threading.Thread(
            target=guarded_worker,
            args=(stateful_task_worker,runtime,item,cfg),
            name="stateful-"+str(item["kind"])+"-"+key)
        runtime_thread_register(runtime,thread)
        with runtime["plan_lock"]:
            runtime.setdefault(
                "stateful_worker_threads",{})[
                    task["task_id"]]=thread
        activated.append(task["task_id"])
        log(
            "STATEFUL HOT ADD ACTIVE task=%s sink=%s writer_version=%d"
            % (task["task_id"],key,version))
    return activated


def stateful_dropped_items(runtime,candidate):
    candidate_tasks=candidate.get("stateful_candidate_tasks")
    if candidate_tasks is None:
        return []
    candidate_ids={
        item["task"]["task_id"]
        for item in candidate_tasks
    }
    with runtime["plan_lock"]:
        prior_tasks=list(runtime.get("stateful_tasks",()))
    return [
        item for item in prior_tasks
        if item["task"]["task_id"] not in candidate_ids
    ]


def activate_stateful_transition(con, cfg, runtime, candidate):
    candidate_tasks=candidate.get("stateful_candidate_tasks")
    if candidate_tasks is None:
        return dict(
            added=activate_stateful_additions(
                cfg,runtime,candidate),
            retired=[])

    candidate_tasks=list(candidate_tasks)
    candidate_ids={
        item["task"]["task_id"]
        for item in candidate_tasks
    }
    dropped=stateful_dropped_items(
        runtime,candidate)
    retiring_ids={
        item["task"]["task_id"]
        for item in dropped
    }
    durable_intents={}
    for item in dropped:
        task_id=item["task"]["task_id"]
        durable_intents[task_id]=(
            stateful_catalog_runtime.retirement_info(
                con,task_id))

    with runtime["plan_lock"]:
        runtime["stateful_tasks"]=list(candidate_tasks)
        runtime["stateful_active_task_ids"]=set(
            candidate_ids|retiring_ids)
        retire_frontiers=runtime.setdefault(
            "stateful_retire_frontiers",{})
        retire_items=runtime.setdefault(
            "stateful_retire_items",{})
        for item in dropped:
            task_id=item["task"]["task_id"]
            retire_frontiers[task_id]=int(
                durable_intents[task_id]["frontier"])
            retire_items[task_id]=item

    for item in dropped:
        task=item["task"]
        frontier=durable_intents[
            task["task_id"]]["frontier"]
        log(
            "STATEFUL HOT DROP FENCE task=%s sink=%s source_seq=%d "
            "policy=catch_up_visible_then_retire durable=1"
            % (
                task["task_id"],task["sink_key"],int(frontier),
            ))

    added=activate_stateful_additions(
        cfg,runtime,candidate)
    return dict(
        added=added,
        retired=sorted(retiring_ids),
    )


def catalog_activation_record(runtime, validation):
    record = {name:validation[name] for name in
              (
                  'status','version','reason','added_sinks','dropped_sinks',
                  'stateful_added_sinks','stateful_dropped_sinks',
                  'stateful_changed_sinks','history_mode'
              ) if name in validation}
    with runtime['plan_lock']:
        runtime['catalog_activation'] = record
    if record.get('status') in ('restart_required','rebuild_required'):
        log(f"PLAN NOT ACTIVATED version={record.get('version',0)} "
            f"active={runtime_active_version(runtime)} status={record['status']} "
            f"reason={record.get('reason','')}")
    return validation


def stateful_rebuild_remote_marker(cfg,table):
    table=str(table)
    with mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            cur.execute("""
                SELECT TABLE_COMMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            """,(cfg["sr"]["database"],table))
            row=cur.fetchone()
            if row is None:
                return None
            return str(row[0] or "")


def ensure_stateful_rebuild_shadow(
        cfg,spec,existing_intent=False
):
    item=spec["new"]
    task=item["task"]
    logical=str(spec["logical_target"])
    shadow=str(spec["shadow_target"])
    marker=stateful_rebuild.remote_marker(
        task["task_id"])
    with mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            if not target_table_exists(
                cur,cfg,logical
            ):
                raise RuntimeError(
                    "stateful rebuild logical target disappeared: "
                    +logical)
            exists=target_table_exists(
                cur,cfg,shadow)
            if exists and not existing_intent:
                raise RuntimeError(
                    "stateful rebuild shadow target already exists without "
                    "durable ownership: "+shadow)
            if not exists:
                cur.execute(
                    "CREATE TABLE "
                    +sql_name(shadow,True)
                    +" LIKE "+sql_name(logical,True))
                if not target_table_exists(
                    cur,cfg,shadow
                ):
                    raise RuntimeError(
                        "stateful rebuild shadow CREATE LIKE returned without "
                        "a visible table: "+shadow)
                cur.execute(
                    "ALTER TABLE "
                    +sql_name(shadow,True)
                    +" COMMENT = %s",
                    (marker,))
            else:
                cur.execute("""
                    SELECT TABLE_COMMENT
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                """,(cfg["sr"]["database"],shadow))
                row=cur.fetchone()
                actual_marker=(
                    "" if row is None
                    else str(row[0] or ""))
                if actual_marker!=marker:
                    raise RuntimeError(
                        "durable stateful rebuild shadow marker differs: "
                        +shadow)
    actual=stateful_catalog_runtime.resolve_target_schema(
        cfg,item["kind"],task["ir"],shadow)
    if list(actual)!=list(task["target_schema"]):
        raise RuntimeError(
            "stateful rebuild shadow target schema differs from compiled "
            "generation: "+shadow)
    return shadow


def activate_stateful_rebuild_candidate(
        cfg,runtime,candidate
):
    specs=list(
        candidate.get("stateful_rebuilds",()) or ())
    if not specs:
        return []
    if len(specs)!=1:
        raise RuntimeError(
            "online stateful rebuild supports one sink per plan")
    spec=specs[0]
    old=spec["old"]
    new=spec["new"]
    task=new["task"]
    sink=str(spec["sink"])
    if mapping_key(new["mapping"])!=sink:
        raise RuntimeError(
            "stateful rebuild writer sink identity changed")
    con=open_state(cfg["state"])
    try:
        existing=stateful_rebuild.maybe_info(
            con,sink)
        if existing is None:
            original_comment=stateful_rebuild_remote_marker(
                cfg,spec["logical_target"])
            if original_comment is None:
                raise RuntimeError(
                    "stateful rebuild logical target disappeared before "
                    "durable intent: "+str(spec["logical_target"]))
            intent=stateful_rebuild.begin(
                con,new["kind"],sink,
                old["task"]["task_id"],
                task["task_id"],
                spec["logical_target"],
                shadow=spec["shadow_target"],
                original_comment=original_comment)
            existing_intent=False
        else:
            intent=stateful_rebuild.begin(
                con,new["kind"],sink,
                old["task"]["task_id"],
                task["task_id"],
                spec["logical_target"],
                shadow=spec["shadow_target"],
                original_comment=existing["original_comment"])
            existing_intent=True
        if intent["phase"] not in {
            "building_shadow","fencing",
            "ready_to_swap"
        }:
            raise RuntimeError(
                "cannot activate rebuild candidate from phase "
                +intent["phase"])
        ensure_stateful_rebuild_shadow(
            cfg,spec,
            existing_intent=existing_intent)
        registered=stateful_catalog_runtime.register_compiled(
            con,[new])[0]
        if registered["task"]["descriptor_hash"]!=task[
            "descriptor_hash"
        ]:
            raise RuntimeError(
                "stateful rebuild durable descriptor changed")
        spec["new"]=registered
        candidate["stateful_rebuilds"]=[spec]
        replacements=stateful_catalog_runtime.compiled_by_sink(
            candidate.get("stateful_candidate_tasks",()))
        replacements[sink]=registered
        candidate["stateful_candidate_tasks"]=[
            replacements[name]
            for name in sorted(replacements)
        ]
    finally:
        con.close()

    mapping=spec["new"]["mapping"]
    task=spec["new"]["task"]
    version=stateful_task_plan.writer_plan_version(
        task["plan_version"])
    identity=(version,sink)
    with runtime["plan_lock"]:
        runtime.setdefault(
            "stateful_mappings",{})[identity]=mapping
        runtime.setdefault(
            "stateful_active_task_ids",set()).add(
            task["task_id"])
        existing_tasks={
            entry["task"]["task_id"]
            for entry in runtime.setdefault(
                "stateful_tasks",[])
        }
        if task["task_id"] not in existing_tasks:
            runtime["stateful_tasks"].append(
                spec["new"])
        runtime.setdefault(
            "stateful_rebuild_plans",{})[sink]=candidate
        runtime.setdefault(
            "stateful_rebuild_locks",{}).setdefault(
                sink,threading.Lock())

    if sink not in runtime.get(
        "worker_keys",set()
    ):
        runtime_add_sink(
            mapping,cfg,runtime,
            historical_snapshot=False)
    thread=threading.Thread(
        target=guarded_worker,
        args=(
            stateful_task_worker,runtime,
            spec["new"],cfg),
        name="stateful-rebuild-"
        +str(spec["new"]["kind"])+"-"+sink)
    runtime_thread_register(
        runtime,thread)
    with runtime["plan_lock"]:
        runtime.setdefault(
            "stateful_worker_threads",{})[
                task["task_id"]]=thread
    log(
        "STATEFUL REBUILD BUILDING sink=%s old=%s new=%s shadow=%s "
        "writer_version=%d"
        % (
            sink,old["task"]["task_id"],
            task["task_id"],
            spec["shadow_target"],version))
    return [task["task_id"]]


def install_stateful_rebuild_plan(
        cfg,runtime,publish_result,validation,candidate
):
    version=int(candidate["version"])
    with runtime["plan_lock"]:
        runtime["plans"][version]=candidate
    activated=activate_stateful_rebuild_candidate(
        cfg,runtime,candidate)
    result=dict(validation)
    result["status"]="rebuild_pending"
    result["activated_tasks"]=activated
    return catalog_activation_record(
        runtime,result)


def install_hot_catalog_plan(cfg, runtime, publish_result, validation):
    status = str(validation.get("status",""))
    if status in ("active","restart_required","rebuild_required"):
        return catalog_activation_record(runtime,validation)

    plan_hash = str(publish_result.get("plan_hash",""))
    with runtime["plan_lock"]:
        cached = runtime.setdefault("validated_catalog_plans",{}).pop(
            plan_hash,None)
    if cached is None:
        # Startup requeue and conservative recovery may not have a validation
        # cache; validate again rather than trusting an unbound catalog plan.
        validation = validate_hot_catalog_plan(cfg,runtime,publish_result)
        status = str(validation.get("status",""))
        if status in ("active","restart_required","rebuild_required"):
            return catalog_activation_record(runtime,validation)
        with runtime["plan_lock"]:
            cached = runtime.setdefault("validated_catalog_plans",{}).pop(
                plan_hash,None)
    if cached is None:
        raise RuntimeError("validated catalog plan disappeared before install")
    candidate,_ = cached
    version = int(candidate["version"])
    if status=="rebuild_pending":
        return install_stateful_rebuild_plan(
            cfg,runtime,publish_result,
            validation,candidate)
    stateful_payload={
        name:candidate[name]
        for name in (
            "stateful_additions",
            "stateful_added_manifests",
            "stateful_source_metadata",
        )
        if name in candidate
    }
    added_sinks = list(validation.get("added_sinks") or ())
    if added_sinks:
        ensure_hot_add_targets(cfg,candidate,added_sinks)
        # Missing targets were validated against deterministic synthetic DDL.
        # Rebind after CREATE so the durable cutover fingerprint contains the
        # server's real SHOW CREATE/schema representation used on restart.
        rebound_plan = _catalog_plan_payload(cfg,publish_result)
        candidate = prepare_runtime_catalog_plan(
            cfg,rebound_plan,hot_add_sinks=added_sinks)
        candidate.update(stateful_payload)
        hot_add_resource_check(
            cfg,runtime,
            runtime_plan(runtime,runtime_active_version(runtime)),
            candidate)
        candidate["hot_add_sinks"] = added_sinks
    if candidate.get("stateful_additions"):
        prepare_hot_stateful_additions(
            cfg,runtime,candidate)

    with runtime["plan_lock"]:
        runtime["plans"][version] = candidate
        if status.startswith("deferred_"):
            runtime["deferred_plan"] = candidate
        else:
            runtime["pending_plan"] = candidate
    if status == "hot_pending":
        log(
            f"PLAN PENDING version={version} active={runtime_active_version(runtime)} "
            f"history_mode={validation.get('history_mode','forward')} "
            f"added_sinks={added_sinks} reason={validation.get('reason','')}")
    return catalog_activation_record(runtime,validation)


def catalog_publish_callback(cfg, runtime, publish_result, phase):
    if phase == "validate":
        return validate_hot_catalog_plan(cfg,runtime,publish_result)
    if phase == "validate_config":
        validation = validate_local_catalog_publish(
            publish_result,"validate_config")
        validation["restart_required"] = True
        return validation
    if phase == "install_config":
        return catalog_activation_record(runtime,dict(
            status="restart_required",
            version=int(publish_result.get("version",0)),
            reason=(
                "persistent runtime configuration changed; restart the daemon "
                "to rebuild live connections/resources from the committed catalog")))
    if phase == "install":
        validation = publish_result.get("validation")
        if validation is None:
            validation = validate_hot_catalog_plan(cfg,runtime,publish_result)
        try:
            return install_hot_catalog_plan(
                cfg,runtime,publish_result,validation)
        except Exception as exc:
            catalog_activation_record(runtime,dict(
                status="restart_required",version=int(publish_result["version"]),
                reason="published plan installation failed: "+str(exc)))
            raise
    raise ValueError(f"unknown catalog publish phase: {phase}")


def queue_hot_catalog_plan(cfg, runtime, publish_result):
    validation = validate_hot_catalog_plan(cfg,runtime,publish_result)
    return install_hot_catalog_plan(
        cfg,runtime,publish_result,validation)


def runtime_thread_register(runtime, thread):
    with runtime["thread_lock"]:
        thread.start()
        runtime["worker_threads"].append(thread)
    return thread


def runtime_sink_retiring(runtime, table):
    with runtime["control_lock"]:
        return str(table) in runtime.get("retiring_sinks",set())


def sink_durable_drained(con, table):
    table = str(table)
    if con.execute(
            "SELECT 1 FROM active_jobs WHERE table_name=? LIMIT 1",
            (table,)).fetchone():
        return False
    if con.execute(
            "SELECT 1 FROM deliveries WHERE table_name=? LIMIT 1",
            (table,)).fetchone():
        return False
    if con.execute(
            "SELECT 1 FROM merge_uncertain WHERE table_name=? LIMIT 1",
            (table,)).fetchone():
        return False
    return True


def runtime_mark_sink_retiring(runtime, table, cfg):
    table = str(table)
    workers = (
        int(cfg["writer_max"])
        if cfg["load_mode"] == "merge_async" else 1)
    with runtime["control_lock"]:
        retiring = runtime.setdefault("retiring_sinks",set())
        counts = runtime.setdefault("retiring_workers",{})
        if table in retiring:
            return False
        if table not in runtime.get("worker_keys",set()):
            return False
        retiring.add(table)
        counts[table] = workers
        event = runtime.get("load_events",{}).get(
            table if cfg["load_mode"] == "merge_async" else (table,0))
    if event is not None:
        event.set()
    log(
        f"SINK RETIRING table={table} workers={workers} "
        "policy=drain_durable_jobs_then_exit")
    return True


def runtime_retiring_worker_done(runtime, table, cfg):
    table = str(table)
    finalized = False
    remaining = 0
    cap = None
    with runtime["control_lock"]:
        retiring = runtime.setdefault("retiring_sinks",set())
        counts = runtime.setdefault("retiring_workers",{})
        if table not in retiring:
            return False
        remaining = int(counts.get(table,1))-1
        if remaining > 0:
            counts[table] = remaining
            return False
        counts.pop(table,None)
        retiring.discard(table)
        runtime.get("worker_keys",set()).discard(table)
        mappings = runtime.get("worker_mappings",[])
        mappings[:] = [
            mapping for mapping in mappings
            if mapping_key(mapping) != table]
        for name in (
                "pressure_until","table_interval","active_writers",
                "last_pressure","last_scale","max_rowset",
                "version_recovery","version_recovery_good",
                "snapshot_transform_bytes_cap"):
            runtime.get(name,{}).pop(table,None)
        runtime.get("quarantined_tables",{}).pop(table,None)
        events = runtime.get("load_events",{})
        events.pop(
            table if cfg["load_mode"] == "merge_async" else (table,0),
            None)
        for lane in range(int(cfg["key_partitions"])):
            runtime.get("lane_locks",{}).pop((table,lane),None)
        sink_count = max(1,len(runtime.get("worker_keys",())))
        memory_cap = duckdb_merge_writer_cap(cfg,sink_count)
        cap = max(
            1,min(
                int(cfg["writer_max"]),
                int(memory_cap),
                int(cfg["resource"]["cpu_target"])//sink_count))
        runtime["resource_writer_cap"] = cap
        finalized = True
    if finalized:
        log(
            f"SINK DRAINED table={table} workers_remaining=0 "
            f"active_sinks={len(runtime.get('worker_keys',()))} "
            f"resource_writer_cap={cap}")
    return finalized


def runtime_add_sink(mapping, cfg, runtime, historical_snapshot=True):
    key = mapping_key(mapping)
    now = time.time()
    with runtime["control_lock"]:
        if key in runtime.get("retiring_sinks",set()):
            raise RuntimeError(
                f"{key}: cannot re-add sink while prior workers are draining")
        if key in runtime["worker_keys"]:
            return False
        runtime["worker_keys"].add(key)
        runtime["worker_mappings"].append(mapping)
        sink_count = max(1,len(runtime["worker_mappings"]))
        memory_cap = duckdb_merge_writer_cap(cfg,sink_count)
        cap = max(
            1,min(
                cfg["writer_max"],
                memory_cap,
                int(cfg["resource"]["cpu_target"])//sink_count))
        runtime["resource_writer_cap"] = min(
            int(runtime.get("resource_writer_cap",cap)),cap)
        runtime["pressure_until"][key] = 0
        runtime["table_interval"][key] = cfg["commit_interval_ms"]/1000
        runtime.setdefault("snapshot_transform_bytes_cap",{})[key] = int(
            cfg["batch_bytes"])
        runtime["active_writers"][key] = min(cfg["writer_initial"],cap)
        runtime["last_pressure"][key] = now
        runtime["last_scale"][key] = now
        runtime["max_rowset"][key] = -1
        runtime["version_recovery"][key] = False
        runtime["version_recovery_good"][key] = 0
        if cfg["load_mode"] == "merge_async":
            runtime["load_events"][key] = threading.Event()
        else:
            runtime["load_events"][(key,0)] = threading.Event()
        for lane in range(cfg["key_partitions"]):
            runtime["lane_locks"][(key,lane)] = threading.Lock()

    metrics = runtime.get("metrics")
    if metrics is not None:
        with metrics["lock"]:
            metrics["tables"].setdefault(
                key,dict(total=metric_bucket(),interval=metric_bucket()))

    for existing in list(runtime["worker_keys"]):
        if writer_target(runtime,existing) > cap:
            set_writer_target(
                runtime,existing,cfg,cap,
                "online sink add reduced per-sink CPU budget",
                pressure=True,minimum=1)

    if cfg["load_mode"] == "merge_async":
        for worker_id in range(cfg["writer_max"]):
            runtime_thread_register(
                runtime,
                threading.Thread(
                    target=guarded_worker,
                    args=(merge_delivery_worker,runtime,mapping,worker_id,cfg),
                    name=f"merge-{key}-{worker_id}"))
    else:
        runtime_thread_register(
            runtime,
            threading.Thread(
                target=guarded_worker,
                args=(table_delivery_worker,runtime,mapping,cfg),
                name=f"load-{key}"))

    if historical_snapshot:
        executor = runtime.get("snapshot_executor")
        if executor is None:
            raise RuntimeError(
                "snapshot executor is unavailable during online sink add")
        if cfg.get("shared_source_state",False):
            probe = open_state(cfg["state"])
            try:
                source_state.relation_info(
                    probe,source_relation_key(cfg,mapping))
            except KeyError as exc:
                raise RuntimeError(
                    "hot-add source relation is outside the mirrored source "
                    "scope; restart/rebuild is required to expand source scope"
                ) from exc
            finally:
                probe.close()
            executor.submit(
                guarded_worker,shared_snapshot_worker,
                runtime,mapping,cfg)
            snapshot_mode = "shared_fixed_w"
        else:
            executor.submit(
                guarded_worker,snapshot_worker,
                runtime,mapping,cfg)
            snapshot_mode = "mysql"
    else:
        snapshot_mode = "stateful_generation"
    log(
        f"HOT ADD WORKERS sink={key} source={mapping.get('src_table','stateful')} "
        f"target={mapping['sr_table']} writer_cap={cap} snapshot={snapshot_mode}")
    return True


def activate_pending_plan(con, decoder, cfg, runtime, position):
    with runtime["plan_lock"]:
        candidate = runtime.get("pending_plan")
        if candidate is None:
            return None
        version = int(candidate["version"])
        current = runtime["plans"][int(runtime["active_plan_version"])]
    change = runtime_plan_topology(current,candidate)
    added = list(change["added"])
    dropped = list(change["dropped"])
    stateful_dropped=stateful_dropped_items(
        runtime,candidate)
    stateful_retire_frontier=(
        source_state.base_applied_seq(con)
        if stateful_dropped else None)
    if added:
        # Recheck immediately before durable cutover. A target that was filled
        # by an external process after catalog validation must never be merged
        # with J4's historical bootstrap.
        ensure_hot_add_targets(cfg,candidate,added)
    with state_transaction(con):
        for item in stateful_dropped:
            stateful_catalog_runtime.stage_retirement(
                con,item["kind"],item["task"],
                stateful_retire_frontier)
        for key in added:
            if con.execute(
                    "SELECT 1 FROM table_state WHERE name=?",(key,)).fetchone():
                raise RuntimeError(
                    f"hot-add sink {key} already has durable table_state before cutover")
            con.execute("INSERT INTO table_state(name) VALUES(?)",(key,))
        for key in dropped:
            prior = current["by_table"][key]
            generation = task_generation.maybe_info(
                con,key,int(prior.get("_plan_version",0)))
            if generation is not None:
                task_generation.set_terminal(
                    con,key,int(prior.get("_plan_version",0)),"retired")
        meta_set(con,"active_plan_version",version)
        meta_set(con,"fingerprint",candidate["fingerprint"])
        meta_set(con,"plan_cutover_position",position)
        meta_set(con,"plan_cutover_time",time.time())
        meta_set(
            con,"plan_history_mode",
            "snapshot_plus_live_cdc" if added else
            "stateful_fixed_w" if candidate.get("stateful_additions")
            else "forward")
    with runtime["plan_lock"]:
        runtime["active_plan_version"] = version
        runtime["pending_plan"] = None
    for key in dropped:
        runtime_mark_sink_retiring(runtime,key,cfg)
    native_reset(
        decoder,cfg,
        runtime_capture_sources(runtime,candidate)[0])
    for key in added:
        runtime_add_sink(candidate["by_table"][key],cfg,runtime)
    stateful_transition=activate_stateful_transition(
        con,cfg,runtime,candidate)
    catalog_activation_record(
        runtime,dict(
            status="active",version=version,
            stateful_added_sinks=[
                item["task"]["sink_key"]
                for item in candidate.get("stateful_additions",())
            ],
            stateful_dropped_sinks=list(
                candidate.get("stateful_dropped_sinks",()))))
    wake_loaders(runtime)
    log(
        f"PLAN ACTIVE version={version} cutover={position[0]}:{position[1]} "
        f"added_sinks={added} dropped_sinks={dropped} "
        f"stateful_added={stateful_transition['added']} "
        f"stateful_retired={stateful_transition['retired']} "
        "old durable jobs continue on stamped plan_version")
    return candidate

def check_initial_targets(cfg, prepared):
    with mysql_connect(cfg,target=True) as con:
        with con.cursor() as cur:
            for mapping in prepared:
                cur.execute("SELECT 1 FROM "+sql_name(mapping["sr_table"],True)+" LIMIT 1")
                if cur.fetchone():
                    raise RuntimeError(
                        f"{mapping['sr_table']}: a new state file requires an EMPTY target table. "
                        "Use a new empty Primary Key table for migration; never discard the old state blindly.")


def curl_request(handle, cfg, url, payload=None, headers=None, stop=None, method=None):
    response = io.BytesIO()
    handle.reset()
    handle.setopt(pycurl.URL,url)
    handle.setopt(pycurl.USERPWD,cfg["sr"]["user"]+":"+cfg["sr"]["password"])
    handle.setopt(pycurl.HTTPAUTH,pycurl.HTTPAUTH_BASIC)
    handle.setopt(pycurl.HTTP_VERSION,pycurl.CURL_HTTP_VERSION_1_1)
    handle.setopt(pycurl.FOLLOWLOCATION,1)
    handle.setopt(pycurl.MAXREDIRS,3)
    # FE redirects to trusted BE nodes, matching the previous curl --location-trusted.
    handle.setopt(pycurl.UNRESTRICTED_AUTH,1)
    handle.setopt(pycurl.CONNECTTIMEOUT,10)
    handle.setopt(pycurl.TIMEOUT,cfg["load_timeout"]+30)
    handle.setopt(pycurl.NOSIGNAL,1)
    handle.setopt(pycurl.WRITEFUNCTION,response.write)
    if stop is not None:
        handle.setopt(pycurl.NOPROGRESS,0)
        handle.setopt(pycurl.XFERINFOFUNCTION,lambda *_: int(stop.is_set()))
    if payload is not None:
        handle.setopt(pycurl.POSTFIELDS,payload)
        handle.setopt(pycurl.CUSTOMREQUEST,"PUT")
    if headers:
        handle.setopt(pycurl.HTTPHEADER,[f"{k}: {v}" for k,v in headers.items()])
    if method:
        handle.setopt(pycurl.CUSTOMREQUEST,method)
    handle.perform()
    status = handle.getinfo(pycurl.RESPONSE_CODE)
    body = response.getvalue()
    try:
        result = orjson.loads(body)
    except orjson.JSONDecodeError:
        raise RuntimeError(f"Stream Load HTTP {status}: invalid JSON response {body[:300]!r}") from None
    if not isinstance(result,dict):
        raise RuntimeError(f"Stream Load HTTP {status}: unexpected response type")
    return status,result


def load_result_text(result):
    try:
        text = orjson.dumps(result,option=orjson.OPT_SORT_KEYS).decode()
    except Exception:
        text = repr(result)
    return text[:4000]


def begin_merge_request(con, mapping, delivery, part, label, payload):
    row = con.execute("SELECT lane FROM deliveries WHERE id=?",(delivery,)).fetchone()
    if not row:
        raise RuntimeError(f"delivery {delivery} disappeared before Merge Commit request")
    now = time.time()
    digest = hashlib.sha256(payload).hexdigest()
    with state_transaction(con):
        con.execute("""
            INSERT INTO merge_uncertain(
                delivery_id,part,table_name,lane,label,payload_sha256,reason,created,updated
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(delivery_id,part) DO UPDATE SET
                reason=excluded.reason,updated=excluded.updated
        """,(delivery,part,mapping_key(mapping),int(row[0]),label,digest,
             "request_inflight_no_txn_id",now,now))


def clear_merge_request(con, delivery, part):
    with state_transaction(con):
        con.execute("DELETE FROM merge_uncertain WHERE delivery_id=? AND part=?",(delivery,part))


def mark_merge_uncertain(con, mapping, delivery, part, reason):
    now = time.time()
    with state_transaction(con):
        con.execute("""
            UPDATE merge_uncertain SET reason=?,updated=?
            WHERE delivery_id=? AND part=?
        """,(str(reason)[:4000],now,delivery,part))
    log(f"MERGE UNCERTAIN table={mapping_key(mapping)} delivery={delivery} part={part} "
        f"reason={str(reason)[:1000]} automatic_retry=0")


def unresolved_merge_uncertain(con):
    return con.execute("""
        SELECT delivery_id,part,table_name,lane,label,payload_sha256,reason,created
        FROM merge_uncertain ORDER BY created,delivery_id,part
    """).fetchall()


def merge_table_quarantined(runtime, table):
    if not runtime.get('quarantined_tables'):
        return False
    with runtime['control_lock']:
        return table in runtime.get('quarantined_tables',{})


def quarantine_merge_table(con, table, runtime, reason):
    rows = con.execute(
        'SELECT delivery_id,part FROM merge_uncertain WHERE table_name=? ORDER BY created',
        (table,)).fetchall()
    if not rows:
        return False
    detail = dict(reason=str(reason)[:2000],parts=len(rows),
                  delivery_id=rows[0][0],part=rows[0][1],replay_disabled=True)
    with runtime['control_lock']:
        quarantined = runtime.setdefault('quarantined_tables',{})
        first = table not in quarantined
        quarantined[table] = detail
    if first:
        log(f"MERGE QUARANTINED table={table} parts={len(rows)} "
            f"replay=0 journal_retained=1 unrelated_targets_continue=1 reason={detail['reason']}")
    return True


def quarantine_pending_merges(con, runtime):
    # Run after legacy sink identity migration: block the actual sink identities,
    # including draining old plans. Never delete or silently acknowledge markers.
    tables = con.execute('SELECT DISTINCT table_name FROM merge_uncertain').fetchall()
    for table, in tables:
        quarantine_merge_table(con,table,runtime,'unresolved request restored from durable state')
    return len(tables)


def curl_error_before_request(exc):
    code = int(exc.args[0]) if getattr(exc,"args",None) else -1
    return code in {
        int(pycurl.E_COULDNT_RESOLVE_HOST),
        int(pycurl.E_COULDNT_CONNECT),
    }


def renew_load_label(con, delivery, part, old_label, reason):
    label = old_label + "_r_" + uuid.uuid4().hex[:12]
    with state_transaction(con):
        con.execute("UPDATE load_parts SET label=?,txn_id=NULL WHERE delivery_id=? AND part=?",
                    (label,delivery,part))
    log(f"LOAD RESET delivery={delivery} part={part} old_label={old_label} "
        f"new_label={label} reason={reason}")
    return label


def retryable_mysql_error(exc):
    return isinstance(exc,(pymysql.err.OperationalError,pymysql.err.InterfaceError)) and \
        bool(exc.args) and exc.args[0] in (0,1040,1205,1213,2002,2003,2006,2013,2055)


def transaction_history_state(con, cfg, txn_id):
    """A missing live transaction is NOT an abort. Only exact terminal evidence can resolve it."""
    details = []
    for source in ("information_schema.loads","_statistics_.loads_history"):
        try:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT LABEL,STATE,RUNTIME_DETAILS,ERROR_MSG FROM "+source+
                    " WHERE DB_NAME=%s AND "
                    "get_json_string(CAST(RUNTIME_DETAILS AS VARCHAR),'$.txn_id')=%s",
                    (cfg["sr"]["database"],str(int(txn_id))))
                rows = cur.fetchall()
        except pymysql.err.DatabaseError as exc:
            if retryable_mysql_error(exc):
                raise
            details.append(f"{source}: {exc}")
            continue
        terminal = {}
        for label,state,metadata,error in rows:
            try:
                metadata = metadata if isinstance(metadata,dict) else orjson.loads(metadata)
                if str(metadata.get("txn_id")) != str(int(txn_id)):
                    continue
            except (TypeError,ValueError,AttributeError):
                continue
            state = str(state).upper()
            if state in ("FINISHED","CANCELLED"):
                terminal[state] = dict(source=source,Label=label,State=state,
                                       TxnId=int(txn_id),Message=error or "",RuntimeDetails=metadata)
        if len(terminal) > 1:
            return "pending",dict(Message=f"conflicting terminal history for txn={txn_id} in {source}")
        if terminal:
            state,detail = next(iter(terminal.items()))
            return ("visible" if state == "FINISHED" else "aborted"),detail
        details.append(f"{source}: no exact terminal record")
    return "pending",dict(Message="; ".join(details))


def wait_visible(cfg, txn_id, stop):
    started = time.monotonic()
    next_warning,failures,con = 0,0,None
    try:
        while not stop.is_set():
            delay,detail = 0.1,{}
            try:
                if con is None:
                    con = mysql_connect(cfg,target=True)
                with con.cursor() as cur:
                    try:
                        cur.execute("SHOW TRANSACTION FROM "+sql_name(cfg["sr"]["database"],True)+" WHERE id=%s",
                                    (int(txn_id),))
                        row = cur.fetchone()
                    except pymysql.err.ProgrammingError as exc:
                        # 1064 also covers real SQL errors: never swallow those indiscriminately.
                        if not (exc.args and exc.args[0] == 1064 and re.search(
                                r"transaction with id\s+"+str(int(txn_id))+r"\s+does not exist",str(exc),re.I)):
                            raise
                        row = None
                    if row:
                        detail = dict(zip([d[0] for d in cur.description],row))
                        status = str(next((v for k,v in detail.items()
                                           if str(k).lower() == "transactionstatus"),"")).upper()
                        if status in ("VISIBLE","ABORTED"):
                            return ("visible" if status == "VISIBLE" else "aborted"),detail
                if not row:
                    state,detail = transaction_history_state(con,cfg,txn_id)
                    if state != "pending":
                        log(f"TXN RECOVERED txn={txn_id} state={state} "
                            f"source={detail['source']} label={detail['Label']}")
                        return state,detail
                    delay = 5
                failures = 0
            except (pymysql.err.OperationalError,pymysql.err.InterfaceError) as exc:
                if not retryable_mysql_error(exc):
                    raise
                if con is not None:
                    con.close()
                    con = None
                failures += 1
                delay = min(30,0.5*2**min(failures,6))
                detail = dict(Message=str(exc))
            now = time.monotonic()
            overdue = now-started >= cfg["load_timeout"]
            if overdue:
                delay = max(delay,5)
            if (delay >= 1 or overdue) and now >= next_warning:
                log(f"TXN WAIT txn={txn_id} waited_seconds={now-started:.1f} "
                    f"retry_seconds={delay} resend=0 journal_retained=1 detail={load_result_text(detail)}")
                next_warning = now+30
            stop.wait(delay)
    finally:
        if con is not None:
            con.close()
    raise RuntimeError(f"stopped while confirming transaction {txn_id}; its delivery remains pending")



def merge_commit_headers(mapping, cfg, request_id):
    columns = mapping["_output_columns"] + (["_cdc_seq"] if mapping["_target_sequence"] else []) + ["__op"]
    headers = {"Expect":"100-continue","Content-Type":"application/json",
               "format":"json","read_json_by_line":"true","strip_outer_array":"false",
               "ignore_json_size":"true","max_filter_ratio":"0","strict_mode":"true",
               "log_rejected_record_num":"10","timezone":"+00:00",
               "timeout":str(cfg["load_timeout"]),
               "columns":",".join(sql_name(n,True) for n in columns),
               "label":request_id,
               "enable_merge_commit":"true","merge_commit_async":"true",
               "merge_commit_interval_ms":str(cfg["merge_commit_interval_ms"]),
               "merge_commit_parallel":str(cfg["merge_commit_parallel"])}
    if cfg["compression"]:
        headers["compression"] = cfg["compression"]
    return headers


def merge_stream_load_url(cfg, mapping):
    sr = cfg["sr"]
    host = sr["host"]
    if ":" in host and not host.startswith("["):
        host = "["+host+"]"
    return (f"http://{host}:{sr['http_port']}/api/{quote(sr['database'],safe='')}/"
            f"{quote(mapping['sr_table'],safe='')}/_stream_load")


def submit_merge_async(handle, con, mapping, delivery, part, cfg, stop, runtime):
    """Submit one immutable payload to Merge Commit async and persist its server txn id."""
    label,payload,nrows,visible,txn_id = con.execute("""
        SELECT label,payload,nrows,visible,txn_id FROM load_parts WHERE delivery_id=? AND part=?
    """,(delivery,part)).fetchone()
    if visible:
        return txn_id,{"Status":"LOCAL_VISIBLE"}
    if txn_id is not None:
        return int(txn_id),{"Status":"LOCAL_PENDING","TxnId":int(txn_id)}

    url = merge_stream_load_url(cfg,mapping)
    headers = merge_commit_headers(mapping,cfg,label)
    last_error = None
    attempt = 0
    while not stop.is_set():
        if stop.is_set():
            raise RuntimeError("stopped with a pending Merge Commit delivery")
        if version_recovery_active(runtime,mapping_key(mapping)):
            if not wait_version_recovery(runtime,mapping_key(mapping),stop):
                raise RuntimeError("stopped during version recovery with a pending Merge Commit delivery")
        begin_merge_request(con,mapping,delivery,part,label,payload)
        try:
            # Drain an already-started request on graceful shutdown so its TxnId can be saved.
            # HTTP's existing load_timeout+30 still bounds the wait; do not manufacture uncertainty
            # by cancelling an upload only because SIGTERM/another worker set the stop event.
            status,result = curl_request(handle,cfg,url,payload,headers,None)
            state = str(result.get("Status","")).lower()
            if 200 <= status < 300 and state == "success":
                remote_txn = result.get("TxnId")
                remote_label = str(result.get("Label","") or "")
                if remote_txn is None or int(remote_txn) < 0 or not remote_label:
                    raise RuntimeError("Merge Commit async success response lacks TxnId/Label: "+
                                       load_result_text(result))
                remote_txn = int(remote_txn)
                # Persist the server transaction identity and clear the write-ahead uncertainty marker atomically.
                with state_transaction(con):
                    con.execute("UPDATE load_parts SET txn_id=? WHERE delivery_id=? AND part=?",
                                (remote_txn,delivery,part))
                    con.execute("DELETE FROM merge_uncertain WHERE delivery_id=? AND part=?",
                                (delivery,part))
                left_merge_ms = int(result.get("LeftMergeTimeMs",0) or 0)
                metric_add_merge(runtime,mapping_key(mapping),remote_txn,nrows,left_merge_ms)
                if cfg.get("detail_logs",False):
                    log(f"MERGE ACCEPTED table={mapping_key(mapping)} delivery={delivery} part={part} "
                        f"rows={nrows} txn={remote_txn} server_label={remote_label} "
                        f"left_merge_ms={left_merge_ms}")
                return remote_txn,result
            if status in (401,403,404):
                clear_merge_request(con,delivery,part)
                raise ValueError(f"Merge Commit HTTP {status}: check endpoint/permissions; "
                                 f"response={load_result_text(result)}")
            if state in ("fail","cancelled","canceled"):
                message = str(result.get("Message",""))
                kind = pressure_kind(message)
                if kind is None:
                    clear_merge_request(con,delivery,part)
                    raise ValueError("Merge Commit request failed: "+load_result_text(result))
                clear_merge_request(con,delivery,part)
                last_error = load_result_text(result)
                table = mapping_key(mapping)
                if kind == "version":
                    enter_version_recovery(runtime,table,cfg,"request pressure: "+message)
                elif kind == "compaction":
                    writer_pressure(runtime,table,cfg,"request compaction pressure: "+message[:300],severe=True)
                else:
                    writer_pressure(runtime,table,cfg,"request transaction pressure: "+message[:300],severe=False)
            else:
                reason = f"HTTP {status}, response={load_result_text(result)}"
                raise RuntimeError(reason)
        except ValueError:
            raise
        except pycurl.error as exc:
            if curl_error_before_request(exc):
                clear_merge_request(con,delivery,part)
                last_error = str(exc)
            else:
                mark_merge_uncertain(con,mapping,delivery,part,exc)
                raise RuntimeError(
                    f"Merge Commit request outcome is uncertain for delivery={delivery} part={part}; "
                    "journal retained and automatic replay disabled"
                ) from exc
        except (RuntimeError,pymysql.err.OperationalError) as exc:
            mark_merge_uncertain(con,mapping,delivery,part,exc)
            raise RuntimeError(
                f"Merge Commit request outcome is uncertain for delivery={delivery} part={part}; "
                "journal retained and automatic replay disabled"
            ) from exc
        metric_increment(runtime,mapping_key(mapping),"merge_retries")
        log(f"MERGE RETRY table={mapping_key(mapping)} delivery={delivery} part={part} "
            f"attempt={attempt+1} safe_pre_send=1 reason={last_error}")
        stop.wait(min(30,0.25*2**min(attempt,7)))
        attempt += 1
    raise RuntimeError(f"unresolved Merge Commit async request delivery={delivery} part={part}: "
                       f"{last_error}; journal retained")


def merge_async_delivery(handle, con, mapping, delivery, cfg, runtime):
    """Send all parts first, then gate journal progress on every merge transaction becoming VISIBLE."""
    stop,table = runtime["stop"],mapping_key(mapping)
    results = []
    while not stop.is_set():
        parts = con.execute("""
            SELECT part FROM load_parts WHERE delivery_id=? AND visible=0 ORDER BY part
        """,(delivery,)).fetchall()
        if not parts:
            return results

        results = []
        for (part,) in parts:
            txn_id,result = submit_merge_async(handle,con,mapping,delivery,part,cfg,stop,runtime)
            results.append((part,txn_id,result))

        txn_ids = sorted({int(txn_id) for _,txn_id,_ in results if txn_id is not None})
        retry = False
        for txn_id in txn_ids:
            state,detail = wait_visible(cfg,txn_id,stop)
            if state == "visible":
                with state_transaction(con):
                    con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=? AND txn_id=?",
                                (delivery,txn_id))
                continue

            failure = load_result_text(detail)
            kind = pressure_kind(failure)
            pressure = kind is not None
            if not pressure and any(term in failure.lower() for term in
                    ("data quality","parse error","column","filtered","invalid argument")):
                raise ValueError(f"Merge Commit transaction {txn_id} aborted: {failure}")

            aborted = con.execute("""
                SELECT part,label FROM load_parts
                WHERE delivery_id=? AND txn_id=? AND visible=0 ORDER BY part
            """,(delivery,txn_id)).fetchall()
            for part,label in aborted:
                renew_load_label(con,delivery,part,label,f"merge txn {txn_id} aborted: {failure}")
            delay = min(cfg["pressure_max_seconds"],2 if pressure else 1)
            if kind == "version":
                enter_version_recovery(runtime,table,cfg,
                                       f"merge txn {txn_id}: {failure}")
                delay = 0
            elif kind == "compaction":
                writer_pressure(runtime,table,cfg,
                                f"merge txn {txn_id} compaction: {failure[:300]}",severe=True)
            elif kind == "transaction":
                writer_pressure(runtime,table,cfg,
                                f"merge txn {txn_id} transaction: {failure[:300]}",severe=False)
            log(f"MERGE BACKOFF table={table} txn={txn_id} pressure={int(pressure)} "
                f"seconds={delay} reason={failure}")
            if stop.wait(delay):
                break
            retry = True
        if not retry:
            remaining = con.execute(
                "SELECT COUNT(*) FROM load_parts WHERE delivery_id=? AND visible=0",(delivery,)).fetchone()[0]
            if not remaining:
                return results
    raise RuntimeError("stopped with durable pending Merge Commit delivery")


def stream_load(handle, con, mapping, delivery, part, cfg, stop):
    label,payload,nrows,visible,txn_id = con.execute("""
        SELECT label,payload,nrows,visible,txn_id FROM load_parts WHERE delivery_id=? AND part=?
    """, (delivery,part)).fetchone()
    if visible:
        return {"Status":"LOCAL_VISIBLE"}
    if txn_id is not None:
        state,detail = wait_visible(cfg,txn_id,stop)
        if state == "visible":
            with state_transaction(con):
                con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=? AND part=?", (delivery,part))
            return {"Status":"RECOVERED_VISIBLE","TxnId":txn_id}
        label = renew_load_label(con,delivery,part,label,load_result_text(detail))
        txn_id = None
    result = None
    if txn_id is None:
        sr = cfg["sr"]
        host = sr["host"]
        if ":" in host and not host.startswith("["):
            host = "["+host+"]"
        url = f"http://{host}:{sr['http_port']}/api/{quote(sr['database'],safe='')}/{quote(mapping['sr_table'],safe='')}/_stream_load"
        columns = mapping["_output_columns"] + (["_cdc_seq"] if mapping["_target_sequence"] else []) + ["__op"]
        headers = {"Expect":"100-continue","Content-Type":"application/json",
                   "format":"json","read_json_by_line":"true","strip_outer_array":"false",
                   "ignore_json_size":"true","max_filter_ratio":"0","strict_mode":"true",
                   "log_rejected_record_num":"10",
                   "timezone":"+00:00","timeout":str(cfg["load_timeout"]),
                   "columns":",".join(sql_name(n,True) for n in columns)}
        headers["label"] = label
        if cfg["compression"]:
            headers["compression"] = cfg["compression"]
        last_error = None
        for attempt in range(cfg["retry_max"]):
            if stop.is_set():
                raise RuntimeError("stopped with a pending delivery")
            try:
                status,result = curl_request(handle,cfg,url,payload,headers,stop)
                state = str(result.get("Status","")).lower()
                if 200 <= status < 300 and state == "success":
                    if int(result.get("NumberFilteredRows",0)) or int(result.get("NumberUnselectedRows",0)):
                        raise ValueError(f"load rejected/filtered rows: label={label}, result={result}")
                    if int(result.get("NumberLoadedRows",nrows)) != nrows:
                        raise ValueError(f"load row count mismatch: label={label}, result={result}")
                    break
                if state == "label already exists":
                    existing = str(result.get("ExistingJobStatus","")).upper()
                    if existing in ("FINISHED","VISIBLE"):
                        break
                    if existing in ("ABORTED","CANCELLED","CANCELED"):
                        label = renew_load_label(con,delivery,part,label,
                                                 "existing job "+existing)
                        headers["label"] = label
                        continue
                    # Retry the SAME label until its original load is resolved.
                    last_error = f"{label}: existing job is {existing}; response={load_result_text(result)}"
                elif state == "publish timeout" and result.get("TxnId") is not None:
                    txn_id = int(result["TxnId"])
                    with state_transaction(con):
                        con.execute("UPDATE load_parts SET txn_id=? WHERE delivery_id=? AND part=?",
                                    (txn_id,delivery,part))
                    txn_state,detail = wait_visible(cfg,txn_id,stop)
                    if txn_state == "visible":
                        break
                    label = renew_load_label(con,delivery,part,label,load_result_text(detail))
                    headers["label"] = label
                    txn_id = None
                    continue
                elif state in ("fail","cancelled","canceled"):
                    if version_pressure(str(result.get("Message",""))):
                        # Legacy requests retain their original label until ABORTED is confirmed.
                        raise RuntimeError("legacy version pressure: "+load_result_text(result))
                    raise ValueError(f"Stream Load failed: label={label}, response={load_result_text(result)}")
                elif status in (401,403,404):
                    raise ValueError(f"Stream Load HTTP {status}: check endpoint/permissions; "
                                     f"response={load_result_text(result)}")
                else:
                    last_error = f"HTTP {status}, response={load_result_text(result)}"
            except (pycurl.error,RuntimeError,pymysql.err.OperationalError) as exc:
                last_error = str(exc)
            log(f"RETRY table={mapping_key(mapping)} label={label} attempt={attempt+1}: {last_error}")
            stop.wait(min(10,0.25*2**attempt))
        else:
            raise RuntimeError(f"unresolved Stream Load {label}: {last_error}; journal retained")
    with state_transaction(con):
        con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=? AND part=?", (delivery,part))
    return result or {"Status":"VISIBLE","TxnId":txn_id}




def writer_target(runtime, table):
    with runtime["control_lock"]:
        return int(runtime["active_writers"][table])


def version_recovery_active(runtime, table):
    with runtime["control_lock"]:
        return bool(runtime["version_recovery"].get(table,False))


def set_writer_target(runtime, table, cfg, target, reason, pressure=False, minimum=None):
    floor = cfg["writer_min"] if minimum is None else max(1,int(minimum))
    target = max(floor,min(cfg["writer_max"],int(target)))
    changed = False
    with runtime["control_lock"]:
        old = int(runtime["active_writers"][table])
        if pressure:
            runtime["last_pressure"][table] = time.time()
        if target != old:
            runtime["active_writers"][table] = target
            runtime["last_scale"][table] = time.time()
            changed = True
    if changed:
        log(f"WRITER SCALE table={table} old={old} new={target} reason={reason}")
        wake_loaders(runtime,table)
    return target


def writer_pressure(runtime, table, cfg, reason, severe=False):
    current = writer_target(runtime,table)
    with runtime["control_lock"]:
        floor = max(1,min(cfg["writer_min"],int(runtime.get("resource_writer_cap",cfg["writer_min"]))))
    target = max(floor,current//2 if severe or current > floor else current)
    runtime["pressure_until"][table] = max(runtime["pressure_until"].get(table,0),
                                           time.time()+(10 if severe else 5))
    return set_writer_target(runtime,table,cfg,target,reason,pressure=True)


def enter_version_recovery(runtime, table, cfg, reason):
    now = time.time()
    entered = False
    old = None
    floor = 1
    with runtime["control_lock"]:
        old = int(runtime["active_writers"][table])
        floor = max(1,min(cfg["writer_min"],int(runtime.get("resource_writer_cap",cfg["writer_min"]))))
        if not runtime["version_recovery"].get(table,False):
            entered = True
        runtime["version_recovery"][table] = True
        runtime["version_recovery_good"][table] = 0
        runtime["active_writers"][table] = floor
        runtime["last_pressure"][table] = now
        runtime["last_scale"][table] = now
    if entered:
        metric_increment(runtime,table,"version_pauses")
        log(f"VERSION PAUSE table={table} writers={old}->{floor} "
            f"reason={reason[:1000]}")
    wake_loaders(runtime,table)
    return entered


def update_version_recovery(runtime, table, cfg, rowsets):
    now = time.time()
    recovered = False
    good = 0
    with runtime["control_lock"]:
        if not runtime["version_recovery"].get(table,False):
            return False
        if rowsets < cfg["rowset_yellow"]:
            runtime["version_recovery_good"][table] += 1
        else:
            runtime["version_recovery_good"][table] = 0
        good = runtime["version_recovery_good"][table]
        if good >= cfg["version_recovery_checks"]:
            floor = max(1,min(cfg["writer_min"],int(runtime.get("resource_writer_cap",cfg["writer_min"]))))
            runtime["version_recovery"][table] = False
            runtime["version_recovery_good"][table] = 0
            runtime["active_writers"][table] = floor
            runtime["last_pressure"][table] = now
            runtime["last_scale"][table] = now
            recovered = True
        else:
            floor = max(1,min(cfg["writer_min"],int(runtime.get("resource_writer_cap",cfg["writer_min"]))))
    if recovered:
        metric_increment(runtime,table,"version_recovers")
        log(f"VERSION RECOVERED table={table} max_rowset={rowsets} "
            f"confirmed={cfg['version_recovery_checks']} writers={floor}")
        wake_loaders(runtime,table)
    return recovered


def wait_version_recovery(runtime, table, stop):
    while version_recovery_active(runtime,table):
        if stop.wait(0.1):
            return False
    return not stop.is_set()


def starrocks_max_rowset(target, cfg, mapping):
    with target.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(MAX(t.NUM_ROWSET),0),COUNT(*)
            FROM information_schema.be_tablets t
            JOIN information_schema.tables_config c ON t.TABLE_ID=c.TABLE_ID
            WHERE c.TABLE_NAME=%s AND c.TABLE_SCHEMA=%s
        """,(mapping["sr_table"],cfg["sr"]["database"]))
        row = cur.fetchone()
        if not row or int(row[1] or 0) <= 0:
            raise RuntimeError(f"rowset monitor found no tablets for "
                               f"{cfg['sr']['database']}.{mapping['sr_table']}")
        return int(row[0] or 0)


def state_checkpoint_worker(cfg, runtime):
    stop = runtime["stop"]
    con = open_state(cfg["state"])
    wal_path = cfg["state"]+"-wal"
    last_error_log = 0.0
    try:
        while not stop.wait(STATE_WAL_CHECKPOINT_INTERVAL):
            try:
                size = os.path.getsize(wal_path)
                if size < STATE_WAL_CHECKPOINT_BYTES:
                    continue
                started = time.monotonic()
                busy,log_pages,checkpointed = con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                elapsed = time.monotonic()-started
                if cfg.get("detail_logs",False):
                    log(f"STATE CHECKPOINT wal_bytes={size} log_pages={log_pages} "
                        f"checkpointed={checkpointed} busy={busy} seconds={elapsed:.3f}")
            except (OSError,sqlite3.Error) as exc:
                now = time.time()
                if now-last_error_log >= 30:
                    log(f"STATE CHECKPOINT advisory error: {exc}")
                    last_error_log = now
        try:
            con.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except sqlite3.Error:
            pass
    finally:
        con.close()


STATE_GC_BATCH_BYTES = 8*1024*1024
STATE_GC_INTERVAL = 0.02


def retired_job_gc_batch(con, byte_limit=STATE_GC_BATCH_BYTES):
    selected,total = [],0
    for job_id,nbytes in con.execute("""
        SELECT r.job_id,length(j.payload)
        FROM retired_jobs r JOIN jobs j ON j.id=r.job_id
        ORDER BY r.job_id LIMIT 256
    """):
        nbytes = int(nbytes)
        if selected and total+nbytes > byte_limit:
            break
        selected.append(int(job_id))
        total += nbytes
        if total >= byte_limit:
            break
    if not selected:
        return 0,0
    marks = ",".join("?" for _ in selected)
    with state_transaction(con):
        con.execute(f"DELETE FROM jobs WHERE id IN ({marks})",selected)
    return len(selected),total


def state_gc_worker(cfg, runtime):
    stop = runtime["stop"]
    con = open_state(cfg["state"])
    last_error_log = 0.0
    last_retention = 0.0
    try:
        while not stop.is_set():
            try:
                count,nbytes = retired_job_gc_batch(con)
                now = time.time()
                if now-last_retention >= 60:
                    value_cutoff = now-float(
                        cfg.get("overflow_value_days",90))*86400
                    metadata_cutoff = now-float(
                        cfg.get("overflow_metadata_days",365))*86400
                    with state_transaction(con):
                        con.execute("""
                            UPDATE field_overflow
                            SET value=X'',value_pruned=1
                            WHERE action='null' AND value_pruned=0 AND created<?
                        """,(value_cutoff,))
                        con.execute("""
                            DELETE FROM field_overflow
                            WHERE action='null' AND created<?
                        """,(metadata_cutoff,))
                    stateful_gc=stateful_physical_registry.gc_retired(
                        con,limit=64)
                    if stateful_gc and cfg.get("detail_logs",False):
                        log(
                            "STATEFUL PHYSICAL GC count=%d tasks=%s"
                            % (
                                len(stateful_gc),
                                ",".join(
                                    item["task_id"]
                                    for item in stateful_gc),
                            ))
                    if cfg.get("shared_source_state",False):
                        incomplete = source_state.status(con)["incomplete_relations"]
                        if not incomplete:
                            source_gc = source_state.gc(con)
                            sync_source_base_catalog(con)
                            if (
                                cfg.get("detail_logs",False)
                                and (source_gc["versions"] or source_gc["commits"])
                            ):
                                log(
                                    f"SOURCE GC floor={source_gc['floor']} "
                                    f"versions={source_gc['versions']} "
                                    f"commits={source_gc['commits']}")
                    # Partial/offline runtimes (notably the release scale
                    # fixture) intentionally omit plan-manager state. Retired
                    # job GC must remain independent of optional catalog GC.
                    catalog_path = cfg.get("catalog")
                    if (
                        catalog_path
                        and runtime.get("plan_lock") is not None
                        and "active_plan_version" in runtime
                    ):
                        live = live_plan_versions(con,runtime)
                        removed = cdc_catalog.prune_plans(
                            catalog_path,live,cfg.get("plan_retain",32))
                        if removed and cfg.get("detail_logs",False):
                            log(f"CATALOG GC plans={removed}")
                    last_retention = now
                if count:
                    if cfg.get("detail_logs",False):
                        log(f"STATE GC jobs={count} bytes={nbytes}")
                    stop.wait(STATE_GC_INTERVAL)
                else:
                    stop.wait(0.2)
            except (OSError,sqlite3.Error) as exc:
                now = time.time()
                if now-last_error_log >= 30:
                    log(f"STATE GC advisory error: {exc}")
                    last_error_log = now
                stop.wait(0.5)
    finally:
        con.close()



def resource_monitor_worker(cfg, prepared, runtime):
    stop = runtime["stop"]
    last_signature = None
    while not stop.is_set():
        tables = max(1,len(prepared))
        sample = current_resource_headroom(cfg,tables)
        signature = (
            sample["writer_cap"],sample["pause_snapshot"],
            sample["memory_pressure"],sample["memory_hard_exceeded"],
            sample["disk_pressure"],sample["disk_hard_exceeded"],
            sample["cpu_pressure"])
        with runtime["control_lock"]:
            runtime["resource_writer_cap"] = sample["writer_cap"]
            runtime["resource_pause_snapshot"] = sample["pause_snapshot"]
            runtime["resource_sample"] = sample
        if signature != last_signature:
            log(
                "RESOURCE RUNTIME cpu_target=%d writer_cap=%d load1=%.2f "
                "memory_available_mb=%d memory_known=%d tree_rss_mb=%.1f tree_processes=%d "
                "memory_pressure=%d memory_hard=%d disk_free_gb=%.1f disk_known=%d "
                "disk_pressure=%d disk_hard=%d cpu_pressure=%d snapshot_pause=%d"
                % (
                    sample["cpu_target"],sample["writer_cap"],sample["load1"],
                    sample["memory_available_mb"],int(sample["memory_known"]),
                    sample["process_tree_rss_mb"],sample["process_tree_processes"],
                    int(sample["memory_pressure"]),int(sample["memory_hard_exceeded"]),
                    sample["disk_free_bytes"]/1024**3 if sample["disk_known"] else -1,
                    int(sample["disk_known"]),int(sample["disk_pressure"]),
                    int(sample["disk_hard_exceeded"]),int(sample["cpu_pressure"]),
                    int(sample["pause_snapshot"]),
                )
            )
            last_signature = signature
        if sample["memory_hard_exceeded"] or sample["disk_hard_exceeded"]:
            message = (
                "CDC resource hard stop memory_hard=%d tree_rss_mb=%.1f "
                "memory_budget_mb=%d disk_hard=%d disk_free_bytes=%d "
                "disk_reserve_bytes=%d"
                % (
                    int(sample["memory_hard_exceeded"]),
                    sample["process_tree_rss_mb"],int(cfg["resource"]["memory_mb"]),
                    int(sample["disk_hard_exceeded"]),sample["disk_free_bytes"],
                    int(cfg["min_free_bytes"]),
                )
            )
            with runtime["error_lock"]:
                runtime["errors"].append(("resource_monitor_worker",message))
            log("FATAL "+message)
            stop.set()
            wake_loaders(runtime)
            return
        if cfg["load_mode"] == "merge_async":
            for mapping in prepared:
                table = mapping_key(mapping)
                current = writer_target(runtime,table)
                if current > sample["writer_cap"]:
                    set_writer_target(
                        runtime,table,cfg,sample["writer_cap"],
                        "server resource headroom reduced",pressure=True,minimum=1)
        stop.wait(cfg["resource_monitor_seconds"])

def adaptive_writer_controller(cfg, prepared, runtime):
    stop = runtime["stop"]
    target = None
    last_error_log = 0
    try:
        while not stop.is_set():
            try:
                if target is None:
                    target = mysql_connect(cfg,target=True)
                now = time.time()
                for mapping in prepared:
                    table = mapping_key(mapping)
                    rowsets = starrocks_max_rowset(target,cfg,mapping)
                    runtime["max_rowset"][table] = rowsets
                    if version_recovery_active(runtime,table):
                        update_version_recovery(runtime,table,cfg,rowsets)
                        continue
                    current = writer_target(runtime,table)
                    if rowsets >= cfg["rowset_red"]:
                        writer_pressure(runtime,table,cfg,
                                        f"max_rowset={rowsets}>={cfg['rowset_red']}",severe=True)
                        continue
                    if rowsets >= cfg["rowset_yellow"]:
                        with runtime["control_lock"]:
                            runtime["last_pressure"][table] = now
                        continue
                    with runtime["control_lock"]:
                        quiet = now-runtime["last_pressure"][table]
                        since_scale = now-runtime["last_scale"][table]
                    with runtime["control_lock"]:
                        resource_cap = int(runtime.get("resource_writer_cap",cfg["writer_max"]))
                    allowed_max = min(cfg["writer_max"],resource_cap)
                    if current < allowed_max and quiet >= cfg["writer_ramp_seconds"] and \
                       since_scale >= cfg["writer_ramp_seconds"]:
                        set_writer_target(runtime,table,cfg,current+1,
                                          f"healthy max_rowset={rowsets} for {int(quiet)}s")
                stop.wait(cfg["writer_monitor_seconds"])
            except Exception as exc:
                # Proactive monitoring is advisory. Missing information_schema privileges or
                # version-specific metadata must hold concurrency, never stop CDC.
                if target is not None:
                    try:
                        target.close()
                    except Exception:
                        pass
                    target = None
                now = time.time()
                if now-last_error_log >= 30:
                    paused = ",".join(mapping_key(m) for m in prepared
                                      if version_recovery_active(runtime,mapping_key(m))) or "none"
                    log(f"WRITER MONITOR hold concurrency: {exc}; version_paused={paused}")
                    last_error_log = now
                with runtime["control_lock"]:
                    for mapping in prepared:
                        runtime["last_pressure"][mapping_key(mapping)] = now
                stop.wait(min(5,cfg["writer_monitor_seconds"]))
    finally:
        if target is not None:
            target.close()


def merge_candidate_lanes(con, table):
    existing = [int(r[0]) for r in con.execute(
        "SELECT lane FROM deliveries WHERE table_name=? ORDER BY rowid",(table,)).fetchall()]
    pending = [int(r[0]) for r in con.execute("""
        SELECT j.lane
        FROM active_jobs j
        JOIN (
            SELECT lane,MIN(id) AS first_id
            FROM active_jobs WHERE table_name=? GROUP BY lane
        ) h ON h.lane=j.lane AND h.first_id=j.id
        LEFT JOIN job_assignments a ON a.job_id=j.id
        WHERE j.table_name=? AND a.job_id IS NULL
        ORDER BY j.id
    """,(table,table)).fetchall()]
    seen = set(existing)
    return existing+[lane for lane in pending if lane not in seen]


def process_merge_lane(con, engine_cache, handle, table, lane, cfg, runtime):
    stop = runtime["stop"]
    if merge_table_quarantined(runtime,table):
        return False
    if version_recovery_active(runtime,table):
        return False

    blocked = lane_blocking_delivery(con,table,lane)
    if blocked is not None:
        owner = con.execute("SELECT lane FROM deliveries WHERE id=?",(blocked,)).fetchone()
        if not owner:
            raise RuntimeError(f"journal delivery {blocked} has assigned jobs but no delivery row")
        if int(owner[0]) != int(lane):
            return False
        delivery = blocked
    else:
        ready = con.execute("SELECT id FROM deliveries WHERE table_name=? AND lane=?",
                            (table,lane)).fetchone()
        if ready:
            delivery = ready[0]
        else:
            pending = pending_batch(con,table,lane,cfg)
            if not pending:
                return False
            kind,created,pending_rows,pending_bytes,full,_ = pending
            if kind == "cdc" and not full and time.time()-created < cfg["batch_ms"]/1000:
                return False
            if kind == "snapshot":
                delivery = claim_snapshot_bundle(con,table,lane,cfg,runtime)
            else:
                delivery = claim_cdc_bundle(con,table,lane,cfg,runtime)
            if not delivery:
                return False

    version = delivery_plan_version(con,delivery)
    mapping = runtime_mapping(runtime,version,table)
    engine = plan_engine(engine_cache,runtime,cfg,version,table)

    lanes = delivery_lanes(con,delivery)
    if not lanes or int(lane) not in lanes:
        raise RuntimeError(f"delivery {delivery} owner lane {lane} is inconsistent with jobs {lanes!r}")

    extra_locks = []
    for other_lane in lanes:
        if other_lane == int(lane):
            continue
        lock = runtime["lane_locks"].get((table,other_lane))
        if lock is None:
            raise RuntimeError(f"missing lane lock table={table} lane={other_lane}")
        if not lock.acquire(blocking=False):
            for held in reversed(extra_locks):
                held.release()
            return False
        extra_locks.append(lock)

    try:
        if merge_table_quarantined(runtime,table) or version_recovery_active(runtime,table):
            return False
        started = time.monotonic()
        try:
            prepared = prepare_delivery(con,engine,mapping,delivery,cfg)
        except Exception as exc:
            if not is_duckdb_oom(exc):
                raise
            # The fast path remains unchanged. OOM recovery is local to this
            # unsent durable delivery: free cached engines, reduce concurrency,
            # and retry a smaller FIFO prefix. Only a minimum one-job delivery
            # receives a short-lived larger DuckDB engine.
            close_plan_engines(engine_cache)
            shrink = shrink_unprepared_delivery(con,delivery)
            if shrink.get("shrunk"):
                if (
                        shrink.get("mode") == "drop_secondary_lanes"
                        and shrink.get("kinds") == ["snapshot"]):
                    cap = snapshot_transform_oom_backoff(
                        cfg,runtime,table,
                        shrink["bytes_before"],shrink["bytes_after"])
                else:
                    cap = snapshot_transform_bytes_cap(cfg,runtime,table)
                log(
                    f"DUCKDB OOM RETRY table={table} delivery={delivery} "
                    f"mode={shrink['mode']} jobs={shrink['jobs_before']}->{shrink['jobs_after']} "
                    f"rows={shrink['rows_before']}->{shrink['rows_after']} "
                    f"bytes={shrink['bytes_before']}->{shrink['bytes_after']} "
                    f"snapshot_logical_bytes_cap={cap} journal_retained=1")
                wake_loaders(runtime,table)
                return False

            # Only a minimum one-job transform needs aggregate-memory headroom
            # for the bounded larger recovery engine. Bundled OOMs are solved by
            # shrinking the local delivery and do not sacrifice writer concurrency.
            writer_pressure(
                runtime,table,cfg,
                "minimum DuckDB transform OOM; reserving recovery headroom",
                severe=True)
            base_bytes = memory_limit_bytes(cfg["duckdb_memory"])
            recovery_bytes = duckdb_oom_recovery_memory_bytes(cfg)
            if recovery_bytes <= base_bytes:
                raise RuntimeError(
                    "minimum durable delivery exceeds DuckDB working memory and "
                    "resource budget cannot safely enlarge the recovery engine; "
                    f"delivery={delivery} memory={cfg['duckdb_memory']} "
                    "journal retained") from exc

            if stop.wait(0.2):
                return False
            recovery_cfg = dict(cfg)
            recovery_mb = max(
                1,(recovery_bytes + 1024**2 - 1)//1024**2)
            recovery_cfg["duckdb_memory"] = f"{recovery_mb}MB"
            recovery_engine=transform_engine_for_version(
                runtime,recovery_cfg,version,table)
            log(
                f"DUCKDB OOM ESCALATE table={table} delivery={delivery} "
                f"jobs=1 memory={cfg['duckdb_memory']}->{recovery_cfg['duckdb_memory']} "
                f"active_writers={writer_target(runtime,table)} journal_retained=1")
            try:
                try:
                    prepared = prepare_delivery(
                        con,recovery_engine,mapping,delivery,recovery_cfg)
                except Exception as recovery_exc:
                    if is_duckdb_oom(recovery_exc):
                        raise RuntimeError(
                            "minimum durable delivery still exceeds bounded DuckDB "
                            f"recovery memory delivery={delivery} "
                            f"memory={recovery_cfg['duckdb_memory']}; journal retained"
                        ) from recovery_exc
                    raise
            finally:
                with contextlib.suppress(Exception):
                    recovery_engine.close()
            if prepared:
                log(
                    f"DUCKDB OOM RECOVERED table={table} delivery={delivery} "
                    f"memory={recovery_cfg['duckdb_memory']} "
                    f"active_writers={writer_target(runtime,table)}")
        if not prepared:
            return False
        kinds,input_rows,source_time = con.execute("""
            SELECT GROUP_CONCAT(DISTINCT j.kind),COALESCE(SUM(j.nrows),0),
                   MIN(CASE WHEN j.kind='cdc' THEN j.source_time END)
            FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
            WHERE a.delivery_id=?
        """,(delivery,)).fetchone()
        parts = con.execute("""
            SELECT part,nrows,length(payload),json_bytes
            FROM load_parts WHERE delivery_id=? ORDER BY part
        """,(delivery,)).fetchall()
        merge_async_delivery(handle,con,mapping,delivery,cfg,runtime)
        remote_txns = con.execute("""
            SELECT COUNT(DISTINCT txn_id) FROM load_parts WHERE delivery_id=? AND txn_id IS NOT NULL
        """,(delivery,)).fetchone()[0]
        age = max(0,time.time()-source_time) if source_time else 0
        acknowledge_delivery(con,delivery)
        prune_plan_engines(engine_cache,con,runtime)
        wake_loaders(runtime,table)
        elapsed = time.monotonic()-started
        byte_count = sum(n for _,_,n,_ in parts)
        json_rows = sum(n for _,n,_,_ in parts)
        json_bytes = sum(n for _,_,_,n in parts)
        avg_json_row_bytes = json_bytes/json_rows if json_rows else 0
        metric_add_visible(runtime,table,kinds,input_rows,byte_count,len(parts),remote_txns,
                           elapsed,age,len(lanes),json_rows,json_bytes)
        if cfg.get("detail_logs",False):
            log(f"VISIBLE table={table} protocol=merge_commit_async lane={lane} bundle_lanes={len(lanes)} "
                f"kind={kinds} input_rows={input_rows} output_rows={json_rows} "
                f"bytes={byte_count} json_bytes={json_bytes} avg_json_row_bytes={avg_json_row_bytes:.1f} "
                f"loads={len(parts)} merge_txns={remote_txns} "
                f"seconds={elapsed:.3f} source_event_age_seconds={age:.3f} "
                f"active_writers={writer_target(runtime,table)}")
        if age > 10:
            log(f"LAG WARNING table={table} source_event_age_seconds={age:.3f} exceeds_10_seconds=1")
        return True
    finally:
        for held in reversed(extra_locks):
            held.release()


def merge_delivery_worker(mapping, worker_id, cfg, runtime):
    """Physical writer pool member. It dynamically serves logical key lanes."""
    con = open_state(cfg["state"])
    engines = {}
    handle = pycurl.Curl()
    stop = runtime["stop"]
    table = mapping_key(mapping)
    wake = runtime["load_events"][table]
    cursor = worker_id
    retired_exit = False
    try:
        while not stop.is_set():
            if runtime_sink_retiring(runtime,table) and sink_durable_drained(
                    con,table):
                retired_exit = True
                break
            if version_recovery_active(runtime,table):
                wake.wait(0.1)
                wake.clear()
                continue
            if merge_table_quarantined(runtime,table):
                if engines:
                    close_plan_engines(engines)
                wake.wait(1)
                wake.clear()
                continue
            if worker_id >= writer_target(runtime,table):
                if engines:
                    close_plan_engines(engines)
                wake.wait(0.1)
                wake.clear()
                continue
            lanes = merge_candidate_lanes(con,table)
            if not lanes:
                wake.wait(0.2)
                wake.clear()
                continue
            worked = False
            if lanes:
                shift = cursor % len(lanes)
                lanes = lanes[shift:]+lanes[:shift]
                cursor += 1
            for lane in lanes:
                lock = runtime["lane_locks"].get((table,lane))
                if lock is None or not lock.acquire(blocking=False):
                    continue
                try:
                    try:
                        if process_merge_lane(
                                con,engines,handle,table,lane,cfg,runtime):
                            worked = True
                            break
                    except (RuntimeError,pycurl.error,pymysql.err.OperationalError) as exc:
                        if not quarantine_merge_table(con,table,runtime,exc):
                            raise
                finally:
                    lock.release()
            if not worked:
                wake.wait(0.2)
                wake.clear()
    finally:
        handle.close()
        close_plan_engines(engines)
        con.close()
        if retired_exit:
            runtime_retiring_worker_done(runtime,table,cfg)


def wake_loaders(runtime, table=None):
    for key,event in runtime.get("load_events",{}).items():
        event_table = key[0] if isinstance(key,tuple) else key
        if table is None or event_table == table:
            event.set()


def claim_table_delivery(con, table, cfg):
    """One owner per table. Drain legacy requests before issuing a new transaction."""
    with state_transaction(con):
        existing = con.execute("SELECT id FROM deliveries WHERE table_name=? ORDER BY rowid LIMIT 1",
                               (table,)).fetchone()
        if existing:
            return existing[0]
        if con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] >= cfg["max_inflight_deliveries"]:
            return None
        if prepared_budget_used(con) >= cfg["max_prepared_bytes"]:
            return None
        selected, count, size, plan_version = [], 0, 0, None
        for job_id,nrows,nbytes,job_version in con.execute("""
            SELECT j.id,j.nrows,j.logical_bytes,j.plan_version
            FROM active_jobs j LEFT JOIN job_assignments a ON a.job_id=j.id
            WHERE j.table_name=? AND a.job_id IS NULL
            ORDER BY j.id LIMIT 4096
        """,(table,)):
            if selected and (
                    int(job_version) != int(plan_version) or
                    count+nrows > cfg["txn_rows"] or size+nbytes > cfg["txn_bytes"]):
                break
            selected.append(job_id)
            plan_version = int(job_version)
            count += nrows
            size += nbytes
        if not selected:
            return None
        delivery = uuid.uuid4().hex
        con.execute(
            "INSERT INTO deliveries(id,table_name,lane,plan_version) VALUES(?,?,-1,?)",
            (delivery,table,int(plan_version)))
        if not prepare_reservation_set_locked(
                con,delivery,prepare_reservation_estimate(cfg,size),cfg):
            con.execute("DELETE FROM deliveries WHERE id=?",(delivery,))
            return None
        con.execute("INSERT INTO load_transactions(delivery_id,label) VALUES(?,?)",
                    (delivery,"cdc_txn_"+delivery))
        assign_jobs(con,delivery,selected)
        return delivery


def sr_http_base(cfg):
    host = cfg["sr"]["host"]
    if ":" in host and not host.startswith("["):
        host = "["+host+"]"
    return f"http://{host}:{cfg['sr']['http_port']}"


def transaction_request(handle, cfg, mapping, label, operation, stop, payload=None):
    headers = dict(db=cfg["sr"]["database"],table=mapping["sr_table"],label=label,
                   timeout=str(cfg["load_timeout"]))
    if operation == "load":
        columns = mapping["_output_columns"] + (["_cdc_seq"] if mapping["_target_sequence"] else []) + ["__op"]
        headers.update({"Expect":"100-continue","Content-Type":"application/json",
                        "format":"json","read_json_by_line":"true","strip_outer_array":"false",
                        "ignore_json_size":"true","strict_mode":"true","max_filter_ratio":"0",
                        "timezone":"+00:00","columns":",".join(sql_name(n,True) for n in columns)})
        if cfg["compression"]:
            headers["compression"] = cfg["compression"]
    url = sr_http_base(cfg)+"/api/transaction/"+operation
    status,result = curl_request(handle,cfg,url,payload if payload is not None else b"",
                                 headers,stop,method="PUT" if operation == "load" else "POST")
    if status in (401,403,404):
        raise ValueError(f"transaction {operation} HTTP {status}: {load_result_text(result)}")
    if not 200 <= status < 300 or str(result.get("Status","")).upper() not in ("OK","SUCCESS"):
        raise RuntimeError(f"transaction {operation} HTTP {status} label={label}: {load_result_text(result)}")
    return result


def transaction_state(handle, cfg, label, stop):
    url = (sr_http_base(cfg)+"/api/"+quote(cfg["sr"]["database"],safe="")+
           "/get_load_state?label="+quote(label,safe=""))
    status,result = curl_request(handle,cfg,url,stop=stop)
    if status in (401,403,404):
        raise ValueError(f"transaction status HTTP {status}: {load_result_text(result)}")
    state = str(result.get("state","")).upper()
    if not 200 <= status < 300 or state not in ("UNKNOWN","PREPARE","PREPARED","COMMITTED","VISIBLE","ABORTED"):
        raise RuntimeError(f"invalid transaction status label={label}: HTTP {status} {load_result_text(result)}")
    return state,str(result.get("reason","") or "")


def transaction_save(con, delivery, phase, txn_id=None, error=None):
    with state_transaction(con):
        con.execute("UPDATE load_transactions SET phase=?,txn_id=COALESCE(?,txn_id) WHERE delivery_id=?",
                    (phase,txn_id,delivery))
        if error is not None:
            con.execute("UPDATE load_transactions SET error=? WHERE delivery_id=?",(error[:4000],delivery))


def pressure_kind(message):
    text = str(message).lower()
    if "too many versions" in text:
        return "version"
    if "too large compaction score" in text:
        return "compaction"
    if "too many running transactions" in text:
        return "transaction"
    return None


def version_pressure(message):
    return pressure_kind(message) is not None


def transaction_recovery_action(phase, state):
    """Never replay a possibly accepted load, or replace an unresolved commit."""
    if state == "UNKNOWN":
        # No load or commit could have been sent in this durable phase. The same
        # label may be retried, but must never be replaced merely due to UNKNOWN.
        if phase == "BEGIN_SENT":
            return "begin"
        raise ValueError("transaction identity missing/expired; automatic replay is unsafe; journal retained")
    if state == "ABORTED":
        return "renew"
    if state == "VISIBLE":
        if phase not in ("PREPARED","COMMIT_SENT"):
            raise ValueError("unexpected external commit; transaction ownership violated")
        return "visible"
    if state == "COMMITTED":
        if phase not in ("PREPARED","COMMIT_SENT"):
            raise ValueError("unexpected committed transaction")
        return "wait"
    if state == "PREPARED" and phase in ("PREPARE_SENT","PREPARED","COMMIT_SENT"):
        return "commit"
    if phase == "COMMIT_SENT":
        raise ValueError("commit state regressed; refuse rollback/replay")
    return "rollback"


def load_transaction(handle, con, mapping, delivery, cfg, runtime):
    """Persist intent before every irreversible operation; resolve by label after failures."""
    stop, table = runtime["stop"], mapping_key(mapping)
    expected = con.execute("SELECT COALESCE(SUM(nrows),0) FROM load_parts WHERE delivery_id=?",
                           (delivery,)).fetchone()[0]
    if not expected:
        return
    if con.execute("SELECT COUNT(*) FROM load_parts WHERE delivery_id=? AND visible=0",
                   (delivery,)).fetchone()[0] == 0:
        # Crash after local VISIBLE, before acknowledging the journal. No remote
        # lookup is needed (the server may already have expired its label).
        return
    network_failures = 0
    while not stop.is_set():
        label,phase,attempt,error,retry_at = con.execute("""
            SELECT label,phase,attempt,error,retry_at FROM load_transactions WHERE delivery_id=?
        """,(delivery,)).fetchone()
        if stop.wait(max(0,retry_at-time.time())):
            break
        try:
            if phase == "NEW":
                transaction_save(con,delivery,"BEGIN_SENT")
                result = transaction_request(handle,cfg,mapping,label,"begin",stop)
                transaction_save(con,delivery,"LOADING",result.get("TxnId"))
                for part,payload in con.execute(
                        "SELECT part,payload FROM load_parts WHERE delivery_id=? ORDER BY part",(delivery,)):
                    # An unknown result causes rollback of the WHOLE transaction.
                    transaction_request(handle,cfg,mapping,label,"load",stop,payload)
                transaction_save(con,delivery,"PREPARE_SENT")
                result = transaction_request(handle,cfg,mapping,label,"prepare",stop)
                if int(result.get("NumberFilteredRows",0)) or int(result.get("NumberUnselectedRows",0)) or \
                   ("NumberLoadedRows" in result and int(result["NumberLoadedRows"]) != expected):
                    transaction_save(con,delivery,"ABORTING",error="invalid row counts: "+load_result_text(result))
                    raise ValueError(f"transaction row counts mismatch label={label}: {load_result_text(result)}")
                transaction_save(con,delivery,"PREPARED",result.get("TxnId"))
                phase = "PREPARED"
            state,reason = transaction_state(handle,cfg,label,stop)
            action = transaction_recovery_action(phase,state)
            if action == "visible":
                with state_transaction(con):
                    con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=?",(delivery,))
                runtime["pressure_until"][table] = 0
                return
            if action == "begin":
                # Re-enter BEGIN_SENT on restart; no data was ever sent by us.
                transaction_request(handle,cfg,mapping,label,"begin",stop)
                transaction_save(con,delivery,"LOADING")
                # Conservatively abort/rebuild instead of guessing the coordinator's state.
                continue
            if action == "commit":
                transaction_save(con,delivery,"COMMIT_SENT")
                transaction_request(handle,cfg,mapping,label,"commit",stop)
                continue
            if action == "rollback":
                transaction_save(con,delivery,"ABORTING")
                transaction_request(handle,cfg,mapping,label,"rollback",stop)
                # A successful rollback response is still verified against FE state.
                continue
            if action == "renew":
                failure = error+" "+reason
                pressure = version_pressure(failure)
                if error.startswith("invalid row counts"):
                    raise ValueError(f"invalid load retained: {error}")
                if not pressure and any(x in failure.lower() for x in
                        ("data quality","parse error","column","filtered","invalid argument")):
                    raise ValueError(f"non-retryable load label={label}: {failure}")
                if not pressure and attempt >= cfg["retry_max"]:
                    raise ValueError(f"transaction retry budget exhausted; journal retained: {failure}")
                delay = min(cfg["pressure_max_seconds"] if pressure else 10,2**min(attempt+1,10))
                if pressure:
                    runtime["pressure_until"][table] = time.time()+delay
                    runtime["table_interval"][table] = min(
                        cfg["pressure_max_seconds"],max(cfg["commit_interval_ms"]/1000,
                                                      runtime["table_interval"].get(table,0)*2,delay))
                with state_transaction(con):
                    con.execute("""UPDATE load_transactions SET label=?,phase='NEW',txn_id=NULL,
                        attempt=attempt+1,error='',retry_at=? WHERE delivery_id=?""",
                        ("cdc_txn_"+uuid.uuid4().hex,time.time()+delay,delivery))
                log(f"TXN BACKOFF table={table} aborted_label={label} pressure={int(pressure)} "
                    f"seconds={delay} reason={failure[:4000]}")
                network_failures = 0
                continue
            stop.wait(0.2)
            network_failures = 0
        except (RuntimeError,pycurl.error,pymysql.err.OperationalError) as exc:
            if stop.is_set():
                break
            network_failures += 1
            # Keep the phase and identity. Next iteration queries the original transaction.
            with state_transaction(con):
                con.execute("UPDATE load_transactions SET error=? WHERE delivery_id=?",
                            (str(exc)[:4000],delivery))
            log(f"TXN RESOLVE table={table} label={label} error={exc}")
            if network_failures >= cfg["retry_max"]:
                raise RuntimeError(f"transaction remains unresolved label={label}; journal retained") from exc
            stop.wait(min(10,0.5*2**min(network_failures,5)))
    raise RuntimeError("stopped with durable pending transaction")


def table_delivery_worker(mapping, cfg, runtime):
    table,stop = mapping_key(mapping),runtime["stop"]
    wake = runtime["load_events"][(table,0)]
    con = open_state(cfg["state"])
    engines,handle = {},pycurl.Curl()
    next_send = time.monotonic()
    retired_exit = False
    try:
        while not stop.is_set():
            if runtime_sink_retiring(runtime,table) and sink_durable_drained(
                    con,table):
                retired_exit = True
                break
            if merge_table_quarantined(runtime,table):
                wake.wait(1)
                wake.clear()
                continue
            existing = con.execute("SELECT id FROM deliveries WHERE table_name=? LIMIT 1",(table,)).fetchone()
            pending = con.execute("SELECT MIN(created) FROM active_jobs WHERE table_name=?",(table,)).fetchone()[0]
            if pending is None and not existing:
                wake.wait(1)
                wake.clear()
                continue
            delay = max(0,next_send-time.monotonic(),
                        cfg["batch_ms"]/1000-(time.time()-pending) if pending is not None and not existing else 0)
            if stop.wait(delay):
                break
            delivery = claim_table_delivery(con,table,cfg)
            if not delivery:
                stop.wait(0.1)
                continue
            started = time.monotonic()
            version = delivery_plan_version(con,delivery)
            mapping = runtime_mapping(runtime,version,table)
            engine = plan_engine(engines,runtime,cfg,version,table)
            if not prepare_delivery(con,engine,mapping,delivery,cfg):
                wake.wait(0.2)
                wake.clear()
                continue
            transactional = con.execute("SELECT 1 FROM load_transactions WHERE delivery_id=?",(delivery,)).fetchone()
            if transactional:
                load_transaction(handle,con,mapping,delivery,cfg,runtime)
            else:
                # Old labels/bytes may already be committed; finish them through the old protocol.
                for (part,) in con.execute("SELECT part FROM load_parts WHERE delivery_id=? ORDER BY part",
                                           (delivery,)).fetchall():
                    stream_load(handle,con,mapping,delivery,part,cfg,stop)
            kinds,nrows,source_time = con.execute("""
                SELECT GROUP_CONCAT(DISTINCT j.kind),SUM(j.nrows),
                       MIN(CASE WHEN j.kind='cdc' THEN j.source_time END)
                FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id
                WHERE a.delivery_id=?
            """,(delivery,)).fetchone()
            rows,parts,json_bytes = con.execute("""
                SELECT COALESCE(SUM(nrows),0),COUNT(*),COALESCE(SUM(json_bytes),0)
                FROM load_parts WHERE delivery_id=?
            """,(delivery,)).fetchone()
            identity = con.execute("SELECT label,txn_id FROM load_transactions WHERE delivery_id=?",
                                   (delivery,)).fetchone()
            age = max(0,time.time()-source_time) if source_time else 0
            acknowledge_delivery(con,delivery)
            prune_plan_engines(engines,con,runtime)
            elapsed = time.monotonic()-started
            # Rate is shared by the whole table, including snapshot and CDC.
            interval = runtime["table_interval"].get(table,cfg["commit_interval_ms"]/1000)
            next_send = time.monotonic()+interval
            runtime["table_interval"][table] = max(cfg["commit_interval_ms"]/1000,interval*0.9)
            if elapsed > cfg["freshness_seconds"] or age > cfg["freshness_seconds"]:
                runtime["pressure_until"][table] = time.time()+cfg["commit_interval_ms"]/1000
            avg_json_row_bytes = json_bytes/rows if rows else 0
            metric_add_visible(runtime,table,kinds,nrows,0,parts,1 if identity else 0,
                               elapsed,age,1,rows,json_bytes)
            if cfg.get("detail_logs",False):
                log(f"VISIBLE table={table} protocol={'transaction' if transactional else 'legacy'} "
                    f"kind={kinds} input_rows={nrows} output_rows={rows} "
                    f"json_bytes={json_bytes} avg_json_row_bytes={avg_json_row_bytes:.1f} loads={parts} "
                    f"seconds={elapsed:.3f} source_event_age_seconds={age:.3f} "
                    f"txn={identity} next_interval_seconds={interval:.3f}")
            if age > 10:
                log(f"LAG WARNING table={table} source_event_age_seconds={age:.3f} exceeds_10_seconds=1")
            wake_loaders(runtime)
    finally:
        handle.close()
        close_plan_engines(engines)
        con.close()
        if retired_exit:
            runtime_retiring_worker_done(runtime,table,cfg)




NATIVE_FRAME_CONFIG = b"C"
NATIVE_FRAME_EVENT = b"E"
NATIVE_FRAME_GROUP = b"G"
NATIVE_FRAME_SNAPSHOT = b"S"
NATIVE_FRAME_QUIT = b"Q"
NATIVE_FRAME_ACK = b"A"
NATIVE_FRAME_BATCH = b"B"
NATIVE_FRAME_ERROR = b"X"
NATIVE_MAX_FRAME_BYTES = 256*1024*1024


class NativeTransportError(RuntimeError):
    pass


class NativeDecoderError(RuntimeError):
    pass


class NativeProtocolError(RuntimeError):
    pass


def native_write_exact(proc, data, timeout):
    view = memoryview(data)
    fd = proc.stdin.fileno()
    deadline = time.monotonic()+timeout
    while view:
        code = proc.poll()
        if code is not None:
            raise NativeTransportError(f"native binlog decoder exited during write rc={code}")
        left = deadline-time.monotonic()
        if left <= 0:
            raise NativeTransportError("native binlog decoder write timeout")
        try:
            _,ready,_ = select.select([],[fd],[],left)
        except (OSError,ValueError) as exc:
            raise NativeTransportError(f"native binlog decoder write select failed: {exc}") from exc
        if not ready:
            raise NativeTransportError("native binlog decoder write timeout")
        try:
            written = os.write(fd,view)
        except InterruptedError:
            continue
        except BlockingIOError:
            continue
        except (BrokenPipeError,OSError) as exc:
            raise NativeTransportError(
                f"native binlog decoder write failed rc={proc.poll()}: {exc}") from exc
        if written <= 0:
            raise NativeTransportError("native binlog decoder write returned zero bytes")
        view = view[written:]


def native_write_frame(proc, kind, payload=b"", timeout=30):
    if len(payload) > NATIVE_MAX_FRAME_BYTES:
        raise NativeProtocolError(
            f"native frame exceeds {NATIVE_MAX_FRAME_BYTES} bytes: {len(payload)}")
    native_write_exact(proc,struct.pack("<cI",kind,len(payload)),timeout)
    if payload:
        native_write_exact(proc,payload,timeout)


def native_writev_frame(proc, kind, parts, payload_size, timeout=30):
    if payload_size < 0 or sum(len(part) for part in parts) != payload_size:
        raise NativeProtocolError("native vectored frame length does not match payload")
    if payload_size > NATIVE_MAX_FRAME_BYTES:
        raise NativeProtocolError(
            f"native frame exceeds {NATIVE_MAX_FRAME_BYTES} bytes: {payload_size}")
    iov = [struct.pack("<cI",kind,payload_size)]
    iov.extend(part for part in parts if len(part))
    if not hasattr(os,"writev"):
        for part in iov:
            native_write_exact(proc,part,timeout)
        return

    fd = proc.stdin.fileno()
    deadline = time.monotonic()+timeout
    index,offset = 0,0
    # Linux IOV_MAX is normally 1024; keep batches conservative and portable.
    max_iov = 128
    while index < len(iov):
        code = proc.poll()
        if code is not None:
            raise NativeTransportError(f"native binlog decoder exited during write rc={code}")
        left = deadline-time.monotonic()
        if left <= 0:
            raise NativeTransportError("native binlog decoder write timeout")
        try:
            _,ready,_ = select.select([],[fd],[],left)
        except (OSError,ValueError) as exc:
            raise NativeTransportError(f"native binlog decoder write select failed: {exc}") from exc
        if not ready:
            raise NativeTransportError("native binlog decoder write timeout")

        batch = iov[index:min(len(iov),index+max_iov)]
        if offset:
            batch[0] = memoryview(batch[0])[offset:]
        try:
            written = os.writev(fd,batch)
        except InterruptedError:
            continue
        except BlockingIOError:
            continue
        except (BrokenPipeError,OSError) as exc:
            raise NativeTransportError(
                f"native binlog decoder writev failed rc={proc.poll()}: {exc}") from exc
        if written <= 0:
            raise NativeTransportError("native binlog decoder writev returned zero bytes")

        remaining = written
        while remaining and index < len(iov):
            available = len(iov[index])-offset
            if remaining < available:
                offset += remaining
                remaining = 0
            else:
                remaining -= available
                index += 1
                offset = 0


def native_read_exact(proc, size, timeout):
    chunks = []
    remaining = size
    deadline = time.monotonic()+timeout
    while remaining:
        left = deadline-time.monotonic()
        if left <= 0:
            raise NativeTransportError("native binlog decoder response timeout")
        try:
            ready,_,_ = select.select([proc.stdout.fileno()],[],[],left)
        except (OSError,ValueError) as exc:
            raise NativeTransportError(f"native binlog decoder read select failed: {exc}") from exc
        if not ready:
            raise NativeTransportError("native binlog decoder response timeout")
        try:
            data = proc.stdout.read(remaining)
        except InterruptedError:
            continue
        except (BrokenPipeError,OSError) as exc:
            raise NativeTransportError(
                f"native binlog decoder read failed rc={proc.poll()}: {exc}") from exc
        if not data:
            code = proc.poll()
            raise NativeTransportError(f"native binlog decoder exited unexpectedly rc={code}")
        chunks.append(data)
        remaining -= len(data)
    return b"".join(chunks)


def native_read_frame(proc, timeout):
    header = native_read_exact(proc,5,timeout)
    kind,length = struct.unpack("<cI",header)
    if length > NATIVE_MAX_FRAME_BYTES:
        raise NativeProtocolError(
            f"native decoder frame exceeds {NATIVE_MAX_FRAME_BYTES} bytes: {length}")
    return kind,native_read_exact(proc,length,timeout) if length else b""


def native_expect_ack(proc, timeout):
    kind,payload = native_read_frame(proc,timeout)
    if kind == NATIVE_FRAME_ACK:
        return
    if kind == NATIVE_FRAME_ERROR:
        raise NativeDecoderError("native binlog decoder: "+payload.decode("utf-8","replace"))
    raise NativeProtocolError(f"native binlog decoder returned unexpected frame {kind!r}")


def native_start(cfg, prepared):
    path = cfg["native_binlog_path"]
    proc = subprocess.Popen(
        [path,"--stdio"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=sys.stderr,
        bufsize=0,close_fds=True)
    try:
        os.set_blocking(proc.stdin.fileno(),False)
        native_write_frame(
            proc,NATIVE_FRAME_CONFIG,native_config_payload(cfg,prepared),cfg["query_timeout"])
        native_expect_ack(proc,cfg["query_timeout"])
        return proc
    except BaseException:
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        raise


def native_reset(proc, cfg, prepared):
    native_write_frame(
        proc,NATIVE_FRAME_CONFIG,native_config_payload(cfg,prepared),cfg["query_timeout"])
    native_expect_ack(proc,cfg["query_timeout"])


def native_stop(proc):
    if proc is None:
        return
    try:
        if proc.poll() is None:
            native_write_frame(proc,NATIVE_FRAME_QUIT,timeout=1)
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
    except (NativeTransportError,NativeProtocolError,BrokenPipeError,OSError):
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def native_batch_payload(payload):
    if len(payload) < 4:
        raise NativeProtocolError("native binlog decoder returned truncated batch")
    db_len = struct.unpack_from("<H",payload,0)[0]
    pos = 2
    if pos+db_len+2 > len(payload):
        raise NativeProtocolError("native binlog decoder returned truncated database name")
    database = payload[pos:pos+db_len].decode("utf-8")
    pos += db_len
    table_len = struct.unpack_from("<H",payload,pos)[0]
    pos += 2
    if pos+table_len >= len(payload):
        raise NativeProtocolError("native binlog decoder returned truncated table batch")
    table = payload[pos:pos+table_len].decode("utf-8")
    pos += table_len
    try:
        batch = pa.ipc.open_stream(pa.BufferReader(payload[pos:])).read_all()
    except Exception as exc:
        raise NativeProtocolError(f"native binlog decoder returned invalid Arrow IPC: {exc}") from exc
    return database,table,batch


def native_decode_event(proc, event, timeout):
    native_write_frame(proc,NATIVE_FRAME_EVENT,event,timeout)
    kind,payload = native_read_frame(proc,timeout)
    if kind == NATIVE_FRAME_ACK:
        return None
    if kind == NATIVE_FRAME_ERROR:
        raise NativeDecoderError("native binlog decoder: "+payload.decode("utf-8","replace"))
    if kind != NATIVE_FRAME_BATCH:
        raise NativeProtocolError(f"native binlog decoder returned invalid frame {kind!r}")
    return native_batch_payload(payload)


def native_decode_events(proc, events, timeout):
    """Consume the final ACK before committing any grouped source transaction."""
    if not events or len(events) > 4096:
        raise NativeProtocolError("native event group requires 1..4096 events")
    parts = [struct.pack("<I",len(events))]
    for event in events:
        parts.extend((struct.pack("<I",len(event)),event))
    native_writev_frame(proc,NATIVE_FRAME_GROUP,parts,sum(len(part) for part in parts),timeout)
    while True:
        kind,payload = native_read_frame(proc,timeout)
        if kind == NATIVE_FRAME_ACK:
            if payload:
                raise NativeProtocolError("native group ACK contains an unexpected payload")
            return
        if kind == NATIVE_FRAME_ERROR:
            raise NativeDecoderError("native group decoder: "+payload.decode("utf-8","replace"))
        if kind != NATIVE_FRAME_BATCH:
            raise NativeProtocolError(f"native group decoder returned invalid frame {kind!r}")
        yield native_batch_payload(payload)


def native_snapshot_decode(proc, cfg, mapping, packets, order_base):
    database = cfg["mysql"]["database"].encode("utf-8")
    table = mapping["src_table"].encode("utf-8")
    if len(database) > 65535 or len(table) > 65535:
        raise NativeProtocolError("native snapshot database/table name exceeds protocol limit")
    parts = [
        struct.pack("<H",len(database)),database,
        struct.pack("<H",len(table)),table,
        struct.pack("<QI",int(order_base),len(packets)),
    ]
    payload_size = sum(len(part) for part in parts)
    for packet in packets:
        if len(packet) > 0xffffffff:
            raise NativeProtocolError("native snapshot row packet exceeds protocol limit")
        parts.append(struct.pack("<I",len(packet)))
        parts.append(packet)
        payload_size += 4+len(packet)
    native_writev_frame(
        proc,NATIVE_FRAME_SNAPSHOT,parts,payload_size,cfg["query_timeout"])
    kind,result = native_read_frame(proc,cfg["query_timeout"])
    if kind == NATIVE_FRAME_ERROR:
        raise NativeDecoderError("native snapshot decoder: "+result.decode("utf-8","replace"))
    if kind != NATIVE_FRAME_BATCH:
        raise NativeProtocolError(f"native snapshot decoder returned invalid frame {kind!r}")
    database_out,table_out,batch = native_batch_payload(result)
    if database_out != cfg["mysql"]["database"] or table_out != mapping["src_table"]:
        raise NativeProtocolError(
            f"native snapshot decoder emitted unexpected table {database_out}.{table_out}")
    expected = [name for name,_ in mapping["_schema"]]+["_sync_op","_sync_order"]
    if batch.column_names != expected:
        raise NativeProtocolError(
            f"{mapping['src_table']}: native snapshot Arrow schema columns differ from checked source schema")
    return batch

def native_raw_event(packet, use_checksum):
    data = packet._data
    if len(data) < 20 or data[0] != 0:
        raise RuntimeError("invalid MySQL replication packet")
    event_size = struct.unpack_from("<I",data,10)[0]
    if event_size < 19 or len(data) != event_size+1:
        raise RuntimeError(
            f"invalid binlog event size declared={event_size} packet={len(data)-1}")
    event = memoryview(data)[1:]
    if use_checksum:
        if len(event) < 23:
            raise RuntimeError("checksummed binlog event is too short")
        expected = struct.unpack_from("<I",event,len(event)-4)[0]
        actual = zlib.crc32(event[:-4]) & 0xffffffff
        if actual != expected:
            raise RuntimeError("binlog checksum failed")
        event = event[:-4]
    return event


def native_event_header(event):
    if len(event) < 19:
        raise RuntimeError("truncated binlog event header")
    return struct.unpack_from("<IBIIIH",event,0)


def native_query_text(event):
    body = event[19:]
    if len(body) < 13:
        raise RuntimeError("truncated QUERY_EVENT")
    schema_len = body[8]
    status_len = struct.unpack_from("<H",body,11)[0]
    start = 13+status_len+schema_len+1
    if start > len(body):
        raise RuntimeError("malformed QUERY_EVENT")
    return bytes(body[start:]).decode("utf-8","replace").strip()


def native_gtid_text(event):
    body = event[19:]
    if len(body) < 25:
        raise RuntimeError("truncated GTID_EVENT")
    sid = str(uuid.UUID(bytes=bytes(body[1:17])))
    gno = struct.unpack_from("<Q",body,17)[0]
    return f"{sid}:{gno}"


BINLOG_QUERY_EVENT = 0x02
BINLOG_STOP_EVENT = 0x03
BINLOG_ROTATE_EVENT = 0x04
BINLOG_FORMAT_DESCRIPTION_EVENT = 0x0f
BINLOG_XID_EVENT = 0x10
BINLOG_TABLE_MAP_EVENT = 0x13
BINLOG_WRITE_ROWS_EVENT_V1 = 0x17
BINLOG_UPDATE_ROWS_EVENT_V1 = 0x18
BINLOG_DELETE_ROWS_EVENT_V1 = 0x19
BINLOG_HEARTBEAT_LOG_EVENT = 0x1b
BINLOG_ROWS_QUERY_LOG_EVENT = 0x1d
BINLOG_WRITE_ROWS_EVENT_V2 = 0x1e
BINLOG_UPDATE_ROWS_EVENT_V2 = 0x1f
BINLOG_DELETE_ROWS_EVENT_V2 = 0x20
BINLOG_GTID_LOG_EVENT = 0x21
BINLOG_ANONYMOUS_GTID_LOG_EVENT = 0x22
BINLOG_PREVIOUS_GTIDS_LOG_EVENT = 0x23
BINLOG_XA_PREPARE_EVENT = 0x26
BINLOG_PARTIAL_UPDATE_ROWS_EVENT = 0x27

MYSQL_COM_BINLOG_DUMP = 0x12
MYSQL_COM_BINLOG_DUMP_GTID = 0x1e


def replication_open_stream(cfg, start, durable_gtid):
    options = dict(cfg["mysql"])
    options.pop("http_port",None)
    options.update(
        charset="utf8mb4",autocommit=True,connect_timeout=10,
        read_timeout=max(10,cfg["query_timeout"]),
        write_timeout=cfg["query_timeout"])
    con = pymysql.connect(**options)
    try:
        with con.cursor() as cur:
            cur.execute("SET time_zone = '+00:00'")
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
            checksum_row = cur.fetchone()
            checksum = bool(
                checksum_row and len(checksum_row) > 1 and
                str(checksum_row[1]).upper() == "CRC32")
            if checksum:
                cur.execute("SET @master_binlog_checksum=@@global.binlog_checksum")
            cur.execute("SET @master_heartbeat_period=%s",(1_000_000_000,))

        if durable_gtid is not None:
            encoded = gtid_encode_set(durable_gtid)
            payload = (
                struct.pack("<HIIQ",0,int(cfg["server_id"]),0,4)
                +struct.pack("<I",len(encoded))+encoded
            )
            con._execute_command(MYSQL_COM_BINLOG_DUMP_GTID,payload)
        else:
            log_file = str(start[0]).encode("utf-8")
            payload = struct.pack(
                "<IHI",int(start[1]),0,int(cfg["server_id"]))+log_file
            con._execute_command(MYSQL_COM_BINLOG_DUMP,payload)
        return dict(
            con=con,log_file=str(start[0]),log_pos=int(start[1]),
            use_checksum=checksum)
    except BaseException:
        with contextlib.suppress(Exception):
            con.close()
        raise


def replication_read_packet(stream):
    return stream["con"]._read_packet()


def replication_close_stream(stream):
    if stream is not None:
        with contextlib.suppress(Exception):
            stream["con"].close()


def native_advance_position(stream, event, event_type, log_pos):
    if event_type == BINLOG_ROTATE_EVENT:
        body = event[19:]
        if len(body) < 8:
            raise RuntimeError("truncated ROTATE_EVENT")
        stream["log_pos"] = struct.unpack_from("<Q",body,0)[0]
        stream["log_file"] = bytes(body[8:]).decode("utf-8")
    elif log_pos:
        stream["log_pos"] = int(log_pos)
    return str(stream["log_file"]),int(stream["log_pos"])


def runtime_capture_sources(runtime, plan):
    combined=list(plan["source_prepared"])+list(
        runtime.get("stateful_source_mappings",()))
    prepared=source_mappings(combined)
    return prepared,{
        str(mapping["src_table"]):mapping
        for mapping in prepared
    }


def capture_binlog_native(cfg, prepared, runtime):
    con = open_state(cfg["state"])
    route_engine = transform_engine(cfg)
    shared_source_state = bool(cfg.get("shared_source_state",False))
    active_plan = runtime_plan(runtime,runtime_active_version(runtime))
    prepared = active_plan["prepared"]
    source_prepared,capture_by_source = runtime_capture_sources(
        runtime,active_plan)
    by_sink = active_plan["by_table"]
    by_source = active_plan["by_source"]
    stop = runtime["stop"]
    stream = None
    decoder = None
    failures = 0
    decoder_failures = 0
    crash_counts = {}
    row_events = {
        BINLOG_WRITE_ROWS_EVENT_V1,BINLOG_UPDATE_ROWS_EVENT_V1,BINLOG_DELETE_ROWS_EVENT_V1,
        BINLOG_WRITE_ROWS_EVENT_V2,BINLOG_UPDATE_ROWS_EVENT_V2,BINLOG_DELETE_ROWS_EVENT_V2,
        BINLOG_PARTIAL_UPDATE_ROWS_EVENT,
    }
    passive_events = {
        BINLOG_ROTATE_EVENT,BINLOG_HEARTBEAT_LOG_EVENT,BINLOG_FORMAT_DESCRIPTION_EVENT,
        BINLOG_PREVIOUS_GTIDS_LOG_EVENT,BINLOG_STOP_EVENT,BINLOG_ROWS_QUERY_LOG_EVENT,
        BINLOG_TABLE_MAP_EVENT,
    }
    try:
        while not stop.is_set():
            start = meta_get(con,"read_position")
            durable_gtid = meta_get(con,"gtid_set")
            event_type = None
            try:
                active_plan = runtime_plan(runtime,runtime_active_version(runtime))
                prepared = active_plan["prepared"]
                source_prepared,capture_by_source = runtime_capture_sources(
                    runtime,active_plan)
                by_sink = active_plan["by_table"]
                by_source = active_plan["by_source"]
                runtime["reader_state"] = "connecting"
                stream = replication_open_stream(cfg,start,durable_gtid)
                runtime["reader_state"] = "connected"
                use_checksum = stream["use_checksum"]
                decoder = native_start(cfg,source_prepared)
                runtime["stream"] = stream
                in_transaction = False
                current_gtid = None
                with tempfile.SpooledTemporaryFile(
                    max_size=1024**2,dir=state_temp_dir(cfg)) as spool:
                    transaction_batches = transaction_batch_new()
                    source_parts = []
                    source_parts_bytes = 0
                    if shared_source_state:
                        resource_mb = int(
                            (cfg.get("resource") or {}).get("memory_mb",512))
                        source_parts_limit = max(
                            64*1024**2,
                            min(
                                int(cfg["txn_spool_max_bytes"]),
                                resource_mb*1024**2//4))
                    else:
                        source_parts_limit = 0
                    spool_guard_state = dict(size=0,checked=0.0)
                    pending_events = []
                    pending_event_bytes = 0
                    group_limit = max(1,min(4096,int(cfg.get("native_event_group_events",1))))
                    group_bytes = min(4*1024*1024,int(cfg["batch_bytes"]))

                    def stage_native_result(result):
                        nonlocal source_parts_bytes
                        if result is None:
                            return
                        database,table,batch = result
                        if (
                            database != cfg["mysql"]["database"]
                            or table not in capture_by_source
                        ):
                            raise RuntimeError(
                                f"native decoder emitted unexpected table {database}.{table}")
                        source_mapping=capture_by_source[table]
                        fanout=by_source.get(table,())
                        expected=[
                            name for name,_ in source_mapping["_schema"]
                        ]+["_sync_op","_sync_order"]
                        if batch.column_names != expected:
                            raise RuntimeError(
                                f"{table}: native Arrow schema differs from checked source schema")
                        if shared_source_state:
                            part = source_state.prepare_part(
                                source_relation_key(cfg,table),batch)
                            source_parts_bytes += len(part["payload"])
                            if source_parts_bytes > source_parts_limit:
                                raise RuntimeError(
                                    "shared source-state transaction exceeds bounded "
                                    f"in-memory log budget bytes={source_parts_bytes} "
                                    f"limit={source_parts_limit}; disk-spooled source "
                                    "parts are not implemented yet")
                            source_parts.append(part)
                        for mapping in fanout:
                            transaction_batch_add(transaction_batches,mapping,batch,cfg,route_engine,spool)

                    def flush_native_events():
                        nonlocal pending_event_bytes
                        if not pending_events:
                            return
                        for result in native_decode_events(decoder,pending_events,cfg["query_timeout"]):
                            stage_native_result(result)
                            transaction_spool_guard(spool,cfg,spool_guard_state)
                        pending_events.clear()
                        pending_event_bytes = 0

                    def append_native_event(event):
                        nonlocal pending_event_bytes
                        if pending_events and pending_event_bytes+len(event)+4 > group_bytes:
                            flush_native_events()
                        pending_events.append(event)
                        pending_event_bytes += len(event)+4
                        if len(pending_events) >= group_limit or pending_event_bytes >= group_bytes:
                            flush_native_events()

                    while not stop.is_set():
                        packet = replication_read_packet(stream)
                        if packet.is_eof_packet():
                            raise pymysql.err.OperationalError(2013,"binlog stream ended")
                        if not packet.is_ok_packet():
                            raise RuntimeError("unexpected non-OK MySQL replication packet")
                        event = native_raw_event(packet,use_checksum)
                        timestamp,event_type,_,_,log_pos,_ = native_event_header(event)
                        position = native_advance_position(stream,event,event_type,log_pos)
                        source_time = float(timestamp or time.time())
                        runtime["reader_ready"].set()
                        runtime["source_seen"] = time.time()

                        if event_type == BINLOG_HEARTBEAT_LOG_EVENT:
                            runtime["heartbeat_count"] += 1
                            if not in_transaction and runtime.get("pending_plan") is not None:
                                activated = activate_pending_plan(
                                    con,decoder,cfg,runtime,position)
                                if activated is not None:
                                    prepared = activated["prepared"]
                                    source_prepared,capture_by_source = runtime_capture_sources(
                                        runtime,activated)
                                    by_sink = activated["by_table"]
                                    by_source = activated["by_source"]

                        if event_type == BINLOG_TABLE_MAP_EVENT:
                            if group_limit == 1:
                                native_decode_event(decoder,event,cfg["query_timeout"])
                            else:
                                append_native_event(event)
                        elif event_type in row_events:
                            if event_type == BINLOG_PARTIAL_UPDATE_ROWS_EVENT:
                                raise RuntimeError("partial JSON updates are unsupported")
                            runtime["source_data_seen"] = time.time()
                            in_transaction = True
                            if group_limit == 1:
                                stage_native_result(native_decode_event(decoder,event,cfg["query_timeout"]))
                            else:
                                append_native_event(event)
                            transaction_spool_guard(
                                spool,cfg,spool_guard_state)
                            continue
                        elif event_type == BINLOG_XA_PREPARE_EVENT:
                            raise RuntimeError("XA transactions are unsupported in phase 1")
                        elif event_type == BINLOG_GTID_LOG_EVENT:
                            if in_transaction:
                                raise RuntimeError("new transaction before previous commit")
                            in_transaction = True
                            current_gtid = native_gtid_text(event)
                            continue
                        elif event_type == BINLOG_ANONYMOUS_GTID_LOG_EVENT:
                            if cfg["gtid_enabled"]:
                                raise RuntimeError(
                                    "anonymous GTID event received while gtid_mode=ON")
                            in_transaction = True
                            continue
                        elif event_type == BINLOG_QUERY_EVENT:
                            query = native_query_text(event)
                            keyword = re.match(r"([A-Za-z]+)",query)
                            keyword = keyword.group(1).upper() if keyword else ""
                            if keyword == "BEGIN":
                                in_transaction = True
                                continue
                            if keyword not in ("COMMIT","ROLLBACK","SET","SAVEPOINT","RELEASE"):
                                raise RuntimeError(
                                    f"unsupported statement/DDL in binlog: {query[:200]}")
                            if keyword in ("SET","SAVEPOINT","RELEASE"):
                                continue
                            if keyword == "ROLLBACK":
                                pending_events.clear()
                                pending_event_bytes = 0
                                transaction_batch_clear(transaction_batches)
                                source_parts.clear()
                                source_parts_bytes = 0
                                spool.seek(0)
                                spool.truncate()
                                spool_guard_state.update(size=0,checked=time.monotonic())
                        elif event_type not in passive_events and event_type != BINLOG_XID_EVENT:
                            raise RuntimeError(f"unsupported binlog event type {event_type}")

                        if event_type == BINLOG_ROTATE_EVENT:
                            flush_native_events()
                            native_reset(decoder,cfg,source_prepared)

                        if event_type == BINLOG_XID_EVENT or event_type == BINLOG_QUERY_EVENT:
                            if event_type == BINLOG_XID_EVENT or keyword == "COMMIT":
                                flush_native_events()
                                transaction_batch_flush_all(
                                    transaction_batches,cfg,route_engine,spool)
                                transaction_spool_guard(
                                    spool,cfg,spool_guard_state,force=True)
                            transaction_plan_version = runtime_active_version(runtime)
                            changed_tables = commit_spool(
                                con,spool,position,source_time,by_sink,
                                current_gtid if durable_gtid is not None else None,
                                plan_version=transaction_plan_version,
                                source_parts=source_parts if shared_source_state else None,
                                source_epoch=runtime.get("source_uuid"))
                            if shared_source_state:
                                source_state.apply_pending(con)
                            if changed_tables:
                                runtime["cdc_transactions"] += 1
                                for table in changed_tables:
                                    wake_loaders(runtime,table)
                            spool.seek(0)
                            spool.truncate()
                            source_parts.clear()
                            source_parts_bytes = 0
                            spool_guard_state.update(size=0,checked=time.monotonic())
                            in_transaction = False
                            current_gtid = None
                            failures = 0
                            decoder_failures = 0
                            runtime["reader_retries"] = 0
                            crash_counts.clear()
                            if runtime.get("pending_plan") is not None:
                                activated = activate_pending_plan(
                                    con,decoder,cfg,runtime,position)
                                if activated is not None:
                                    prepared = activated["prepared"]
                                    source_prepared,capture_by_source = runtime_capture_sources(
                                        runtime,activated)
                                    by_sink = activated["by_table"]
                                    by_source = activated["by_source"]
                            while meta_get(con,"pending_bytes",0) >= cfg["max_backlog_bytes"] and not stop.is_set():
                                stop.wait(0.05)
                        elif not in_transaction:
                            with state_transaction(con):
                                cursor_advance(con,position)
                            failures = 0
                            decoder_failures = 0
                            runtime["reader_retries"] = 0
                            crash_counts.clear()

                        journal_disk_guard(cfg)
            except (pymysql.err.OperationalError,pymysql.err.InterfaceError,NativeTransportError) as exc:
                if stop.is_set():
                    break
                mysql_transport = isinstance(exc,(pymysql.err.OperationalError,pymysql.err.InterfaceError))
                if mysql_transport and not retryable_mysql_error(exc):
                    raise RuntimeError(
                        f"binlog access/history unavailable: {exc}; automatic reset is forbidden") from exc
                failures += 1
                if not mysql_transport:
                    decoder_failures += 1
                durable = meta_get(con,"read_position")
                child_rc = decoder.poll() if decoder is not None else None
                if isinstance(exc,NativeTransportError) and child_rc is not None:
                    key = (durable[0],int(durable[1]),event_type,int(child_rc))
                    crash_counts[key] = crash_counts.get(key,0)+1
                    if crash_counts[key] >= 3:
                        raise RuntimeError(
                            "native decoder crash loop at durable cursor "
                            f"{durable[0]}:{durable[1]} event_type={event_type} "
                            f"child_rc={child_rc} repeats={crash_counts[key]}") from exc
                if decoder_failures > cfg["retry_max"] and not mysql_transport:
                    raise RuntimeError(
                        "native transport retry budget exhausted at durable cursor "
                        f"{durable[0]}:{durable[1]} event_type={event_type} "
                        f"child_rc={child_rc} failures={failures}: {exc}") from exc
                runtime["reader_state"] = "recovering"
                runtime["reader_retries"] = failures
                # A temporary MySQL outage can exceed the normal retry window.
                # Retain durable state and keep a bounded, cancellable wait;
                # malformed events/decoder loops still fail closed above.
                waiting = mysql_transport and failures > cfg["retry_max"]
                log(
                    "BINLOG native recover "
                    f"cursor={durable[0]}:{durable[1]} event_type={event_type} "
                    f"child_rc={child_rc} attempt={failures} retry_window={cfg['retry_max']} "
                    f"waiting_for_source={int(waiting)} reason={exc}")
                stop.wait(30 if waiting else min(10,0.5*2**min(failures,5)))
            except (NativeDecoderError,NativeProtocolError) as exc:
                durable = meta_get(con,"read_position")
                child_rc = decoder.poll() if decoder is not None else None
                raise RuntimeError(
                    "native fail-closed at durable cursor "
                    f"{durable[0]}:{durable[1]} event_type={event_type} "
                    f"child_rc={child_rc}: {exc}") from exc
            finally:
                if stream is not None:
                    replication_close_stream(stream)
                stream = None
                runtime["stream"] = None
                native_stop(decoder)
                decoder = None
    finally:
        if stream is not None:
            replication_close_stream(stream)
        native_stop(decoder)
        route_engine.close()
        con.close()

def capture_binlog(cfg, prepared, runtime):
    return capture_binlog_native(cfg,prepared,runtime)


def source_state_snapshot_worker(mapping, cfg, runtime):
    con = open_state(cfg["state"])
    source = None
    decoder = None
    stop = runtime["stop"]
    relation = source_relation_key(cfg,mapping)
    count = min(int(cfg["snapshot_rows"]),1024)
    source_index = mapping.get("_source_index") or {
        name:index for index,(name,_) in enumerate(mapping["_schema"])}
    key_indexes = [source_index[name] for name in pk_columns(mapping)]
    try:
        while not stop.is_set():
            info = source_state.relation_info(con,relation)
            if info["complete_seq"] is not None:
                return
            if not runtime["reader_ready"].wait(0.1):
                continue
            if source is None:
                source = mysql_connect(cfg)
            if decoder is None:
                decoder = native_start(cfg,[mapping])
            if not info["snapshot_upper_set"]:
                upper = snapshot_upper(source,mapping)
                source_state.snapshot_set_upper(con,relation,upper)
            else:
                upper = info["snapshot_upper"]
            cursor = info["snapshot_cursor"]

            started = time.monotonic()
            rows,fetch_info = fetch_snapshot(
                source,mapping,cursor,upper,count,cfg,decoder)
            high = binlog_position(source)
            row_count = rows.num_rows if isinstance(rows,pa.Table) else len(rows)
            if row_count and isinstance(rows,pa.Table):
                next_cursor = tuple(
                    rows.column(name)[row_count-1].as_py()
                    for name in pk_columns(mapping))
            else:
                next_cursor = (
                    tuple(rows[-1][index] for index in key_indexes)
                    if row_count else cursor)
            is_last = (
                not fetch_info["budget_limited"]
                and int(fetch_info["source_rows"]) < count)

            while not stop.is_set():
                if not position_ge(meta_get(con,"read_position"),high):
                    stop.wait(0.02)
                    continue
                source_state.apply_pending(con)
                try:
                    source_state.stage_snapshot_batch(
                        con,relation,
                        rows if isinstance(rows,pa.Table) else snapshot_arrow(mapping,rows),
                        next_cursor,is_last=is_last)
                    if is_last:
                        sync_source_base_catalog(con)
                    break
                except RuntimeError as exc:
                    if (
                        is_last
                        and "base apply lags log" in str(exc)
                    ):
                        stop.wait(0.02)
                        continue
                    raise
            if stop.is_set():
                return
            elapsed = time.monotonic()-started
            rate = row_count/elapsed if elapsed else 0
            if cfg.get("detail_logs",False) or is_last:
                state = source_state.status(con)
                log(
                    f"SOURCE MIRROR table={relation} rows={row_count} "
                    f"cursor={next_cursor!r} barrier={high[0]}:{high[1]} "
                    f"last={int(is_last)} rows_per_second={rate:.1f} "
                    f"log_durable={state['log_durable_seq']} "
                    f"base_applied={state['base_applied_seq']}")
            count = snapshot_next_row_limit(
                count,row_count,
                int(fetch_info["retained_bytes"]) if isinstance(rows,pa.Table) else 0,
                elapsed,cfg)
            if is_last:
                return
    finally:
        if source is not None:
            with contextlib.suppress(Exception):
                source.close()
        native_stop(decoder)
        con.close()


def shared_snapshot_worker(mapping, cfg, runtime):
    con = open_state(cfg["state"])
    stop = runtime["stop"]
    table = mapping_key(mapping)
    relation = source_relation_key(cfg,mapping)
    pin = None
    try:
        state = con.execute("""
            SELECT cursor,staged_cursor,staged_done,snapshot_done
            FROM table_state WHERE name=?
        """,(table,)).fetchone()
        if state is None:
            raise RuntimeError("shared snapshot sink lacks durable table_state")
        owner = "sink:%s:plan:%d" % (
            table,int(mapping.get("_plan_version",0)))
        if state[2]:
            generation = task_generation.maybe_info(
                con,table,int(mapping.get("_plan_version",0)))
            if generation is None:
                generation = task_generation.import_existing(
                    con,table,int(mapping.get("_plan_version",0)),relation,
                    "ready" if state[3] else "history_staged")
            if (
                not generation["imported"]
                and not generation["source_pin_released"]
            ):
                task_generation.finalize_history_and_release_pin(
                    con,table,int(mapping.get("_plan_version",0)))
            else:
                row = con.execute(
                    "SELECT pin_id FROM source_pins WHERE owner=?",(owner,)
                ).fetchone()
                if row:
                    source_state.release_pin(con,row[0])
            if state[3]:
                task_generation.mark_ready_if_exists(
                    con,table,int(mapping.get("_plan_version",0)))
            return

        while not stop.is_set():
            info = source_state.relation_info(con,relation)
            if info["complete_seq"] is not None:
                break
            stop.wait(0.05)
        if stop.is_set():
            return

        sync_source_base_catalog(con)
        pin = source_state.acquire_or_resume_pin(con,owner,[relation])
        task_generation.ensure_build(
            con,table,int(mapping.get("_plan_version",0)),relation,
            pin["watermark"],pin["pin_id"])
        cursor_blob = state[1] if state[1] is not None else state[0]
        cursor = unpack(cursor_blob) if cursor_blob is not None else None
        count = min(int(cfg["snapshot_rows"]),4096)

        while not stop.is_set():
            rows,next_cursor = source_state.read_snapshot_batch(
                con,pin["pin_id"],relation,cursor,limit=count)
            is_last = rows.num_rows < count
            if rows.num_rows == 0:
                next_cursor = cursor
                is_last = True
            high = meta_get(con,"read_position")
            while not stop.is_set():
                if stage_snapshot(
                        con,mapping,rows,next_cursor,is_last,high,cfg):
                    wake_loaders(runtime,table)
                    break
                stop.wait(0.02)
            if stop.is_set():
                return
            if cfg.get("detail_logs",False) or is_last:
                log(
                    f"SHARED BACKFILL sink={table} source={relation} "
                    f"fixed_w={pin['watermark']} rows={rows.num_rows} "
                    f"cursor_bytes={0 if next_cursor is None else len(next_cursor)} "
                    f"last={int(is_last)}")
            cursor = next_cursor
            if is_last:
                task_generation.finalize_history_and_release_pin(
                    con,table,int(mapping.get("_plan_version",0)))
                pin = None
                return
    finally:
        # Keep the pin across crashes/cancellation so the same W remains
        # resumable. It is released only after the final source batch is staged.
        con.close()


def snapshot_upper(source, mapping):
    keys = pk_columns(mapping)
    columns = ",".join(sql_name(k,True) for k in keys)
    order = ",".join(sql_name(k,True)+" DESC" for k in keys)
    with source.cursor() as cur:
        cur.execute("SELECT "+columns+" FROM "+sql_name(mapping["src_table"],True)+" ORDER BY "+order+" LIMIT 1")
        row = cur.fetchone()
        return tuple(row) if row else None


def snapshot_select(mapping, cursor, upper, count, cfg):
    keys = pk_columns(mapping)
    lhs = "("+",".join(sql_name(k,True) for k in keys)+")"
    rhs = "("+",".join(["%s"]*len(keys))+")"
    conditions,params = [],[]
    if cursor is not None:
        conditions.append(lhs+">"+rhs)
        params.extend(cursor)
    conditions.append(lhs+"<="+rhs)
    params.extend(upper)
    names = [name for name,_ in mapping["_schema"]]
    select = ",".join(sql_name(k,True) for k in names)
    order = ",".join(sql_name(k,True) for k in keys)
    sql = (f"SELECT /*+ MAX_EXECUTION_TIME({cfg['query_timeout']*1000}) */ {select} FROM "
           +sql_name(mapping["src_table"],True)+" WHERE "+" AND ".join(conditions)
           +" ORDER BY "+order+" LIMIT %s")
    params.append(count)
    return sql,params


def fetch_snapshot_native(source, decoder, mapping, cursor, upper, count, cfg):
    sql,params = snapshot_select(mapping,cursor,upper,count,cfg)
    tables = []
    packets = []
    packet_bytes = 0
    retained_bytes = 0
    source_rows = 0
    budget_limited = False
    order_base = 0
    expected = [name for name,_ in mapping["_schema"]]
    flush_bytes = max(1024*1024,min(int(cfg["batch_bytes"]),NATIVE_MAX_FRAME_BYTES//4))
    byte_budget = int(cfg["snapshot_chunk_bytes"])

    def flush():
        nonlocal packets,packet_bytes,order_base,retained_bytes,budget_limited
        if not packets:
            return
        batch = native_snapshot_decode(decoder,cfg,mapping,packets,order_base)
        if batch.num_rows != len(packets):
            raise NativeProtocolError(
                f"{mapping['src_table']}: native snapshot row count mismatch "
                f"packets={len(packets)} arrow={batch.num_rows}")
        projected = retained_bytes+int(batch.nbytes)
        # Always retain one decoded batch so even one exceptionally wide row can advance.
        if tables and projected > byte_budget:
            budget_limited = True
        else:
            tables.append(batch)
            retained_bytes = projected
            order_base += len(packets)
        packets = []
        packet_bytes = 0

    with source.cursor(pymysql.cursors.SSCursor) as cur:
        cur.execute(sql,params)
        result = getattr(cur,"_result",None)
        connection = getattr(result,"connection",None)
        if result is None or connection is None or not getattr(result,"unbuffered_active",False) or \
           not hasattr(result,"_check_packet_is_eof"):
            raise RuntimeError("PyMySQL unbuffered raw-result API changed")
        actual = [str(item[0]) for item in (result.description or ())]
        if actual != expected:
            raise RuntimeError(
                f"{mapping['src_table']}: snapshot result columns differ from checked schema "
                f"expected={expected!r} actual={actual!r}")
        while True:
            packet = connection._read_packet()
            if result._check_packet_is_eof(packet):
                result.unbuffered_active = False
                result.connection = None
                result.rows = None
                break
            source_rows += 1
            if budget_limited:
                continue
            data = packet.get_all_data()
            projected = packet_bytes+4+len(data)
            if packets and (projected > flush_bytes or len(packets) >= 8192):
                flush()
                if budget_limited:
                    continue
            if len(data)+64 >= NATIVE_MAX_FRAME_BYTES:
                raise NativeProtocolError(
                    f"{mapping['src_table']}: snapshot row packet exceeds native frame limit")
            packets.append(data)
            packet_bytes += 4+len(data)
        if not budget_limited:
            flush()

    table = raw_arrow(mapping,[]) if not tables else (
        tables[0] if len(tables) == 1 else pa.concat_tables(tables))
    return table,dict(
        source_rows=source_rows,budget_limited=budget_limited,
        retained_bytes=retained_bytes,byte_budget=byte_budget)


def fetch_snapshot(source, mapping, cursor, upper, count, cfg, decoder=None):
    if upper is None:
        rows = raw_arrow(mapping,[]) if decoder is not None else []
        return rows,dict(source_rows=0,budget_limited=False,retained_bytes=0,
                         byte_budget=int(cfg.get("snapshot_chunk_bytes",0)))
    if decoder is not None:
        return fetch_snapshot_native(source,decoder,mapping,cursor,upper,count,cfg)
    sql,params = snapshot_select(mapping,cursor,upper,count,cfg)
    with source.cursor(pymysql.cursors.SSCursor) as cur:
        cur.execute(sql,params)
        rows = cur.fetchmany(count)
    return rows,dict(source_rows=len(rows),budget_limited=False,retained_bytes=0,
                     byte_budget=int(cfg.get("snapshot_chunk_bytes",0)))


def snapshot_next_row_limit(current, row_count, retained_bytes, elapsed, cfg):
    time_target = int(
        current*min(2,max(0.25,cfg["freshness_seconds"]/max(elapsed,0.001))))
    if row_count and retained_bytes:
        avg_bytes = max(1,int(retained_bytes)//int(row_count))
        byte_target = max(1,int(cfg["snapshot_chunk_bytes"])//avg_bytes)
    else:
        byte_target = int(cfg["snapshot_rows"])
    return max(
        1,min(int(cfg["snapshot_rows"]),time_target,byte_target))


def snapshot_worker(mapping, cfg, runtime):
    con = open_state(cfg["state"])
    source = None
    snapshot_decoder = None
    route_engine = transform_engine(cfg)
    stop,table = runtime["stop"],mapping_key(mapping)
    # Probe row width conservatively before expanding toward CDC_SNAPSHOT_ROWS.
    count = min(int(cfg["snapshot_rows"]),1024)
    source_index = mapping.get("_source_index") or {
        name:index for index,(name,_) in enumerate(mapping["_schema"])}
    key_indexes = [source_index[name] for name in pk_columns(mapping)]
    try:
        while not stop.is_set():
            state = con.execute("""
                SELECT cursor,upper_key,upper_set,snapshot_done,
                       staged_cursor,staged_done
                FROM table_state WHERE name=?
            """,(table,)).fetchone()
            if state[5]:
                return
            if merge_table_quarantined(runtime,table):
                stop.wait(1)
                continue
            if runtime.get("resource_pause_snapshot",False) or \
               version_recovery_active(runtime,table) or \
               time.time() < runtime["pressure_until"].get(table,0) or \
               meta_get(con,"pending_bytes",0) >= cfg["max_backlog_bytes"]:
                stop.wait(0.2)
                continue
            if int(con.execute(
                    "SELECT COUNT(*) FROM snapshot_groups WHERE table_name=?",
                    (table,)).fetchone()[0]) >= int(
                        cfg.get("snapshot_read_ahead_groups",1)):
                stop.wait(0.02)
                continue
            if not runtime["reader_ready"].wait(0.1):
                continue
            if source is None:
                source = mysql_connect(cfg)
            if snapshot_decoder is None:
                snapshot_decoder = native_start(cfg,[mapping])
            if not state[2]:
                upper = snapshot_upper(source,mapping)
                with state_transaction(con):
                    con.execute("UPDATE table_state SET upper_key=?,upper_set=1 WHERE name=?",(pack(upper),table))
            else:
                upper = unpack(state[1])
            cursor_blob = state[4] if state[4] is not None else state[0]
            cursor = unpack(cursor_blob) if cursor_blob is not None else None
            oldest = con.execute("SELECT MIN(source_time) FROM active_jobs WHERE kind='cdc' AND table_name=?",
                                 (table,)).fetchone()[0]
            if oldest is not None and time.time()-oldest > cfg["freshness_seconds"]:
                stop.wait(0.05)
                continue
            started = time.monotonic()
            read_started = time.monotonic()
            rows,fetch_info = fetch_snapshot(
                source,mapping,cursor,upper,count,cfg,snapshot_decoder)
            read_seconds = time.monotonic()-read_started
            # This barrier is after the SELECT, not merely after its start.
            admit_started = time.monotonic()
            high = binlog_position(source)
            row_count = rows.num_rows if isinstance(rows,pa.Table) else len(rows)
            if row_count and isinstance(rows,pa.Table):
                next_cursor = tuple(
                    rows.column(name)[row_count-1].as_py() for name in pk_columns(mapping))
            else:
                next_cursor = tuple(rows[-1][index] for index in key_indexes) if row_count else cursor
            is_last = (
                not fetch_info["budget_limited"]
                and int(fetch_info["source_rows"]) < count
            )
            while not stop.is_set():
                if stage_snapshot(con,mapping,rows,next_cursor,is_last,high,cfg,route_engine):
                    wake_loaders(runtime,table)
                    break
                stop.wait(0.02)
            admit_seconds = time.monotonic()-admit_started
            elapsed = time.monotonic()-started
            metric_add_snapshot_chunk(
                runtime,table,row_count,read_seconds,admit_seconds,elapsed,
                fetch_info["source_rows"])
            count = snapshot_next_row_limit(
                count,row_count,
                int(fetch_info["retained_bytes"]) if isinstance(rows,pa.Table) else 0,
                elapsed,cfg)
            read_rate = row_count/read_seconds if read_seconds else 0
            chunk_rate = row_count/elapsed if elapsed else 0
            log(f"BACKFILL table={table} read_rows={row_count} cursor={next_cursor!r} "
                f"barrier={high[0]}:{high[1]} last={is_last} "
                f"read_seconds={read_seconds:.3f} admit_seconds={admit_seconds:.3f} "
                f"chunk_seconds={elapsed:.3f} read_rows_per_second={read_rate:.1f} "
                f"chunk_rows_per_second={chunk_rate:.1f} "
                f"source_rows={fetch_info['source_rows']} "
                f"read_amplification={fetch_info['source_rows']/max(1,row_count):.2f} "
                f"retained_bytes={fetch_info['retained_bytes']} "
                f"byte_budget={fetch_info['byte_budget']} "
                f"budget_limited={int(fetch_info['budget_limited'])} next_rows={count}")
    finally:
        if source is not None:
            source.close()
        native_stop(snapshot_decoder)
        route_engine.close()
        con.close()


def catalog_server_runtime_worker(cfg, runtime):
    cdc_catalog.server_worker(
        cfg["catalog"],cfg["catalog_socket"],
        cfg.get("catalog_version",0),runtime["stop"],
        lambda result,phase: catalog_publish_callback(
            cfg,runtime,result,phase),
        lambda: runtime_active_version(runtime))


def guarded_worker(function, runtime, *args):
    failures = 0
    while not runtime["stop"].is_set():
        try:
            function(*args,runtime)
            return
        except Exception as exc:
            if runtime["stop"].is_set():
                return
            # Snapshot reads restart from the durable chunk cursor; unjournaled rows are discarded.
            snapshot_transport = function is snapshot_worker and isinstance(exc,NativeTransportError)
            if function is snapshot_worker and (retryable_mysql_error(exc) or snapshot_transport):
                failures += 1
                if snapshot_transport and failures >= 3:
                    log(f"BACKFILL NATIVE FAIL-STOP table={mapping_key(args[0])} "
                        f"repeats={failures} durable_cursor_retained=1 reason={exc}")
                else:
                    delay = min(30,0.5*2**min(failures,6))
                    log(f"BACKFILL RECONNECT table={mapping_key(args[0])} "
                        f"retry_seconds={delay} durable_cursor_retained=1 reason={exc}")
                    runtime["stop"].wait(delay)
                    continue
            with runtime["error_lock"]:
                runtime["errors"].append((function.__name__,str(exc)))
            log(f"FATAL worker={function.__name__}: {exc}")
            traceback.print_exc(file=sys.stdout)
            runtime["stop"].set()


@contextlib.contextmanager
def process_signals():
    control = dict(stop=threading.Event(),reason="stopped",requested=False)
    previous = {}

    def request_stop(signum, frame):
        control["reason"] = "signal="+signal.Signals(signum).name
        # Do not acquire Event/print locks inside a signal handler interrupting the main thread.
        control["requested"] = True

    try:
        for signum in (signal.SIGINT,signal.SIGTERM):
            previous[signum] = signal.signal(signum,request_stop)
        if hasattr(signal,"SIGHUP"):
            previous[signal.SIGHUP] = signal.signal(signal.SIGHUP,signal.SIG_IGN)
        log("SIGNALS SIGHUP=ignore SIGINT/SIGTERM=graceful_stop")
        yield control
    finally:
        for signum,handler in previous.items():
            signal.signal(signum,handler)


def signal_stop_requested(control):
    if control.get("requested",False):
        control["stop"].set()
    return control["stop"].is_set()


@contextlib.contextmanager
def process_lock(path):
    import fcntl
    handle = open(path+".lock","a+b")
    try:
        try:
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another process owns this state file") from None
        yield
    finally:
        handle.close()


def stateful_retire_frontier(runtime, task_id):
    with runtime["plan_lock"]:
        value=runtime.get(
            "stateful_retire_frontiers",{}).get(str(task_id))
    return None if value is None else int(value)


def stateful_output_frontier(con, kind, consumer_id):
    if str(kind)=="aggregate":
        return int(aggregate_outbox.visible_frontier(
            con,consumer_id))
    if str(kind)=="inner_join":
        return int(join_outbox.visible_frontier(
            con,consumer_id))
    raise ValueError(
        "unsupported stateful task kind: "+str(kind))


def stateful_stage_pending(con, kind, consumer_id, mapping, cfg):
    if str(kind)=="aggregate":
        return aggregate_job_bridge.stage_pending(
            con,consumer_id,mapping,cfg)
    if str(kind)=="inner_join":
        return join_job_bridge.stage_pending(
            con,consumer_id,mapping,cfg)
    raise ValueError(
        "unsupported stateful task kind: "+str(kind))


def stateful_durable_task(con, kind, task_id):
    if str(kind)=="aggregate":
        return aggregate_task_catalog.task_info(
            con,task_id)
    if str(kind)=="inner_join":
        return join_task_catalog.task_info(
            con,task_id)
    raise ValueError(
        "unsupported stateful task kind: "+str(kind))


def stateful_finish_retirement(
        con,kind,task,mapping,cfg,runtime,frontier
):
    consumer=source_state.consumer_info(
        con,task["consumer_id"])
    watermark=int(consumer["watermark"])
    if watermark!=int(frontier):
        raise RuntimeError(
            "stateful retirement frontier mismatch task=%s "
            "consumer=%d frontier=%d"
            % (task["task_id"],watermark,int(frontier)))
    stateful_stage_pending(
        con,kind,task["consumer_id"],mapping,cfg)
    wake_loaders(runtime,mapping_key(mapping))
    visible=stateful_output_frontier(
        con,kind,task["consumer_id"])
    if visible<int(frontier):
        return False

    durable=stateful_durable_task(
        con,kind,task["task_id"])
    stateful_catalog_runtime.retire_task(
        con,cfg,kind,durable)
    stateful_catalog_runtime.clear_retirement(
        con,task["task_id"])
    reclaimed=stateful_physical_registry.gc_retired(
        con,limit=16)
    key=mapping_key(mapping)
    with runtime["plan_lock"]:
        runtime.get(
            "stateful_active_task_ids",set()).discard(
                task["task_id"])
        runtime.get(
            "stateful_retire_frontiers",{}).pop(
                task["task_id"],None)
        runtime.get(
            "stateful_retire_items",{}).pop(
                task["task_id"],None)
    runtime_mark_sink_retiring(
        runtime,key,cfg)
    log(
        "STATEFUL HOT DROP RETIRED task=%s sink=%s source_seq=%d "
        "visible_frontier=%d target_preserved=1 reclaimed_state=%d"
        % (
            task["task_id"],key,int(frontier),visible,
            len(reclaimed),
        ))
    return True


def stateful_rebuild_writer_drained(
        con,sink_key,task_version
):
    sink_key=str(sink_key)
    writer_version=stateful_task_plan.writer_plan_version(
        task_version)
    if con.execute("""
        SELECT 1 FROM active_jobs
        WHERE table_name=? AND plan_version=?
        LIMIT 1
    """,(sink_key,writer_version)).fetchone():
        return False
    if con.execute("""
        SELECT 1 FROM deliveries
        WHERE table_name=? AND plan_version=?
        LIMIT 1
    """,(sink_key,writer_version)).fetchone():
        return False
    if con.execute("""
        SELECT 1 FROM merge_uncertain
        WHERE table_name=? LIMIT 1
    """,(sink_key,)).fetchone():
        return False
    return True


def stateful_rebuild_remote_state(cfg,rebuild):
    marker=stateful_rebuild.remote_marker(
        rebuild["new_task_id"])
    logical=stateful_rebuild_remote_marker(
        cfg,rebuild["logical_target"])
    shadow=stateful_rebuild_remote_marker(
        cfg,rebuild["shadow_target"])
    if logical==marker and shadow!=marker:
        return "swapped"
    if shadow==marker and logical!=marker:
        return "shadow"
    raise RuntimeError(
        "stateful rebuild remote marker is ambiguous "
        "sink=%s logical_marker=%r shadow_marker=%r expected=%r"
        % (
            rebuild["sink_key"],logical,shadow,marker))


def stateful_rebuild_swap_remote(cfg,rebuild):
    state=stateful_rebuild_remote_state(
        cfg,rebuild)
    if state=="swapped":
        return False
    with mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            cur.execute(
                "ALTER TABLE "
                +sql_name(rebuild["logical_target"],True)
                +" SWAP WITH "
                +sql_name(rebuild["shadow_target"],True))
    if stateful_rebuild_remote_state(
        cfg,rebuild
    )!="swapped":
        raise RuntimeError(
            "StarRocks returned from SWAP without publishing rebuild")
    return True


def stateful_rebuild_cleanup_remote(cfg,rebuild):
    marker=stateful_rebuild.remote_marker(
        rebuild["new_task_id"])
    logical=stateful_rebuild_remote_marker(
        cfg,rebuild["logical_target"])
    if logical not in {
        marker,
        str(rebuild.get("original_comment","")),
    }:
        raise RuntimeError(
            "stateful rebuild logical table marker changed before cleanup")
    with mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            if target_table_exists(
                cur,cfg,rebuild["shadow_target"]
            ):
                cur.execute(
                    "DROP TABLE "
                    +sql_name(
                        rebuild["shadow_target"],True))
            if target_table_exists(
                cur,cfg,rebuild["logical_target"]
            ):
                cur.execute(
                    "ALTER TABLE "
                    +sql_name(
                        rebuild["logical_target"],True)
                    +" COMMENT = %s",
                    (str(rebuild.get(
                        "original_comment","")),))


def stateful_rebuild_freeze_if_ready(
        con,item,result
):
    rebuild=stateful_rebuild.for_task(
        con,item["task"]["task_id"])
    if (
        rebuild is None
        or rebuild["new_task_id"]
        !=item["task"]["task_id"]
        or rebuild["phase"]!="building_shadow"
        or result.get("phase")!="ready"
    ):
        return rebuild
    consumer=result.get("consumer")
    if consumer is None:
        return rebuild
    frontier=int(source_state.base_applied_seq(con))
    if int(consumer["watermark"])>frontier:
        raise RuntimeError(
            "stateful rebuild candidate consumer is ahead of source base")
    rebuild=stateful_rebuild.freeze_frontier(
        con,rebuild["sink_key"],frontier)
    log(
        "STATEFUL REBUILD FENCE sink=%s frontier=%d old=%s new=%s"
        % (
            rebuild["sink_key"],frontier,
            rebuild["old_task_id"],
            rebuild["new_task_id"]))
    return rebuild


def stateful_rebuild_frontier_for_task(
        con,task_id
):
    rebuild=stateful_rebuild.for_task(
        con,task_id)
    if (
        rebuild is None
        or rebuild["frontier"] is None
        or rebuild["phase"] not in {
            "fencing","ready_to_swap"
        }
    ):
        return rebuild,None
    return rebuild,int(rebuild["frontier"])


def stateful_rebuild_switch_runtime(
        runtime,rebuild,candidate
):
    sink=str(rebuild["sink_key"])
    new_item=next(
        item for item in candidate[
            "stateful_candidate_tasks"]
        if item["task"]["task_id"]
        ==rebuild["new_task_id"])
    old_item=next(
        item for item in runtime.get(
            "stateful_tasks",())
        if item["task"]["task_id"]
        ==rebuild["old_task_id"])
    logical=dict(
        new_item.get(
            "logical_mapping")
        or new_item["mapping"])
    logical["sr_table"]=str(
        rebuild["logical_target"])
    live_mapping=new_item["mapping"]
    live_mapping.clear()
    live_mapping.update(logical)
    version=stateful_task_plan.writer_plan_version(
        new_item["task"]["plan_version"])
    old_version=stateful_task_plan.writer_plan_version(
        old_item["task"]["plan_version"])
    with runtime["plan_lock"]:
        runtime.setdefault(
            "stateful_mappings",{})[
                (version,sink)]=live_mapping
        runtime["stateful_mappings"].pop(
            (old_version,sink),None)
        runtime["active_plan_version"]=int(
            candidate["version"])
        runtime["pending_plan"]=None
        runtime["deferred_plan"]=None
        runtime["stateful_tasks"]=list(
            candidate["stateful_candidate_tasks"])
        active_ids=runtime.setdefault(
            "stateful_active_task_ids",set())
        active_ids.discard(
            rebuild["old_task_id"])
        active_ids.add(
            rebuild["new_task_id"])
        runtime.setdefault(
            "stateful_rebuild_plans",{}).pop(
                sink,None)
    return old_item,new_item


def stateful_rebuild_try_cutover(
        con,cfg,runtime,item
):
    task=item["task"]
    rebuild=stateful_rebuild.for_task(
        con,task["task_id"])
    if (
        rebuild is None
        or task["task_id"]!=rebuild["new_task_id"]
        or rebuild["frontier"] is None
        or rebuild["phase"] not in {
            "fencing","ready_to_swap"
        }
    ):
        return False
    frontier=int(rebuild["frontier"])
    kind=str(rebuild["kind"])
    old=stateful_durable_task(
        con,kind,rebuild["old_task_id"])
    new=stateful_durable_task(
        con,kind,rebuild["new_task_id"])
    try:
        old_consumer=source_state.consumer_info(
            con,old["consumer_id"])
        new_consumer=source_state.consumer_info(
            con,new["consumer_id"])
    except KeyError:
        return False
    old_visible=stateful_output_frontier(
        con,kind,old["consumer_id"])
    new_visible=stateful_output_frontier(
        con,kind,new["consumer_id"])
    if not stateful_rebuild.frontier_reached(
        rebuild,
        old_consumer["watermark"],
        new_consumer["watermark"],
        old_visible,new_visible
    ):
        return False
    if not (
        stateful_rebuild_writer_drained(
            con,rebuild["sink_key"],
            old["plan_version"])
        and stateful_rebuild_writer_drained(
            con,rebuild["sink_key"],
            new["plan_version"])
    ):
        return False

    if rebuild["phase"]=="fencing":
        rebuild=stateful_rebuild.mark_ready_to_swap(
            con,rebuild["sink_key"])
    swapped=stateful_rebuild_swap_remote(
        cfg,rebuild)
    rebuild=stateful_rebuild.mark_swapped(
        con,rebuild["sink_key"])

    candidate=runtime.get(
        "stateful_rebuild_plans",{}).get(
            rebuild["sink_key"])
    if candidate is None:
        raise RuntimeError(
            "stateful rebuild candidate plan disappeared before cutover")
    with state_transaction(con):
        meta_set(
            con,"active_plan_version",
            int(candidate["version"]))
        meta_set(
            con,"fingerprint",
            candidate["fingerprint"])
        meta_set(
            con,"plan_cutover_position",
            meta_get(con,"read_position"))
        meta_set(
            con,"plan_cutover_time",
            time.time())
        meta_set(
            con,"plan_history_mode",
            "stateful_shadow_rebuild")
        stateful_rebuild.mark_cleanup(
            con,rebuild["sink_key"])

    old_item,new_item=stateful_rebuild_switch_runtime(
        runtime,rebuild,candidate)
    retired=stateful_catalog_runtime.retire_task(
        con,cfg,kind,old)
    stateful_physical_registry.gc_retired(
        con,kind=kind)
    stateful_rebuild_cleanup_remote(
        cfg,rebuild)
    stateful_rebuild.mark_complete(
        con,rebuild["sink_key"])
    catalog_activation_record(
        runtime,dict(
            status="active",
            version=int(candidate["version"]),
            history_mode="stateful_shadow_rebuild",
            stateful_changed_sinks=[
                rebuild["sink_key"]]))
    log(
        "STATEFUL REBUILD ACTIVE sink=%s frontier=%d old=%s new=%s "
        "remote_swap=%d old_status=%s"
        % (
            rebuild["sink_key"],frontier,
            rebuild["old_task_id"],
            rebuild["new_task_id"],
            1 if swapped else 0,
            retired["task"]["status"]))
    return True


def recover_stateful_rebuilds_startup(
        con,cfg,compiled_stateful,fingerprint
):
    records=stateful_rebuild.active(con)
    if not records:
        return dict(
            compiled=list(compiled_stateful),
            worker_items=list(compiled_stateful),
            protected=list(compiled_stateful),
            rebuild_specs=[],
            recovered_cutover=False)
    if len(records)!=1:
        raise RuntimeError(
            "startup recovery supports one active stateful rebuild at a time")
    rebuild=records[0]
    by_task={
        item["task"]["task_id"]:item
        for item in compiled_stateful
    }
    new_item=by_task.get(
        rebuild["new_task_id"])
    if new_item is None:
        raise RuntimeError(
            "published catalog does not contain durable rebuild generation "
            +rebuild["new_task_id"])
    if (
        str(new_item["kind"])!=rebuild["kind"]
        or new_item["task"]["sink_key"]!=rebuild["sink_key"]
        or new_item["task"]["target_table"]!=rebuild["logical_target"]
    ):
        raise RuntimeError(
            "published rebuild generation identity changed across restart")
    old_task=stateful_durable_task(
        con,rebuild["kind"],
        rebuild["old_task_id"])
    old_item=stateful_catalog_runtime.item_from_durable(
        rebuild["kind"],old_task)
    spec=dict(
        sink=rebuild["sink_key"],
        old=old_item,
        new=new_item,
        shadow_target=rebuild["shadow_target"],
        logical_target=rebuild["logical_target"],
    )

    phase=rebuild["phase"]
    if phase in {
        "building_shadow","fencing","ready_to_swap"
    }:
        remote=stateful_rebuild_remote_state(
            cfg,rebuild)
        if (
            phase=="ready_to_swap"
            and remote=="swapped"
        ):
            rebuild=stateful_rebuild.mark_swapped(
                con,rebuild["sink_key"])
            phase="swapped"
        elif remote!="shadow":
            raise RuntimeError(
                "stateful rebuild remote target was swapped before durable "
                "ready_to_swap phase")

    if phase in {"swapped","cleanup"}:
        if phase=="swapped":
            with state_transaction(con):
                meta_set(
                    con,"active_plan_version",
                    int(cfg.get("catalog_version",0)))
                meta_set(
                    con,"fingerprint",
                    fingerprint)
                meta_set(
                    con,"plan_history_mode",
                    "stateful_shadow_rebuild")
                stateful_rebuild.mark_cleanup(
                    con,rebuild["sink_key"])
            rebuild=stateful_rebuild.info(
                con,rebuild["sink_key"])
        if old_task["status"] not in {
            "retired","failed"
        }:
            stateful_catalog_runtime.retire_task(
                con,cfg,rebuild["kind"],
                old_task)
        stateful_rebuild_cleanup_remote(
            cfg,rebuild)
        stateful_rebuild.mark_complete(
            con,rebuild["sink_key"])
        log(
            "STATEFUL REBUILD RECOVERED CUTOVER sink=%s new=%s"
            % (
                rebuild["sink_key"],
                rebuild["new_task_id"]))
        return dict(
            compiled=list(compiled_stateful),
            worker_items=list(compiled_stateful),
            protected=list(compiled_stateful),
            rebuild_specs=[],
            recovered_cutover=True)

    logical_mapping=dict(
        new_item["mapping"])
    shadow_mapping=dict(
        logical_mapping)
    shadow_mapping["sr_table"]=str(
        rebuild["shadow_target"])
    new_item=dict(new_item)
    new_item["logical_mapping"]=logical_mapping
    new_item["mapping"]=shadow_mapping
    new_item["shadow_target"]=rebuild[
        "shadow_target"]
    new_item["rebuild"]=True
    spec["new"]=new_item
    ensure_stateful_rebuild_shadow(
        cfg,spec,existing_intent=True)
    registered=stateful_catalog_runtime.register_compiled(
        con,[new_item])[0]
    spec["new"]=registered
    final=[]
    for item in compiled_stateful:
        if item["task"]["task_id"]==rebuild[
            "new_task_id"
        ]:
            final.append(registered)
        else:
            final.append(item)
    workers=list(final)+[old_item]
    return dict(
        compiled=final,
        worker_items=workers,
        protected=workers,
        rebuild_specs=[spec],
        recovered_cutover=False)


def stateful_rebuild_guarded_step(
        con,cfg,runtime,item,runner,mapping,
        bootstrap_limit
):
    task=item["task"]
    rebuild=stateful_rebuild.for_task(
        con,task["task_id"])
    if rebuild is None:
        return runner.step(
            con,task["task_id"],cfg,
            mapping=mapping,
            bootstrap_limit=bootstrap_limit)
    lock=runtime.setdefault(
        "stateful_rebuild_locks",{}).setdefault(
            rebuild["sink_key"],threading.Lock())
    with lock:
        rebuild,frontier=stateful_rebuild_frontier_for_task(
            con,task["task_id"])
        if frontier is not None:
            consumer=source_state.consumer_info(
                con,task["consumer_id"])
            watermark=int(consumer["watermark"])
            if watermark>frontier:
                raise RuntimeError(
                    "stateful rebuild worker crossed frozen frontier "
                    "task=%s consumer=%d frontier=%d"
                    % (
                        task["task_id"],watermark,
                        frontier))
            if watermark==frontier:
                cutover=False
                if task["task_id"]==rebuild[
                    "new_task_id"
                ]:
                    cutover=stateful_rebuild_try_cutover(
                        con,cfg,runtime,item)
                return dict(
                    rebuild_paused=True,
                    rebuild_cutover=bool(cutover),
                    rebuild_frontier=frontier)

        result=runner.step(
            con,task["task_id"],cfg,
            mapping=mapping,
            bootstrap_limit=bootstrap_limit)
        rebuild=stateful_rebuild_freeze_if_ready(
            con,item,result)
        if (
            rebuild is not None
            and rebuild["frontier"] is not None
            and rebuild["phase"] in {
                "fencing","ready_to_swap"
            }
        ):
            consumer=result.get("consumer")
            if consumer is not None:
                frontier=int(rebuild["frontier"])
                watermark=int(consumer["watermark"])
                if watermark>frontier:
                    raise RuntimeError(
                        "stateful rebuild runner crossed frozen frontier "
                        "task=%s consumer=%d frontier=%d"
                        % (
                            task["task_id"],watermark,
                            frontier))
                if (
                    watermark==frontier
                    and task["task_id"]==rebuild[
                        "new_task_id"]
                ):
                    result["rebuild_cutover"]=bool(
                        stateful_rebuild_try_cutover(
                            con,cfg,runtime,item))
        return result


def stateful_task_worker(item, cfg, runtime):
    con=open_state(cfg["state"])
    stop=runtime["stop"]
    kind=str(item["kind"])
    task=item["task"]
    mapping=item["mapping"]
    runner=(
        aggregate_task_runner
        if kind=="aggregate"
        else join_task_runner
    )
    relations=(
        [task["source_relation"]]
        if kind=="aggregate"
        else list(task["source_relations"])
    )
    try:
        while not stop.is_set():
            with runtime["plan_lock"]:
                active_ids=runtime.get("stateful_active_task_ids")
                if (
                    active_ids is not None
                    and task["task_id"] not in active_ids
                ):
                    break
            retire_frontier=stateful_retire_frontier(
                runtime,task["task_id"])

            complete=True
            for relation in relations:
                info=source_state.relation_info(con,relation)
                if info["complete_seq"] is None:
                    complete=False
                    break
            if not complete:
                stop.wait(0.05)
                continue

            if retire_frontier is not None:
                consumer=source_state.consumer_info(
                    con,task["consumer_id"])
                watermark=int(consumer["watermark"])
                if watermark>retire_frontier:
                    raise RuntimeError(
                        "stateful worker crossed retirement frontier "
                        "task=%s consumer=%d frontier=%d"
                        % (
                            task["task_id"],watermark,
                            retire_frontier,
                        ))
                if watermark==retire_frontier:
                    if stateful_finish_retirement(
                        con,kind,task,mapping,cfg,runtime,
                        retire_frontier
                    ):
                        break
                    stop.wait(0.05)
                    continue

            try:
                result=stateful_rebuild_guarded_step(
                    con,cfg,runtime,item,runner,mapping,
                    bootstrap_limit=max(
                        1,min(int(cfg.get("snapshot_rows",1000)),4096)))
            except (RuntimeError,KeyError):
                with runtime["plan_lock"]:
                    active_ids=runtime.get("stateful_active_task_ids")
                    removed=(
                        active_ids is not None
                        and task["task_id"] not in active_ids
                    )
                if removed:
                    break
                raise
            if result.get("rebuild_paused"):
                stop.wait(0.05)
                continue
            if result.get("waiting_shared_leader"):
                stop.wait(0.05)
                continue
            wake_loaders(runtime,mapping_key(mapping))
            if result.get("shared_physical"):
                log(
                    "STATEFUL PHYSICAL REUSE task=%s sink=%s kind=%s "
                    "mode=shared_%s leader=%s state=%s"
                    % (
                        task["task_id"],mapping_key(mapping),kind,
                        result.get("shared_reuse_mode","exact"),
                        result.get("shared_leader_task_id"),
                        result.get("shared_state_id"),
                    ))
            elif result.get("reused_physical"):
                log(
                    "STATEFUL PHYSICAL REUSE task=%s sink=%s kind=%s "
                    "fixed_w=%s mode=atomic_clone"
                    % (
                        task["task_id"],mapping_key(mapping),kind,
                        result["generation"].get("fixed_w"),
                    ))
            stateful_physical_registry.sync_runtime_result(
                con,item,result)
            consumer=result.get("consumer")

            if retire_frontier is not None and consumer is not None:
                watermark=int(consumer["watermark"])
                if watermark>retire_frontier:
                    raise RuntimeError(
                        "stateful runner crossed retirement frontier "
                        "task=%s consumer=%d frontier=%d"
                        % (
                            task["task_id"],watermark,
                            retire_frontier,
                        ))
                if watermark==retire_frontier:
                    if stateful_finish_retirement(
                        con,kind,task,mapping,cfg,runtime,
                        retire_frontier
                    ):
                        break
                    stop.wait(0.05)
                    continue

            applied=int(result.get(
                "source_applied",source_state.base_applied_seq(con)))
            caught=(
                consumer is not None
                and int(consumer["watermark"])>=applied
            )
            pending=con.execute("""
                SELECT 1 FROM active_jobs
                WHERE table_name=? LIMIT 1
            """,(mapping_key(mapping),)).fetchone()
            if caught and pending is None and result.get("phase")=="ready":
                stop.wait(0.05)
    finally:
        with runtime["plan_lock"]:
            threads=runtime.get("stateful_worker_threads",{})
            if threads.get(task["task_id"]) is threading.current_thread():
                threads.pop(task["task_id"],None)
        con.close()


def run_cdc(
        cfg, prepared, source_uuid, start, start_gtid, available_logs,
        fingerprint, control=None, catalog_control=None):
    control = control if control is not None else dict(stop=threading.Event(),reason="stopped")
    apply_resource_policy(cfg["resource"])
    if signal_stop_requested(control):
        return 0
    state_dir = os.path.dirname(cfg["state"])
    os.makedirs(state_dir,exist_ok=True)
    with process_lock(cfg["state"]):
        stateful_manifests=list(
            cfg.get("catalog_stateful_tasks",()) or ())
        if stateful_manifests and not cfg.get("shared_source_state",False):
            raise RuntimeError(
                "stateful catalog tasks require CDC_SHARED_SOURCE_STATE=1")
        stateful_scope=(
            stateful_catalog_runtime.source_scope(
                cfg,stateful_manifests,prepared)
            if stateful_manifests
            else dict(
                required_sources=[],
                capture_mappings=list(prepared),
                source_metadata={})
        )
        stateful_source_mappings=[
            mapping
            for mapping in stateful_scope["capture_mappings"]
            if str(mapping["src_table"])
               in set(stateful_scope["required_sources"])
        ]
        compiled_stateful=(
            stateful_catalog_runtime.compile_catalog_tasks(
                cfg,int(cfg.get("catalog_version",0)),
                stateful_manifests,
                stateful_scope["source_metadata"])
            if stateful_manifests
            else []
        )
        con = init_state(cfg["state"])
        migrated_stateful_writer_versions=(
            stateful_catalog_runtime.migrate_writer_versions(con))
        if migrated_stateful_writer_versions:
            log(
                "STATEFUL WRITER VERSION MIGRATION rows=%d"
                % int(migrated_stateful_writer_versions))
        stored_fingerprint = meta_get(con,"fingerprint")
        fresh_state = stored_fingerprint is None
        if stored_fingerprint is None:
            check_initial_targets(cfg,prepared)
        elif meta_get(con,"state_migrated_from") in (2,3) and stored_fingerprint != fingerprint:
            migrated_from = int(meta_get(con,"state_migrated_from"))
            legacy_fingerprint = config_fingerprint(
                cfg,prepared,state_format=migrated_from)
            if (
                stored_fingerprint != legacy_fingerprint
                or meta_get(con,"source_uuid") != source_uuid
            ):
                raise RuntimeError(
                    f"state format {migrated_from} -> {STATE_FORMAT} migration "
                    "fingerprint/source check failed; preserve state and rebuild explicitly")
            with state_transaction(con):
                meta_set(con,"fingerprint",fingerprint)
                meta_set(con,"state_migration_verified",STATE_FORMAT)
            log(
                f"STATE MIGRATION format={migrated_from}->{STATE_FORMAT} "
                "fingerprint_verified=1")
        if cfg.get("shared_source_state",False):
            capture_seed=stateful_catalog_runtime.extend_durable_source_scope(
                con,cfg,
                list(prepared)+list(stateful_source_mappings))
            capture_source_mappings=source_mappings(
                capture_seed)
            # Persist the complete monotonic capture scope in runtime memory so
            # later stateless hot-plan decoder resets cannot drop a relation
            # whose durable shared base still exists.
            stateful_source_mappings=list(
                capture_source_mappings)
        else:
            capture_source_mappings=source_mappings(
                list(prepared))
        bootstrap(con,fingerprint,source_uuid,start,[mapping_key(m) for m in prepared],start_gtid)
        if cfg.get("shared_source_state",False):
            for source_mapping in capture_source_mappings:
                source_state.register_relation(
                    con,source_relation_key(cfg,source_mapping),source_uuid,
                    source_arrow_schema(source_mapping),pk_columns(source_mapping))
            recovered = source_state.apply_pending(con)
            sync_source_base_catalog(con)
            if recovered:
                log(f"SOURCE STATE replayed_pending_commits={recovered}")
        startup_rebuild=recover_stateful_rebuilds_startup(
            con,cfg,compiled_stateful,fingerprint)
        compiled_stateful=list(
            startup_rebuild["compiled"])
        startup_stateful_workers=list(
            startup_rebuild["worker_items"])
        startup_stateful_protected=list(
            startup_rebuild["protected"])
        startup_rebuild_specs=list(
            startup_rebuild["rebuild_specs"])
        rebuild_new_ids={
            spec["new"]["task"]["task_id"]
            for spec in startup_rebuild_specs
        }
        if compiled_stateful:
            registration_safe=[
                item for item in compiled_stateful
                if item["task"]["task_id"]
                not in rebuild_new_ids
            ]
            if registration_safe:
                stateful_catalog_runtime.ensure_registration_safe(
                    con,cfg,registration_safe)
            compiled_stateful=stateful_catalog_runtime.register_compiled(
                con,compiled_stateful)
            registered_by_id={
                item["task"]["task_id"]:item
                for item in compiled_stateful
            }
            if startup_rebuild_specs:
                for spec in startup_rebuild_specs:
                    spec["new"]=registered_by_id[
                        spec["new"]["task"]["task_id"]]
                startup_stateful_workers=[
                    registered_by_id.get(
                        item["task"]["task_id"],item)
                    for item in startup_stateful_workers
                ]
                startup_stateful_protected=[
                    registered_by_id.get(
                        item["task"]["task_id"],item)
                    for item in startup_stateful_protected
                ]
            else:
                startup_stateful_workers=list(
                    compiled_stateful)
                startup_stateful_protected=list(
                    compiled_stateful)
            share_preferences=stateful_share_policy.plan_graph(
                con,compiled_stateful,cfg=cfg)
            if share_preferences:
                log(
                    "STATEFUL SHARE GRAPH preferences=%d mode=%s"
                    % (
                        len(share_preferences),
                        cfg.get("stateful_share_mode","compatible"),
                    ))
        # A catalog restart/drop is a cutover too. Persist a fixed retirement
        # frontier instead of immediately deleting the old consumer; this lets
        # a crash/restart resume the exact same catch-up boundary. An in-flight
        # semantic rebuild protects both old and new generations until SWAP.
        startup_retirement=(
            stateful_catalog_runtime.prepare_startup_retirements(
                con,cfg,startup_stateful_protected,
                source_state.base_applied_seq(con)))
        if startup_retirement["abandoned"]:
            log(
                "STATEFUL STARTUP ABANDON unpublished=%d"
                % len(startup_retirement["abandoned"]))
        retiring_stateful=(
            stateful_catalog_runtime.pending_retirements(con))
        durable_stateful_mappings=(
            stateful_catalog_runtime.durable_mappings(con))
        for spec in startup_rebuild_specs:
            new_item=spec["new"]
            identity=(
                stateful_task_plan.writer_plan_version(
                    new_item["task"]["plan_version"]),
                str(spec["sink"]),
            )
            durable_stateful_mappings[identity]=new_item[
                "mapping"]
        migrate_sink_identity(
            con,cfg["catalog"],int(cfg.get("catalog_version",0)),
            prepared,fresh=fresh_state)
        with state_transaction(con):
            active_version = meta_get(con,"active_plan_version")
            if active_version is None:
                active_version = int(cfg.get("catalog_version",0))
                meta_set(con,"active_plan_version",active_version)
            if int(active_version) != int(cfg.get("catalog_version",0)):
                if (
                    cfg.get("catalog_rebuild_recover",False)
                    and startup_rebuild_specs
                ):
                    log(
                        "STATEFUL REBUILD RESTART RESUME active=%d "
                        "published=%d sinks=%s"
                        % (
                            int(active_version),
                            int(cfg.get("catalog_version",0)),
                            ",".join(
                                sorted(
                                    spec["sink"]
                                    for spec in startup_rebuild_specs)),
                        ))
                elif not cfg.get("catalog_restart_promote",False):
                    raise RuntimeError(
                        f"durable active plan {active_version} differs from loaded catalog "
                        f"plan {cfg.get('catalog_version',0)}")
                else:
                    old_version=int(active_version)
                    active_version=int(cfg.get("catalog_version",0))
                    meta_set(con,"active_plan_version",active_version)
                    meta_set(con,"fingerprint",fingerprint)
                    meta_set(con,"stateful_restart_from_plan",old_version)
                    meta_set(con,"stateful_restart_to_plan",active_version)
                    log(
                        f"STATEFUL PLAN RESTART CUTOVER old={old_version} "
                        f"new={active_version}")
            con.execute(
                "UPDATE jobs SET plan_version=? WHERE plan_version=0",
                (int(active_version),))
            con.execute(
                "UPDATE deliveries SET plan_version=? WHERE plan_version=0",
                (int(active_version),))
            meta_set(con,"plan_versioned_jobs_v1",1)
        saved = meta_get(con,"read_position")
        saved_gtid = meta_get(con,"gtid_set")
        if saved_gtid is not None:
            if start_gtid is None:
                raise RuntimeError("durable GTID state cannot resume on a source with GTID OFF")
            if not gtid_contains(start_gtid,saved_gtid):
                con.close()
                raise RuntimeError("source gtid_executed does not contain the durable GTID set")
        else:
            if saved[0] not in available_logs:
                con.close()
                raise RuntimeError(f"saved binlog {saved[0]} was purged; preserve state and explicitly rebuild")
            if not position_ge(start,saved):
                con.close()
                raise RuntimeError("source log position regressed; refuse automatic reset")
        stored_partitions = meta_get(con,"key_partitions")
        if stored_partitions is None:
            with state_transaction(con):
                meta_set(con,"key_partitions",cfg["key_partitions"])
            stored_partitions = cfg["key_partitions"]
        if int(stored_partitions) != cfg["key_partitions"]:
            raise RuntimeError("key partition count changed; use a fresh state and rebuild explicitly")

        active_plan = runtime_plan_entry(
            int(active_version),prepared,
            cfg.get("catalog_macros",()),cfg.get("catalog_udfs",()),fingerprint)
        recovered_plans = {int(active_plan["version"]):active_plan}
        startup_rebuild_plans={}
        if startup_rebuild_specs:
            rebuild_plan=runtime_plan_entry(
                int(cfg.get("catalog_version",0)),prepared,
                cfg.get("catalog_macros",()),
                cfg.get("catalog_udfs",()),fingerprint)
            rebuild_plan["stateful_candidate_tasks"]=list(
                compiled_stateful)
            rebuild_plan["stateful_rebuilds"]=list(
                startup_rebuild_specs)
            recovered_plans[int(rebuild_plan["version"])]=rebuild_plan
            startup_rebuild_plans={
                str(spec["sink"]):rebuild_plan
                for spec in startup_rebuild_specs
            }
        stateful_mappings=dict(durable_stateful_mappings)
        current_stateful_keys={
            mapping_key(item["mapping"])
            for item in retiring_stateful
        }
        for item in compiled_stateful:
            mapping=item["mapping"]
            key=mapping_key(mapping)
            version=stateful_task_plan.writer_plan_version(
                item["task"]["plan_version"])
            identity=(version,key)
            previous=stateful_mappings.get(identity)
            if previous is not None and (
                previous.get("_output_columns")!=mapping.get("_output_columns")
                or previous.get("sr_table")!=mapping.get("sr_table")
            ):
                raise RuntimeError(
                    "durable stateful writer identity changed: %r" % (identity,))
            if key in active_plan["by_table"]:
                raise RuntimeError(
                    "stateful/stateless sink identity collision: "+key)
            stateful_mappings[identity]=mapping
            current_stateful_keys.add(key)

        worker_by_table = dict(active_plan["by_table"])
        rebuild_sinks={
            str(spec["sink"])
            for spec in startup_rebuild_specs
        }
        for mapping in stateful_mappings.values():
            key=mapping_key(mapping)
            previous=worker_by_table.get(key)
            if previous is not None and previous is not mapping:
                if key not in rebuild_sinks:
                    raise RuntimeError(
                        "multiple active writer mappings for sink "+key)
                # During a semantic rebuild one physical writer pool drains
                # jobs for both generation versions; runtime_mapping() binds
                # each delivery back to its exact old/new mapping.
                continue
            worker_by_table[key]=mapping

        durable_versions = con.execute("""
            SELECT DISTINCT table_name,plan_version FROM active_jobs
            UNION
            SELECT DISTINCT table_name,plan_version FROM deliveries
        """).fetchall()
        for table,version in durable_versions:
            version = int(version)
            stateful=stateful_mappings.get((version,str(table)))
            if stateful is not None:
                worker_by_table.setdefault(str(table),stateful)
                continue
            if version not in recovered_plans:
                historical = prepare_runtime_catalog_plan(
                    cfg,cdc_catalog.load_plan_version(cfg["catalog"],version))
                recovered_plans[version] = historical
            mapping = recovered_plans[version]["by_table"].get(table)
            if mapping is None:
                raise RuntimeError(
                    f"durable job table {table} is absent from catalog plan {version}")
            worker_by_table.setdefault(table,mapping)
        worker_mappings = list(worker_by_table.values())
        draining_tables=sorted(
            set(worker_by_table)
            -set(active_plan["by_table"])
            -current_stateful_keys)
        if draining_tables:
            log(
                "PLAN RECOVERY active_tables=%d stateful_tables=%d "
                "draining_tables=%s"
                % (
                    len(prepared),len(current_stateful_keys),
                    draining_tables,
                )
            )
        enforce_resource_config(cfg,len(worker_mappings))
        log(
            "RESOURCE COMPUTE duckdb_memory=%s requested_mb=%.1f "
            "engine_slots=%d writer_memory_cap=%d aggregate_duckdb_cap_mb=%.1f "
            "prepared_cap_mb=%.1f"
            % (
                cfg["duckdb_memory"],
                cfg["duckdb_memory_requested_bytes"]/1024**2,
                cfg["duckdb_engine_slots"],
                cfg["duckdb_memory_writer_cap"],
                cfg["duckdb_memory_cap_bytes"]*cfg["duckdb_engine_slots"]/1024**2,
                cfg["max_prepared_bytes"]/1024**2,
            )
        )

        if cfg["load_mode"] == "merge_async":
            unresolved = con.execute("SELECT COUNT(*) FROM load_transactions").fetchone()[0]
            if unresolved:
                raise RuntimeError("cannot enter merge_async with unresolved 2PC transactions; "
                                   "restart once with CDC_LOAD_MODE=transaction to drain them")
            load_events = {
                mapping_key(mapping):threading.Event() for mapping in worker_mappings}
        else:
            load_events = {
                (mapping_key(mapping),0):threading.Event() for mapping in worker_mappings}

        now = time.time()
        startup_retiring = (
            set(worker_by_table)
            -set(active_plan["by_table"])
            -current_stateful_keys
        )
        runtime = dict(stop=control["stop"],reader_ready=threading.Event(),errors=[],
                       error_lock=threading.Lock(),stream=None,source_uuid=str(source_uuid),source_seen=now,
                       source_data_seen=now,heartbeat_count=0,cdc_transactions=0,
                       load_events=load_events,pressure_until={mapping_key(m):0 for m in worker_mappings},
                       table_interval={mapping_key(m):cfg["commit_interval_ms"]/1000 for m in worker_mappings},
                       control_lock=threading.Lock(),
                       active_writers={mapping_key(m):min(
                           cfg["writer_initial"],
                           max(1,cfg["resource"]["cpu_target"]//max(1,len(worker_mappings))))
                           for m in worker_mappings},
                       last_pressure={mapping_key(m):now for m in worker_mappings},
                       last_scale={mapping_key(m):now for m in worker_mappings},
                       max_rowset={mapping_key(m):-1 for m in worker_mappings},
                       version_recovery={mapping_key(m):False for m in worker_mappings},
                       version_recovery_good={mapping_key(m):0 for m in worker_mappings},
                       resource_writer_cap=max(
                           1,min(
                               cfg["writer_max"],
                               cfg["resource"]["cpu_target"]//max(1,len(worker_mappings)))),
                       resource_pause_snapshot=False,resource_sample=None,quarantined_tables={},
                       snapshot_transform_bytes_cap={
                           mapping_key(m):int(cfg["batch_bytes"])
                           for m in worker_mappings},
                       lane_locks={},plan_lock=threading.RLock(),
                       active_plan_version=int(active_version),
                       catalog_activation=dict(
                           status=(
                               "rebuild_pending"
                               if startup_rebuild_specs else "active"),
                           version=(
                               int(cfg.get("catalog_version",0))
                               if startup_rebuild_specs
                               else int(active_version))),
                       plans=recovered_plans,
                       stateful_mappings=stateful_mappings,
                       stateful_tasks=list(startup_stateful_workers),
                       stateful_active_task_ids={
                           item["task"]["task_id"]
                           for item in (
                               list(startup_stateful_workers)
                               +list(retiring_stateful))},
                       stateful_worker_threads={},
                       stateful_rebuild_plans=dict(
                           startup_rebuild_plans),
                       stateful_rebuild_locks={
                           str(spec["sink"]):threading.Lock()
                           for spec in startup_rebuild_specs},
                       stateful_retire_frontiers={
                           item["task"]["task_id"]:int(item["frontier"])
                           for item in retiring_stateful},
                       stateful_retire_items={
                           item["task"]["task_id"]:item
                           for item in retiring_stateful},
                       stateful_source_mappings=list(stateful_source_mappings),
                       pending_plan=None,deferred_plan=None,
                       validated_catalog_plans={},
                       worker_mappings=worker_mappings,
                       worker_keys={mapping_key(m) for m in worker_mappings},
                       retiring_sinks=set(startup_retiring),
                       retiring_workers={
                           key:(
                               int(cfg["writer_max"])
                               if cfg["load_mode"] == "merge_async" else 1)
                           for key in startup_retiring},
                       worker_threads=[],thread_lock=threading.Lock(),
                       snapshot_executor=None)
        quarantine_pending_merges(con,runtime)
        runtime["plan_loader"] = lambda version: prepare_runtime_catalog_plan(
            cfg,cdc_catalog.load_plan_version(cfg["catalog"],version))
        runtime["metrics"] = init_run_metrics(worker_mappings)
        metrics_path,summary_path = report_paths(cfg)
        append_report(metrics_path,dict(
            event="run_start",run_id=runtime["metrics"]["run_id"],
            timestamp=runtime["metrics"]["started"],protocol=cfg["load_mode"],
            catalog_version=cfg.get("catalog_version",0),
            catalog_hash=cfg.get("catalog_hash",""),
            tables=[mapping_key(m) for m in prepared],
            draining_tables=sorted(startup_retiring),
        ))
        log(f"REPORT metrics={metrics_path} summary={summary_path} "
            f"detail_logs={int(cfg['detail_logs'])}")
        published_version = int(cfg.get("catalog_published_version",0))
        if (
            published_version
            and published_version != runtime_active_version(runtime)
            and not startup_rebuild_specs
        ):
            queue_hot_catalog_plan(
                cfg,runtime,dict(version=published_version))
        for mapping in worker_mappings:
            for lane in range(cfg["key_partitions"]):
                runtime["lane_locks"][(mapping_key(mapping),lane)] = threading.Lock()

        if catalog_control is not None:
            catalog_control_activate(catalog_control,cfg,runtime)

        threads,executor = runtime["worker_threads"],None
        try:
            checkpoint_thread = threading.Thread(
                target=state_checkpoint_worker,args=(cfg,runtime),name="state-checkpoint")
            checkpoint_thread.start()
            threads.append(checkpoint_thread)
            gc_thread = threading.Thread(
                target=state_gc_worker,args=(cfg,runtime),name="state-gc")
            gc_thread.start()
            threads.append(gc_thread)
            resource_thread = threading.Thread(
                target=guarded_worker,args=(resource_monitor_worker,runtime,cfg,worker_mappings),
                name="resource-monitor")
            resource_thread.start()
            threads.append(resource_thread)
            if cfg["load_mode"] == "merge_async":
                for mapping in list(worker_mappings):
                    for worker_id in range(cfg["writer_max"]):
                        thread = threading.Thread(target=guarded_worker,
                                                  args=(merge_delivery_worker,runtime,mapping,worker_id,cfg),
                                                  name=f"merge-{mapping_key(mapping)}-{worker_id}")
                        thread.start()
                        threads.append(thread)

                controller = threading.Thread(target=guarded_worker,
                                              args=(adaptive_writer_controller,runtime,cfg,worker_mappings),
                                              name="writer-controller")
                controller.start()
                threads.append(controller)
            else:
                for mapping in list(worker_mappings):
                    thread = threading.Thread(target=guarded_worker,
                                              args=(table_delivery_worker,runtime,mapping,cfg),
                                              name=f"load-{mapping_key(mapping)}")
                    thread.start()
                    threads.append(thread)
            reader = threading.Thread(target=guarded_worker,args=(capture_binlog,runtime,cfg,prepared),
                                      name="binlog-reader")
            reader.start()
            threads.append(reader)
            executor = ThreadPoolExecutor(
                max_workers=cfg["snapshot_workers"],thread_name_prefix="backfill")
            runtime["snapshot_executor"] = executor
            futures = []
            if cfg.get("shared_source_state",False):
                # All source-mirror jobs are queued before sink builds. With a
                # single worker this guarantees the mirror completes first;
                # with multiple workers, queued source jobs still precede every
                # sink job. Restarted hot-added sinks therefore keep using the
                # durable fixed-W cursor instead of falling back to MySQL.
                futures.extend(
                    executor.submit(
                        guarded_worker,source_state_snapshot_worker,
                        runtime,mapping,cfg)
                    for mapping in capture_source_mappings)
                futures.extend(
                    executor.submit(
                        guarded_worker,shared_snapshot_worker,
                        runtime,mapping,cfg)
                    for mapping in prepared)
            else:
                futures.extend(
                    executor.submit(
                        guarded_worker,snapshot_worker,runtime,mapping,cfg)
                    for mapping in prepared)
            stateful_worker_items=[]
            seen_stateful_workers=set()
            for item in list(startup_stateful_workers)+list(retiring_stateful):
                task_id=item["task"]["task_id"]
                if task_id in seen_stateful_workers:
                    continue
                seen_stateful_workers.add(task_id)
                stateful_worker_items.append(item)
            for item in stateful_worker_items:
                thread=threading.Thread(
                    target=guarded_worker,
                    args=(stateful_task_worker,runtime,item,cfg),
                    name="stateful-"+str(item["kind"])+"-"
                         +mapping_key(item["mapping"]))
                thread.start()
                threads.append(thread)
                runtime["stateful_worker_threads"][
                    item["task"]["task_id"]]=thread
            began = time.monotonic()
            next_status = began + cfg["status_seconds"]
            if cfg["load_mode"] == "merge_async":
                log(f"START source_reader={cfg.get('source_reader','native_c_v1')} "
                    f"tables={len(prepared)} stateful_tasks={len(compiled_stateful)} protocol=merge_commit_async "
                    f"key_partitions={cfg['key_partitions']} writers_initial={cfg['writer_initial']} "
                    f"writers_min={cfg['writer_min']} writers_max={cfg['writer_max']} "
                    f"snapshot_bundle_max_lanes={cfg['snapshot_bundle_max_lanes']} "
                    f"snapshot_read_ahead_groups={cfg['snapshot_read_ahead_groups']} "
                    f"merge_interval_ms={cfg['merge_commit_interval_ms']} "
                    f"merge_parallel={cfg['merge_commit_parallel']} "
                    f"shared_source_state={int(cfg.get('shared_source_state',False))} "
                    f"durable_position={saved[0]}:{saved[1]} gtid_resume={int(saved_gtid is not None)}")
            else:
                log(f"START source_reader={cfg.get('source_reader','native_c_v1')} "
                    f"tables={len(prepared)} stateful_tasks={len(compiled_stateful)} protocol=transaction writer_per_table=1 "
                    f"key_partitions={key_partition_count(cfg)} "
                    f"shared_source_state={int(cfg.get('shared_source_state',False))} "
                    f"durable_position={saved[0]}:{saved[1]} gtid_resume={int(saved_gtid is not None)}")
            while not runtime["stop"].wait(1) and not signal_stop_requested(control):
                now_mono = time.monotonic()
                if cfg["run_seconds"] and now_mono-began >= cfg["run_seconds"]:
                    control["reason"] = "run_seconds_limit"
                    break
                if now_mono < next_status:
                    continue
                pending = con.execute(
                    "SELECT COUNT(*),MIN(created),COALESCE(SUM(logical_bytes),0) FROM active_jobs").fetchone()
                prepared_bytes = con.execute(
                    "SELECT COALESCE(SUM(length(payload)),0) FROM load_parts WHERE visible=0").fetchone()[0]
                reserved_bytes = con.execute(
                    "SELECT COALESCE(SUM(reserved_bytes),0) FROM prepare_reservations").fetchone()[0]
                requirement_state = con.execute("""
                    SELECT COUNT(*),COALESCE(SUM(required_bytes),0),
                           COALESCE(SUM(full_prepare_attempts),0)
                    FROM prepare_requirements WHERE required_bytes IS NOT NULL
                """).fetchone()
                inflight = con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
                done,backfill_tables = active_snapshot_status(con,runtime)
                draining_versions = durable_draining_plan_versions(
                    con,runtime_active_version(runtime))
                if done == backfill_tables and not draining_versions:
                    with runtime["plan_lock"]:
                        deferred = runtime.get("deferred_plan")
                        if deferred is not None and runtime.get("pending_plan") is None:
                            runtime["pending_plan"] = deferred
                            runtime["deferred_plan"] = None
                            log(
                                f"PLAN PENDING version={deferred['version']} "
                                "after snapshot/draining prerequisites completed")
                read = meta_get(con,"read_position")
                age = max(0,time.time()-pending[1]) if pending[1] is not None else 0
                sharing_status=stateful_share_policy.status(con)
                writer_state = ""
                if cfg["load_mode"] == "merge_async":
                    writer_state = " writers=" + ",".join(
                        f"{mapping_key(m)}:{writer_target(runtime,mapping_key(m))}/{cfg['writer_max']}"
                        f":rowset={runtime['max_rowset'].get(mapping_key(m),-1)}"
                        f":version_recovery={int(version_recovery_active(runtime,mapping_key(m)))}"
                        for m in list(runtime["worker_mappings"]))
                log(f"STATUS backfill_done={done}/{backfill_tables} pending_jobs={pending[0]} "
                    f"pending_bytes={pending[2]} prepared_bytes={prepared_bytes} "
                    f"prepare_reserved_bytes={reserved_bytes} "
                    f"prepare_known={requirement_state[0]} "
                    f"prepare_required_bytes={requirement_state[1]} "
                    f"prepare_full_attempts={requirement_state[2]} "
                    f"prepared_budget_used={prepared_bytes+reserved_bytes} inflight={inflight} "
                    f"oldest_queue_seconds={age:.3f} read={read[0]}:{read[1]} "
                    f"stream_silence_seconds={time.time()-runtime['source_seen']:.1f} "
                    f"data_silence_seconds={time.time()-runtime['source_data_seen']:.1f} "
                    f"heartbeats={runtime['heartbeat_count']} cdc_transactions={runtime['cdc_transactions']} "
                    f"reader_state={runtime.get('reader_state','unknown')} "
                    f"reader_retries={runtime.get('reader_retries',0)} "
                    f"active_plan={runtime_active_version(runtime)} "
                    f"pending_plan={(runtime.get('pending_plan') or {}).get('version',0)} "
                    f"shared_selected={sharing_status['selected']} "
                    f"shared_samples={sharing_status['samples']} "
                    f"shared_max_source_lag={sharing_status['max_source_lag']}"
                    f"{writer_state}")
                report_state = dict(
                    backfill_done=done,backfill_tables=backfill_tables,pending_jobs=pending[0],
                    pending_bytes=pending[2],prepared_bytes=prepared_bytes,
                    prepare_reserved_bytes=reserved_bytes,
                    prepare_known=requirement_state[0],
                    prepare_required_bytes=requirement_state[1],
                    prepare_full_attempts=requirement_state[2],
                    prepared_budget_used=prepared_bytes+reserved_bytes,inflight=inflight,
                    oldest_queue_seconds=age,durable_position=f"{read[0]}:{read[1]}",
                    stream_silence_seconds=time.time()-runtime["source_seen"],
                    data_silence_seconds=time.time()-runtime["source_data_seen"],
                    heartbeats=runtime["heartbeat_count"],cdc_transactions=runtime["cdc_transactions"],
                    reader_state=runtime.get("reader_state","unknown"),
                    reader_retries=int(runtime.get("reader_retries",0)),
                    active_plan_version=runtime_active_version(runtime),
                    catalog_activation=dict(runtime.get("catalog_activation",{})),
                    stateful_sharing=dict(sharing_status),
                    quarantined_tables=dict(runtime.get("quarantined_tables",{})),
                    health="degraded" if runtime.get("quarantined_tables") else "normal",
                    pending_plan_version=int((runtime.get("pending_plan") or {}).get("version",0)),
                    draining_plan_versions=draining_versions,
                    deferred_plan_version=int((runtime.get("deferred_plan") or {}).get("version",0)),
                    field_overflow_rows=con.execute("SELECT COUNT(*) FROM field_overflow").fetchone()[0],
                    merge_uncertain_rows=con.execute("SELECT COUNT(*) FROM merge_uncertain").fetchone()[0],
                )
                metrics_status_record(
                    runtime,cfg,list(runtime["worker_mappings"]),report_state)
                active = pending[0] or inflight or done < backfill_tables
                next_status = now_mono + (cfg["status_seconds"] if active else cfg["idle_status_seconds"])
                if shutil.disk_usage(state_dir).free < cfg["min_free_bytes"]:
                    raise RuntimeError("journal disk space below CDC_MIN_FREE_BYTES")
        except KeyboardInterrupt:
            if control["reason"] == "stopped":
                control["reason"] = "keyboard_interrupt"
        except Exception as exc:
            with runtime["error_lock"]:
                runtime["errors"].append(("main",str(exc)))
            log(f"FATAL worker=main: {exc}")
            raise
        finally:
            runtime["stop"].set()
            wake_loaders(runtime)
            log(f"STOP requested reason={control['reason']} worker_errors={len(runtime['errors'])}; journal retained")
            stream = runtime.get("stream")
            runtime["stream"] = None
            replication_close_stream(stream)
            runtime["snapshot_executor"] = None
            if executor is not None:
                executor.shutdown(wait=True,cancel_futures=True)
            with runtime["thread_lock"]:
                shutdown_threads = list(runtime["worker_threads"])
            for thread in shutdown_threads:
                thread.join()
            try:
                con.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except sqlite3.Error as exc:
                log(f"STOP checkpoint: {exc}")
            reason = "error" if runtime["errors"] else control["reason"]
            try:
                final_run_summary(
                    runtime,cfg,list(runtime["worker_mappings"]),con,reason)
            finally:
                con.close()
        if runtime["errors"]:
            raise RuntimeError(f"pipeline stopped: {runtime['errors'][0]}")
        return 0


def durable_active_plan_version(state_path):
    if not os.path.exists(state_path):
        return None
    con = open_state(state_path)
    try:
        value = meta_get(con,"active_plan_version")
        return int(value) if value is not None else None
    finally:
        con.close()


def durable_stateful_rebuild_records(state_path):
    if not os.path.exists(state_path):
        return []
    con=open_state(state_path)
    try:
        exists=con.execute("""
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='stateful_rebuilds'
        """).fetchone()
        if not exists:
            return []
        rows=con.execute("""
            SELECT sink_key FROM stateful_rebuilds
            WHERE phase!='complete'
            ORDER BY created,sink_key
        """).fetchall()
        return [
            stateful_rebuild.info(
                con,row[0])
            for row in rows
        ]
    finally:
        con.close()


def activate_catalog(cfg):
    global mappings
    release_plan = os.environ.get("CDC_RELEASE_CATALOG_PLAN","").strip()
    if release_plan:
        published = orjson.loads(release_plan)
        plan = published
    else:
        published = cdc_catalog.load_plan(cfg["catalog"],cfg["catalog_seed"])
        active_version = durable_active_plan_version(cfg["state"])
        rebuild_records=durable_stateful_rebuild_records(
            cfg["state"])
        failed_rebuilds=[
            item for item in rebuild_records
            if item["phase"]=="failed"
        ]
        if failed_rebuilds:
            raise RuntimeError(
                "durable stateful rebuild is failed; inspect/clear it before "
                "catalog activation: "
                +",".join(
                    item["sink_key"]
                    for item in failed_rebuilds))
        cfg["catalog_restart_promote"] = False
        cfg["catalog_rebuild_recover"] = False
        if active_version and active_version != int(published.get("version",0)):
            active_plan=cdc_catalog.load_plan_version(
                cfg["catalog"],active_version)
            stateful_changed=(
                list(active_plan.get("stateful_tasks",()))
                !=list(published.get("stateful_tasks",()))
            )
            if stateful_changed:
                if list(active_plan.get("mappings",()))!=list(
                    published.get("mappings",())
                ):
                    raise RuntimeError(
                        "restart-boundary stateful activation cannot also "
                        "change stateless sink topology; publish those changes "
                        "separately")
                plan=published
                if rebuild_records:
                    cfg["catalog_rebuild_recover"]=True
                else:
                    cfg["catalog_restart_promote"] = True
            else:
                plan=active_plan
        else:
            plan=published
    mappings = [dict(item) for item in plan["mappings"]]
    cfg["catalog_stateful_tasks"] = [
        dict(item) for item in plan.get("stateful_tasks",())
    ]
    version = int(plan.get("version",0))
    for mapping in mappings:
        mapping["_plan_version"] = version
    cfg["catalog_version"] = version
    cfg["catalog_published_version"] = int(published.get("version",version))
    cfg["catalog_hash"] = str(plan.get("plan_hash",""))
    cfg["catalog_macros"] = list(plan.get("macros",()))
    cfg["catalog_udfs"] = list(plan.get("udfs",()))
    log(
        f"CATALOG active_version={cfg['catalog_version']} "
        f"published_version={cfg['catalog_published_version']} "
        f"hash={cfg['catalog_hash'][:16]} mappings={len(mappings)} "
        f"stateful_tasks={len(cfg.get('catalog_stateful_tasks',()))} "
        f"macros={len(cfg['catalog_macros'])} udfs={len(cfg['catalog_udfs'])}")
    return mappings


def catalog_bootstrap_paths():
    bootstrap = cdc_catalog.catalog_paths(__file__)
    variables = cdc_catalog.variables_get(bootstrap["catalog"])
    return cdc_catalog.catalog_paths(__file__,variables=variables)


def catalog_bootstrap_ready(paths):
    variables = cdc_catalog.variables_get(paths["catalog"])
    try:
        if cdc_catalog.connection_settings_values(
                variables,require=False) is None:
            return False
    except ValueError:
        return False
    current_paths = cdc_catalog.catalog_paths(__file__,variables=variables)
    try:
        plan = cdc_catalog.load_plan(paths["catalog"],current_paths["seed"])
    except RuntimeError as exc:
        if "catalog has no published plan" in str(exc):
            return False
        raise
    return bool(plan.get("mappings") or plan.get("stateful_tasks"))


def bootstrap_publish_draft(paths, ready):
    variables = cdc_catalog.variables_get(paths["catalog"])
    if cdc_catalog.connection_settings_values(
            variables,require=False) is None:
        return None
    con = cdc_catalog.catalog_open(paths["catalog"])
    try:
        mappings,stateful_tasks,_,_,_ = cdc_catalog.compile_draft(con)
    finally:
        con.close()
    if not mappings and not stateful_tasks:
        return None
    return cdc_catalog.publish(
        paths["catalog"],
        lambda result,phase: bootstrap_catalog_publish_callback(
            paths,ready,result,phase))


def bootstrap_catalog_publish_callback(paths, ready, publish_result, phase):
    if phase in ("validate","validate_config"):
        return validate_local_catalog_publish(publish_result,phase)
    if phase in ("install","install_config"):
        if catalog_bootstrap_ready(paths):
            installed=validate_local_catalog_publish(
                publish_result,phase)
            ready.set()
            return dict(
                status="daemon_starting",
                version=int(publish_result.get("version",0)),
                target_install_status=str(
                    installed.get("status","validated")))
        if phase == "install_config":
            try:
                promoted = bootstrap_publish_draft(paths,ready)
            except Exception as exc:
                return dict(
                    status="bootstrap_waiting",
                    version=int(publish_result.get("version",0)),
                    note="draft publish deferred: "+str(exc))
            if promoted is not None and ready.is_set():
                return dict(
                    status="daemon_starting",
                    version=int(promoted.get("version",0)),
                    promoted_draft=True)
        return dict(
            status="bootstrap_waiting",
            version=int(publish_result.get("version",0)),
            note=(
                "waiting for complete connection settings and at least one "
                "validated published sink"))
    raise ValueError(f"unknown bootstrap catalog phase: {phase}")


def catalog_control_active_version(control_state):
    with control_state["lock"]:
        runtime = control_state.get("runtime")
    return runtime_active_version(runtime) if runtime is not None else 0


def catalog_control_publish(control_state, publish_result, callback_phase):
    with control_state["lock"]:
        mode = control_state["mode"]
        cfg = control_state.get("cfg")
        runtime = control_state.get("runtime")
    if mode == "starting":
        if callback_phase in ("validate","validate_config"):
            raise cdc_catalog.CatalogBusyError(
                "J4 data plane is starting; retry this catalog mutation after startup")
        return dict(
            status="restart_required",
            version=int(publish_result.get("version",0)),
            reason="catalog mutation crossed the daemon startup boundary; retry")
    if mode == "running":
        if cfg is None or runtime is None:
            raise RuntimeError("catalog control entered running mode without runtime state")
        return catalog_publish_callback(cfg,runtime,publish_result,callback_phase)
    if mode != "bootstrap":
        raise RuntimeError(f"unknown catalog control mode: {mode}")
    result = bootstrap_catalog_publish_callback(
        control_state["paths"],control_state["ready"],
        publish_result,callback_phase)
    if (
        callback_phase in ("install","install_config")
        and str(result.get("status","")) == "daemon_starting"
    ):
        with control_state["lock"]:
            if control_state["mode"] == "bootstrap":
                control_state["mode"] = "starting"
    return result


def catalog_control_worker(control_state):
    process_control = control_state["process_control"]
    try:
        cdc_catalog.server_worker(
            control_state["paths"]["catalog"],
            control_state["paths"]["socket"],
            0,process_control["stop"],
            lambda result,phase: catalog_control_publish(
                control_state,result,phase),
            lambda: catalog_control_active_version(control_state),
            control_state["listening"])
    except Exception as exc:
        if process_control["stop"].is_set():
            return
        with control_state["lock"]:
            runtime = control_state.get("runtime")
            control_state["errors"].append(exc)
        if runtime is not None:
            with runtime["error_lock"]:
                runtime["errors"].append(("catalog_control_worker",str(exc)))
        log(f"FATAL worker=catalog_control_worker: {exc}")
        traceback.print_exc(file=sys.stdout)
        control_state["listening"].set()
        control_state["ready"].set()
        process_control["stop"].set()


def start_catalog_control_daemon(control):
    paths = catalog_bootstrap_paths()
    control_state = dict(
        paths=paths,
        lock=threading.RLock(),
        ready=threading.Event(),
        listening=threading.Event(),
        errors=[],
        mode="bootstrap",
        cfg=None,
        runtime=None,
        process_control=control,
    )
    thread = threading.Thread(
        target=catalog_control_worker,
        args=(control_state,),
        name="catalog-control")
    control_state["thread"] = thread
    thread.start()
    while not control_state["listening"].wait(0.1):
        if control_state["errors"]:
            raise control_state["errors"][0]
        if signal_stop_requested(control):
            return control_state
    if control_state["errors"]:
        raise control_state["errors"][0]
    if catalog_bootstrap_ready(paths):
        with control_state["lock"]:
            control_state["mode"] = "starting"
        control_state["ready"].set()
    else:
        log(
            f"BOOTSTRAP WAITING catalog={paths['catalog']} "
            f"shell_socket={paths['socket']} "
            "run 'python j4.py cli' or 'python j4.py sql <file.sql>'")
    return control_state


def wait_catalog_control_ready(control_state, control):
    global _CATALOG_VARIABLES
    while not control_state["ready"].wait(0.25):
        if control_state["errors"]:
            raise control_state["errors"][0]
        if signal_stop_requested(control):
            return False
    if control_state["errors"]:
        raise control_state["errors"][0]
    if signal_stop_requested(control):
        return False
    with control_state["lock"]:
        if control_state["mode"] == "bootstrap":
            control_state["mode"] = "starting"
    # The transaction that made bootstrap ready committed before setting the
    # event. Discard any empty/startup cache and reread the durable catalog.
    _CATALOG_VARIABLES = None
    return True


def catalog_control_activate(control_state, cfg, runtime):
    paths = control_state["paths"]
    if os.path.abspath(cfg["catalog"]) != os.path.abspath(paths["catalog"]):
        raise RuntimeError("catalog path changed during daemon startup")
    if os.path.abspath(cfg["catalog_socket"]) != os.path.abspath(paths["socket"]):
        raise RuntimeError(
            "catalog socket locator changed during daemon startup; "
            "CDC_CATALOG_SOCKET is bootstrap-only")
    with control_state["lock"]:
        if control_state["errors"]:
            raise control_state["errors"][0]
        control_state["cfg"] = cfg
        control_state["runtime"] = runtime
        control_state["mode"] = "running"
    log(
        f"CATALOG CONTROL RUNNING socket={paths['socket']} "
        f"active_version={runtime_active_version(runtime)}")


def stop_catalog_control(control_state):
    if control_state is None:
        return
    control_state["process_control"]["stop"].set()
    thread = control_state.get("thread")
    if thread is not None:
        thread.join()




def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",nargs="?",default="run",
        choices=("run","cli","sql","check","probe","selftest"))
    parser.add_argument("sql_file",nargs="?")
    args = parser.parse_args()

    if args.command in ("cli","sql"):
        if args.command == "sql" and not args.sql_file:
            parser.error("python j4.py sql requires a SQL file")
        if args.command == "cli" and args.sql_file:
            parser.error("python j4.py cli does not accept a SQL file")
        bootstrap = cdc_catalog.catalog_paths(__file__)
        variables = cdc_catalog.variables_get(bootstrap["catalog"])
        paths = cdc_catalog.catalog_paths(__file__,variables=variables)
        return cdc_catalog.shell(
            paths["catalog"],paths["socket"],paths["seed"],
            command=None,
            file_path=args.sql_file if args.command == "sql" else None,
            auto_publish_file=True,
            publish_callback=validate_local_catalog_publish)

    if args.sql_file:
        parser.error("a SQL file is valid only with 'python j4.py sql <file.sql>'")

    if args.command == "selftest":
        if cdc_catalog.selftest():
            return 1
        test_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),"cdc_selftest.py")
        result = subprocess.run([sys.executable,test_path]).returncode
        if result:
            return result
        if online_config_available():
            cfg = read_config()
            activate_catalog(cfg)
            apply_resource_policy(cfg["resource"])
            prepared,_,_,_,_,_=preflight(
                cfg,create_missing=False,mapping_defs=mappings,
                plan_macros=cfg.get("catalog_macros"),
                plan_udfs=cfg.get("catalog_udfs"))
            validate_stateful_catalog_plan(
                cfg,
                dict(
                    version=int(cfg.get("catalog_version",0)),
                    stateful_tasks=list(
                        cfg.get("catalog_stateful_tasks",()) or ())),
                prepared,
                create_missing=False,
                allow_missing=False)
            log(
                "SELFTEST ONLINE CHECK OK: stateless/stateful schema and "
                "server checks passed; no target DDL or data was written")
        else:
            log(
                "SELFTEST ONLINE CHECK SKIPPED: persistent database "
                "connection configuration is incomplete")
        return result

    mode = args.command
    with (
            process_signals()
            if mode == "run"
            else contextlib.nullcontext(None)) as control:
        catalog_control = None
        try:
            if mode == "run":
                catalog_control = start_catalog_control_daemon(control)
                if not wait_catalog_control_ready(catalog_control,control):
                    log(f"STOP during bootstrap reason={control['reason']}")
                    return 0
                log(
                    "BOOTSTRAP READY: committed catalog is valid; "
                    "starting CDC preflight")
            cfg = read_config()
            activate_catalog(cfg)
            apply_resource_policy(cfg["resource"])
            prepared,source_uuid,start,start_gtid,available_logs,fingerprint = preflight(
                cfg,create_missing=mode not in ("check","probe"),
                mapping_defs=mappings,
                plan_macros=cfg.get("catalog_macros"),
                plan_udfs=cfg.get("catalog_udfs"))
            if mode in ("check","probe"):
                validate_stateful_catalog_plan(
                    cfg,
                    dict(
                        version=int(cfg.get("catalog_version",0)),
                        stateful_tasks=list(
                            cfg.get("catalog_stateful_tasks",()) or ())),
                    prepared,
                    create_missing=False,
                    allow_missing=False)
                log(
                    "CHECK OK: stateless/stateful schema and server checks "
                    "passed; no target DDL or data was written")
                return 0
            if control is not None and signal_stop_requested(control):
                log(f"STOP before startup reason={control['reason']}")
                return 0
            log("PREFLIGHT OK: schema and server checks passed; starting CDC")
            return run_cdc(
                cfg,prepared,source_uuid,start,start_gtid,
                available_logs,fingerprint,control,catalog_control)
        finally:
            if catalog_control is not None:
                stop_catalog_control(catalog_control)




if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        log(f"FAILED: {error}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

