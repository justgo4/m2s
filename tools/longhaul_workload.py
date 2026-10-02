#!/usr/bin/env python3
"""Fixed-resource MySQL -> j4 -> StarRocks long-haul workload driver.

This is the P11 workload generator, separate from longhaul_gate.py.  The
production certification profile is 50M initial rows, 50 newly inserted source
rows/s and 72h.  The driver keeps seed memory bounded, adds identical aggregate
and INNER JOIN tasks online to exercise both shared stateful runtimes, can
hard-kill/restart the daemon, and copies the daemon's final durable
summary/metrics into benchmark-results.

Use only disposable MySQL/StarRocks services; --isolated is mandatory.
"""
import argparse
import contextlib
import hashlib
import json
import os
import platform
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import cdc_catalog
import j4
import p11_profile
from starrocks_contract import configuration,execute,wait_ready
import process_resource_probe
import service_resource_probe


DATABASE="m2s_longhaul"
QUERY=(
    "SELECT id,bucket,v,payload "
    "FROM mysql.events"
)


def source_options():
    return dict(
        host=os.environ.get(
            "M2S_TEST_MYSQL_HOST","127.0.0.1"),
        port=int(os.environ.get(
            "M2S_TEST_MYSQL_PORT","3306")),
        user=os.environ.get(
            "M2S_TEST_MYSQL_USER","root"),
        password=os.environ.get(
            "M2S_TEST_MYSQL_PASSWORD",""),
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=5,
        read_timeout=60,
        write_timeout=60,
    )


def literal(value):
    return "'" + str(value).replace("'","''") + "'"


def percentile(values,p):
    values=sorted(float(value) for value in values)
    if not values:
        return None
    index=min(
        len(values)-1,
        max(0,int((len(values)-1)*float(p))))
    return values[index]



def interval_overlap_seconds(start,end,window_start,window_end):
    start=float(start)
    end=float(end)
    window_start=float(window_start)
    window_end=float(window_end)
    if end<start:
        raise ValueError("interval end precedes start")
    if window_end<window_start:
        raise ValueError("window end precedes start")
    return max(
        0.0,
        min(end,window_end)-max(start,window_start),
    )


def topology_resource_budget(
        memory_mb,cpu_cap,dynamic_tasks,load_mode,
        cpu_target=None,initial_sinks=3,
        requested_writer_max=8,
        requested_snapshot_workers=2,
        requested_duckdb_mb=128
):
    """Model daemon startup + every planned hot-add before a long run starts."""
    memory_mb=max(1,int(memory_mb))
    cpu_cap=max(1,int(cpu_cap))
    cpu_target=(
        cpu_cap
        if cpu_target is None
        else max(1,min(cpu_cap,int(cpu_target))))
    dynamic_tasks=max(0,int(dynamic_tasks))
    initial_sinks=max(1,int(initial_sinks))
    requested_writer_max=max(1,int(requested_writer_max))
    requested_snapshot_workers=max(
        0,int(requested_snapshot_workers))
    requested_duckdb_bytes=max(
        1,int(requested_duckdb_mb))*1024**2
    if load_mode not in {"merge_async","transaction"}:
        raise ValueError(
            "unsupported longhaul load mode: "+str(load_mode))

    snapshot_cap=max(
        1,min(
            2,
            cpu_target//2
            if cpu_target>1 else 1))
    snapshot_workers=min(
        requested_snapshot_workers,
        snapshot_cap)
    writer_max=min(
        requested_writer_max,
        max(1,cpu_cap//initial_sinks))
    duckdb_budget=max(
        1,memory_mb*1024**2//2)

    if load_mode=="merge_async":
        affordable_slots=max(
            1,
            duckdb_budget//requested_duckdb_bytes)
        fixed_slots=1+snapshot_workers
        memory_writer_cap=max(
            1,
            (affordable_slots-fixed_slots)
            //max(1,2*initial_sinks))
        writer_max=min(
            writer_max,memory_writer_cap)
    else:
        memory_writer_cap=writer_max

    startup_loader_slots=(
        writer_max*initial_sinks
        if load_mode=="merge_async"
        else initial_sinks)
    startup_engine_slots=max(
        1,
        1+snapshot_workers
        +startup_loader_slots*2)
    startup_cap=max(
        1,
        duckdb_budget//startup_engine_slots)
    startup_ok=not (
        requested_duckdb_bytes>startup_cap
        and startup_cap<32*1024**2)
    effective_duckdb_bytes=min(
        requested_duckdb_bytes,startup_cap)

    stages=[]
    for physical_sinks in range(
            initial_sinks,
            initial_sinks+dynamic_tasks+1):
        if load_mode=="merge_async":
            per_sink_writers=max(
                1,min(
                    writer_max,
                    cpu_target//physical_sinks))
            loader_slots=(
                per_sink_writers*physical_sinks)
        else:
            per_sink_writers=1
            loader_slots=physical_sinks
        engine_slots=max(
            1,
            1+snapshot_workers
            +loader_slots*2)
        per_engine_cap=max(
            1,duckdb_budget//engine_slots)
        admitted=(
            startup_ok
            if physical_sinks==initial_sinks
            else effective_duckdb_bytes<=per_engine_cap)
        stages.append(dict(
            physical_sinks=physical_sinks,
            writers_per_sink=per_sink_writers,
            engine_slots=engine_slots,
            per_engine_cap_mb=(
                per_engine_cap//1024**2),
            admitted=bool(admitted),
        ))

    failing=[
        item["physical_sinks"]
        for item in stages
        if not item["admitted"]
    ]
    return dict(
        ok=not failing,
        memory_mb=memory_mb,
        cpu_cap=cpu_cap,
        cpu_target=cpu_target,
        initial_physical_sinks=initial_sinks,
        final_physical_sinks=(
            initial_sinks+dynamic_tasks),
        dynamic_tasks=dynamic_tasks,
        load_mode=str(load_mode),
        snapshot_workers=snapshot_workers,
        writer_max=writer_max,
        memory_writer_cap=memory_writer_cap,
        requested_duckdb_mb=int(
            requested_duckdb_mb),
        effective_duckdb_mb=(
            effective_duckdb_bytes//1024**2),
        minimum_per_engine_cap_mb=min(
            item["per_engine_cap_mb"]
            for item in stages),
        failing_physical_sinks=failing,
        stages=stages,
    )


def runtime_topology_resource_preflight(args):
    """Fail before the 50M seed if the planned online topology cannot fit."""
    policy=j4.read_resource_policy()
    memory=j4.system_memory_stats()
    requested_memory=max(
        1,int(args.memory_mb))
    detected_memory=int(
        memory.get("total_mb") or 0)
    effective_memory=(
        min(requested_memory,detected_memory)
        if detected_memory>0
        else requested_memory)
    result=topology_resource_budget(
        effective_memory,
        int(policy["cpu_cap"]),
        int(args.dynamic_tasks),
        str(args.load_mode),
        # Auto cpu_target can rise when host load falls. Prove the topology
        # against the highest target allowed by the selected CPU cap.
        cpu_target=int(policy["cpu_cap"]),
    )
    result["requested_memory_mb"]=requested_memory
    result["detected_memory_mb"]=(
        detected_memory
        if detected_memory>0 else None)
    result["cpu_target_assumption"]="cpu_cap_worst_case"
    if (
        detected_memory>0
        and detected_memory<requested_memory
    ):
        result["ok"]=False
        result["memory_budget_available"]=False
    else:
        result["memory_budget_available"]=True
    if not result["ok"]:
        raise RuntimeError(
            "longhaul topology resource preflight failed before seed: "
            +json.dumps(
                result,sort_keys=True))
    return result


def _resource_file(path):
    try:
        return Path(path).read_text(
            encoding="utf-8").strip()
    except (OSError,UnicodeError):
        return None


def machine_resource_fingerprint():
    """Record reproducibility evidence without host/user/network identity."""
    cpu_max=_resource_file(
        "/sys/fs/cgroup/cpu.max")
    cpu_quota_cores=None
    if cpu_max:
        parts=cpu_max.split()
        if (
            len(parts)>=2
            and parts[0]!="max"
        ):
            try:
                quota=float(parts[0])
                period=float(parts[1])
                if quota>0 and period>0:
                    cpu_quota_cores=quota/period
            except ValueError:
                pass

    memory_max=_resource_file(
        "/sys/fs/cgroup/memory.max")
    cgroup_memory_limit_bytes=None
    if memory_max and memory_max!="max":
        try:
            value=int(memory_max)
            if value>0:
                cgroup_memory_limit_bytes=value
        except ValueError:
            pass

    return dict(
        architecture=str(platform.machine() or ""),
        system=str(platform.system() or ""),
        kernel_release=str(platform.release() or ""),
        python_version=str(platform.python_version() or ""),
        logical_cpus=int(os.cpu_count() or 0),
        cgroup_cpu_quota_cores=cpu_quota_cores,
        cgroup_memory_limit_bytes=(
            cgroup_memory_limit_bytes),
    )


def code_revision():
    """Return a reproducible code identity without exposing checkout paths."""
    for name in ("M2S_CODE_REVISION","GITHUB_SHA"):
        value=str(os.environ.get(name,"") or "").strip().lower()
        if (
            7<=len(value)<=64
            and all(
                char in "0123456789abcdef"
                for char in value)
        ):
            return value
    try:
        result=subprocess.run(
            ["git","rev-parse","HEAD"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=5,
            check=False)
    except (OSError,subprocess.SubprocessError):
        return ""
    value=str(result.stdout or "").strip().lower()
    if (
        result.returncode==0
        and 7<=len(value)<=64
        and all(
            char in "0123456789abcdef"
            for char in value)
    ):
        return value
    return ""


def code_worktree_clean():
    """Return clean/dirty without exposing repository paths or filenames."""
    try:
        result=subprocess.run(
            [
                "git","status","--porcelain",
                "--untracked-files=all",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=10,
            check=False)
    except (OSError,subprocess.SubprocessError):
        return None
    if result.returncode!=0:
        return None
    return not bool(
        str(result.stdout or "").strip())


def software_fingerprint(source,starrocks_version):
    """Capture versions/modes needed to reproduce one benchmark result."""
    with source.cursor() as cur:
        cur.execute(
            "SELECT VERSION(),@@GLOBAL.gtid_mode,"
            "@@GLOBAL.binlog_format,"
            "@@GLOBAL.binlog_row_image")
        row=cur.fetchone()
    if row is None or len(row)<4:
        raise RuntimeError(
            "MySQL software fingerprint is incomplete")
    return dict(
        code_revision=code_revision(),
        code_worktree_clean=code_worktree_clean(),
        mysql_version=str(row[0] or ""),
        mysql_gtid_mode=str(row[1] or "").upper(),
        mysql_binlog_format=str(row[2] or "").upper(),
        mysql_binlog_row_image=str(row[3] or "").upper(),
        starrocks_version=str(starrocks_version or ""),
        duckdb_version=str(
            getattr(j4.duckdb,"__version__","") or ""),
        pyarrow_version=str(
            getattr(j4.pa,"__version__","") or ""),
        pymysql_version=str(
            getattr(j4.pymysql,"__version__","") or ""),
        sqlglot_version=str(
            getattr(j4.sqlglot,"__version__","") or ""),
    )


def wait_create(cfg,ddl):
    deadline=time.monotonic()+120
    while True:
        try:
            execute(cfg,ddl)
            return
        except j4.pymysql.err.ProgrammingError as exc:
            if (
                "backends without enough disk space"
                not in str(exc).lower()
                or time.monotonic()>=deadline
            ):
                raise
            time.sleep(1)


def setup_databases(source,cfg,rows,seed_chunk):
    execute(
        cfg,"DROP DATABASE IF EXISTS "+DATABASE)
    execute(
        cfg,"CREATE DATABASE "+DATABASE)
    wait_create(cfg,(
        "CREATE TABLE "+DATABASE+".events("
        "id BIGINT NOT NULL,"
        "bucket INT NOT NULL,"
        "v BIGINT NOT NULL,"
        "payload VARCHAR(64) NULL"
        ") PRIMARY KEY(id) "
        "DISTRIBUTED BY HASH(id) BUCKETS 8 "
        'PROPERTIES("replication_num"="1")'
    ))
    wait_create(cfg,(
        "CREATE TABLE "+DATABASE+".agg_000("
        "bucket INT NOT NULL,"
        "n BIGINT NOT NULL,"
        "total LARGEINT NULL"
        ") PRIMARY KEY(bucket) "
        "DISTRIBUTED BY HASH(bucket) BUCKETS 4 "
        'PROPERTIES("replication_num"="1")'
    ))
    wait_create(cfg,(
        "CREATE TABLE "+DATABASE+".join_000("
        "_j4_pair_id VARCHAR(1024) NOT NULL,"
        "event_id BIGINT NULL,"
        "bucket INT NULL,"
        "label VARCHAR(64) NULL,"
        "v BIGINT NULL"
        ") PRIMARY KEY(_j4_pair_id) "
        "DISTRIBUTED BY HASH(_j4_pair_id) BUCKETS 8 "
        'PROPERTIES("replication_num"="1")'
    ))

    with source.cursor() as cur:
        cur.execute(
            "DROP DATABASE IF EXISTS "+DATABASE)
        cur.execute(
            "CREATE DATABASE "+DATABASE
            +" CHARACTER SET utf8mb4")
        cur.execute(
            "CREATE TABLE "+DATABASE+".events("
            "id BIGINT NOT NULL,"
            "bucket INT NOT NULL,"
            "v BIGINT NOT NULL,"
            "payload VARCHAR(64) NULL,"
            "PRIMARY KEY(id)"
            ") ENGINE=InnoDB")
        cur.execute(
            "CREATE TABLE "+DATABASE+".dimensions("
            "bucket INT NOT NULL,"
            "label VARCHAR(64) NOT NULL,"
            "PRIMARY KEY(bucket)"
            ") ENGINE=InnoDB")
        cur.executemany(
            "INSERT INTO "+DATABASE+".dimensions "
            "VALUES(%s,%s)",
            [
                (bucket,"dim-%04d" % bucket)
                for bucket in range(1024)
            ])
    source.commit()

    started=time.monotonic()
    inserted=0
    while inserted<int(rows):
        upper=min(
            int(rows),inserted+int(seed_chunk))
        batch=[
            (
                index,
                index%1024,
                index,
                "seed-%d" % (index%1000),
            )
            for index in range(inserted,upper)
        ]
        with source.cursor() as cur:
            cur.executemany(
                "INSERT INTO "+DATABASE+".events "
                "VALUES(%s,%s,%s,%s)",
                batch)
        source.commit()
        inserted=upper
        if inserted==rows or inserted%(seed_chunk*100)==0:
            print(
                "longhaul seed rows=%d/%d elapsed=%.1fs"
                % (
                    inserted,rows,
                    time.monotonic()-started),
                flush=True)
    return time.monotonic()-started


def setup_catalog(
        directory,cfg,source,mode,
        snapshot_rows,memory_mb,share_mode
):
    values=dict(
        CDC_MYSQL_HOST=source["host"],
        CDC_MYSQL_PORT=source["port"],
        CDC_MYSQL_USER=source["user"],
        CDC_MYSQL_PASSWORD=source["password"],
        CDC_MYSQL_SCHEMA=DATABASE,
        CDC_SR_FE_HOST=cfg["sr"]["host"],
        CDC_SR_FE_PORT=cfg["sr"]["http_port"],
        CDC_SR_QUERY_PORT=cfg["sr"]["port"],
        CDC_SR_USER=cfg["sr"]["user"],
        CDC_SR_PASSWORD=cfg["sr"]["password"],
        CDC_SR_DB=DATABASE,
        CDC_SERVER_ID=188690,
        CDC_STATE_FILE=str(
            directory/"state.sqlite3"),
        CDC_LOAD_MODE=mode,
        CDC_NATIVE_BINLOG_PATH=str(
            ROOT/"build/native/mysql_arrow_reader"),
        CDC_NATIVE_EVENT_GROUP_EVENTS=512,
        CDC_KEY_PARTITIONS=16,
        CDC_WRITE_WORKERS_MIN=1,
        CDC_WRITE_WORKERS_INITIAL=2,
        CDC_WRITE_WORKERS_MAX=8,
        CDC_ROWSET_YELLOW=500,
        CDC_ROWSET_RED=700,
        CDC_SNAPSHOT_WORKERS=2,
        CDC_SNAPSHOT_ROWS=int(snapshot_rows),
        CDC_SNAPSHOT_READ_AHEAD_GROUPS=2,
        CDC_SNAPSHOT_BUNDLE_MAX_LANES=16,
        CDC_COMMIT_INTERVAL_MS=500,
        CDC_BATCH_MS=100,
        CDC_QUERY_TIMEOUT=30,
        CDC_LOAD_TIMEOUT=120,
        CDC_STATUS_SECONDS=30,
        CDC_IDLE_STATUS_SECONDS=300,
        CDC_COMPRESSION="",
        CDC_DETAIL_LOGS=False,
        CDC_SHARED_SOURCE_STATE=True,
        CDC_RESOURCE_MEMORY_MB=int(memory_mb),
        CDC_DUCKDB_MEMORY="128MB",
        CDC_STATEFUL_SHARE_MODE=share_mode,
        CDC_STATEFUL_SHARE_MAX_FOLLOWERS=1000,
    )
    catalog=directory/"catalog.sqlite3"
    commands=[
        "SET VARIABLE "+key+" = "+literal(value)
        for key,value in values.items()
    ]
    commands.extend([
        "CREATE TABLE starrocks.events AS "+QUERY,
        (
            "CREATE TABLE starrocks.agg_000 AS "
            "SELECT bucket,COUNT(*) AS n,SUM(v) AS total "
            "FROM mysql.events GROUP BY bucket"
        ),
        (
            "CREATE TABLE starrocks.join_000 AS "
            "SELECT e.id AS event_id,e.bucket AS bucket,"
            "d.label AS label,e.v AS v "
            "FROM mysql.events e INNER JOIN mysql.dimensions d "
            "ON e.bucket=d.bucket"
        ),
    ])
    old_catalog=os.environ.get(
        "CDC_CATALOG_FILE")
    old_socket=os.environ.get(
        "CDC_CATALOG_SOCKET")
    os.environ["CDC_CATALOG_FILE"]=str(catalog)
    os.environ["CDC_CATALOG_SOCKET"]=str(
        directory/"control.sock")
    try:
        result=cdc_catalog.execute_batch(
            str(catalog),commands,
            publish_callback=j4.validate_local_catalog_publish)
    finally:
        if old_catalog is None:
            os.environ.pop(
                "CDC_CATALOG_FILE",None)
        else:
            os.environ["CDC_CATALOG_FILE"]=old_catalog
        if old_socket is None:
            os.environ.pop(
                "CDC_CATALOG_SOCKET",None)
        else:
            os.environ["CDC_CATALOG_SOCKET"]=old_socket
    if not result.get("publish"):
        raise RuntimeError(
            "longhaul catalog was not published")
    return catalog,dict(
        os.environ,
        CDC_CATALOG_FILE=str(catalog),
        CDC_CATALOG_SOCKET=str(
            directory/"control.sock"),
    )


def start_daemon(directory,env,index):
    log=directory/(
        "daemon-%03d.log" % int(index))
    handle=log.open("wb")
    proc=subprocess.Popen(
        [sys.executable,str(ROOT/"j4.py")],
        env=env,
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True)
    return proc,handle,log


def stop_daemon(proc,handle,kill=False):
    if proc is None:
        return
    if proc.poll() is None:
        os.killpg(
            proc.pid,
            signal.SIGKILL if kill
            else signal.SIGTERM)
    try:
        proc.wait(timeout=90)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid,signal.SIGKILL)
        proc.wait()
        raise RuntimeError(
            "longhaul daemon did not stop")
    finally:
        handle.close()
    if not kill and proc.returncode!=0:
        raise RuntimeError(
            "longhaul daemon graceful stop rc=%d"
            % proc.returncode)


def assert_live(proc,log):
    if proc.poll() is None:
        return
    detail=(
        log.read_text(errors="replace")
        if log.exists() else "")
    raise RuntimeError(
        "longhaul daemon exited rc=%d diagnostics=%s"
        % (
            proc.returncode,
            detail[-8000:],
        ))


def read_state(path):
    if not path.exists():
        return None
    try:
        con=sqlite3.connect(
            "file:"+str(path)+"?mode=ro",
            uri=True,timeout=2)
        try:
            pending=int(con.execute(
                "SELECT COUNT(*) FROM active_jobs"
            ).fetchone()[0])
            deliveries=int(con.execute(
                "SELECT COUNT(*) FROM deliveries"
            ).fetchone()[0])
            source=con.execute("""
                SELECT table_name,complete_seq
                FROM source_relations
                ORDER BY table_name
            """).fetchall()
            aggregate_tasks=con.execute("""
                SELECT sink_key,status
                FROM aggregate_task_descriptors
                ORDER BY sink_key
            """).fetchall()
            join_tasks=con.execute("""
                SELECT sink_key,status
                FROM join_task_descriptors
                ORDER BY sink_key
            """).fetchall()
            aggregate_shared=int(con.execute(
                "SELECT COUNT(*) "
                "FROM aggregate_shared_followers"
            ).fetchone()[0])
            join_shared=int(con.execute(
                "SELECT COUNT(*) "
                "FROM join_shared_followers"
            ).fetchone()[0])
            meta=dict(con.execute("""
                SELECT key,value FROM source_state_meta
                WHERE key IN(
                    'log_durable_seq',
                    'base_applied_seq')
            """).fetchall())
            return dict(
                pending=pending,
                deliveries=deliveries,
                source=[
                    (
                        str(row[0]),
                        None if row[1] is None
                        else int(row[1]),
                    )
                    for row in source
                ],
                aggregate_tasks=[
                    (str(row[0]),str(row[1]))
                    for row in aggregate_tasks
                ],
                join_tasks=[
                    (str(row[0]),str(row[1]))
                    for row in join_tasks
                ],
                aggregate_shared_followers=aggregate_shared,
                join_shared_followers=join_shared,
                shared_followers=(
                    aggregate_shared+join_shared),
                log_durable_seq=int(
                    meta.get("log_durable_seq",0)),
                base_applied_seq=int(
                    meta.get("base_applied_seq",0)),
            )
        finally:
            con.close()
    except sqlite3.Error:
        return None


def source_ready(value):
    if value is None or not value["source"]:
        return False
    return all(
        complete is not None
        for _,complete in value["source"])


def run_sql(directory,env,name,sql):
    path=directory/(str(name)+".sql")
    path.write_text(
        str(sql).rstrip()+"\n",
        encoding="utf-8")
    result=subprocess.run(
        [
            sys.executable,str(ROOT/"j4.py"),
            "sql",str(path),
        ],
        env=env,
        capture_output=True,
        timeout=180)
    if result.returncode:
        raise RuntimeError(
            "longhaul catalog SQL failed rc=%d output=%s"
            % (
                result.returncode,
                result.stdout.decode(
                    errors="replace")[-5000:],
            ))
    response=json.loads(
        result.stdout.decode())
    return (
        ((response.get("result") or {})
         .get("publish") or {})
        .get("activation") or {}
    )


def add_aggregate_task(
        directory,env,index
):
    sink="agg_%03d" % int(index)
    activation=run_sql(
        directory,env,"add-"+sink,
        (
            "CREATE TABLE starrocks."+sink+" AS "
            "SELECT bucket,COUNT(*) AS n,SUM(v) AS total "
            "FROM mysql.events GROUP BY bucket;"
        ))
    if activation.get("status") not in {
        "hot_pending",
        "deferred_until_snapshot_done",
        "deferred_until_previous_plan_drained",
    }:
        raise RuntimeError(
            "longhaul dynamic aggregate was not "
            "accepted online: "+repr(activation))
    return "starrocks."+sink


def add_join_task(
        directory,env,index
):
    sink="join_%03d" % int(index)
    activation=run_sql(
        directory,env,"add-"+sink,
        (
            "CREATE TABLE starrocks."+sink+" AS "
            "SELECT e.id AS event_id,e.bucket AS bucket,"
            "d.label AS label,e.v AS v "
            "FROM mysql.events e INNER JOIN mysql.dimensions d "
            "ON e.bucket=d.bucket;"
        ))
    if activation.get("status") not in {
        "hot_pending",
        "deferred_until_snapshot_done",
        "deferred_until_previous_plan_drained",
    }:
        raise RuntimeError(
            "longhaul dynamic JOIN was not "
            "accepted online: "+repr(activation))
    return "starrocks."+sink


def dynamic_task_kind(mix,index):
    mix=str(mix)
    index=int(index)
    if mix=="aggregate":
        return "aggregate"
    if mix=="join":
        return "join"
    if mix=="mixed":
        return "aggregate" if index%2 else "join"
    raise ValueError(
        "unsupported dynamic task mix: "+mix)


def wait_started(
        proc,log,state_path,timeout=180,
        progress=None
):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if progress is not None:
            progress()
        assert_live(proc,log)
        current=read_state(state_path)
        if current is not None:
            return current
        time.sleep(.2)
    raise RuntimeError(
        "longhaul daemon did not initialize state")


def visible_markers(cfg,marker_ids):
    marker_ids=[
        int(value) for value in marker_ids
    ]
    if not marker_ids:
        return set()
    result=set()
    for offset in range(0,len(marker_ids),512):
        chunk=marker_ids[offset:offset+512]
        rows,_=execute(
            cfg,
            "SELECT id FROM "+DATABASE+".events "
            "WHERE id IN ("
            +",".join(str(value) for value in chunk)
            +")")
        result.update(
            int(row[0]) for row in rows)
    return result


def recover_after_fault(
        proc,log,state_path,cfg,commit_times,
        timeout_seconds,progress=None
):
    pending=set(
        int(value) for value in commit_times)
    started=time.monotonic()
    deadline=started+float(timeout_seconds)
    recorded=[]
    while time.monotonic()<deadline:
        if progress is not None:
            progress()
        pending.update(
            int(value) for value in commit_times)
        assert_live(proc,log)
        visible=visible_markers(
            cfg,pending)
        if visible:
            observed=time.monotonic()
            for marker in visible:
                committed=commit_times.pop(
                    marker,None)
                pending.discard(marker)
                if committed is not None:
                    recorded.append(
                        observed-committed)
        current=read_state(state_path)
        if (
            not pending
            and current is not None
            and current["log_durable_seq"]
                ==current["base_applied_seq"]
        ):
            return dict(
                seconds=time.monotonic()-started,
                latencies=recorded,
                state=current,
            )
        time.sleep(.2)
    raise RuntimeError(
        "longhaul fault recovery timed out "
        "pending_markers=%d state=%r"
        % (len(pending),read_state(state_path)))


def mutate_join_right(source,index,initial_rows):
    """Commit one deterministic right-side JOIN update while the daemon is down."""
    index=max(1,int(index))
    bucket=(index-1)%max(
        1,min(1024,int(initial_rows)))
    label="fault-%06d-bucket-%04d" % (
        index,bucket)
    with source.cursor() as cur:
        cur.execute(
            "UPDATE "+DATABASE+".dimensions "
            "SET label=%s WHERE bucket=%s",
            (label,bucket))
        if int(cur.rowcount)!=1:
            raise RuntimeError(
                "longhaul right-side JOIN mutation "
                "did not update exactly one dimension row")
    source.commit()
    return dict(
        bucket=int(bucket),
        revision=int(index),
        label=label,
    )


def source_target_totals(source,cfg):
    with source.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*),SUM(v) "
            "FROM "+DATABASE+".events")
        expected=cur.fetchone()
    actual,_=execute(
        cfg,
        "SELECT COUNT(*),SUM(v) "
        "FROM "+DATABASE+".events")
    return (
        (int(expected[0]),int(expected[1] or 0)),
        (int(actual[0][0]),int(actual[0][1] or 0)),
    )


def _aggregate_rows_source(source):
    with source.cursor() as cur:
        cur.execute(
            "SELECT bucket,COUNT(*),SUM(v) "
            "FROM "+DATABASE+".events "
            "GROUP BY bucket ORDER BY bucket")
        return [
            (
                int(row[0]),
                int(row[1]),
                int(row[2] or 0),
            )
            for row in cur.fetchall()
        ]


def _aggregate_rows_target(cfg,table):
    rows,_=execute(
        cfg,
        "SELECT bucket,n,total FROM "
        +DATABASE+"."+str(table)
        +" ORDER BY bucket")
    return [
        (
            int(row[0]),
            int(row[1]),
            int(row[2] or 0),
        )
        for row in rows
    ]


def _rows_digest(rows):
    payload=json.dumps(
        list(rows),
        ensure_ascii=False,
        separators=(",",":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _partition_digest_update(hasher,partition,rows):
    digest=_rows_digest(rows)
    hasher.update(
        ("%d:%d:%s\n" % (
            int(partition),len(rows),digest
        )).encode("ascii"))
    return digest


def _partitioned_exactness(
        source,cfg,tables,source_rows,target_rows,
        source_total_rows,target_total_rows,
        partitions=None
):
    """Compare full projected rows and prove the partition scan covered all rows."""
    partitions=list(
        range(1024)
        if partitions is None
        else partitions
    )
    partitions=[
        int(value) for value in partitions
    ]
    if not partitions:
        raise ValueError(
            "exactness partitions cannot be empty")
    if len(partitions)!=len(set(partitions)):
        raise ValueError(
            "exactness partitions must be unique")

    expected_hasher=hashlib.sha256()
    expected_rows=0
    checks={
        str(table):dict(
            rows=0,
            hasher=hashlib.sha256(),
            mismatches=[],
        )
        for table in tables
    }
    for partition in partitions:
        expected=list(
            source_rows(source,partition))
        expected_digest=_partition_digest_update(
            expected_hasher,partition,expected)
        expected_rows+=len(expected)
        for table in tables:
            key=str(table)
            actual=list(
                target_rows(
                    cfg,table,partition))
            actual_digest=_partition_digest_update(
                checks[key]["hasher"],
                partition,actual)
            checks[key]["rows"]+=len(actual)
            if actual!=expected and len(
                checks[key]["mismatches"]
            )<8:
                checks[key]["mismatches"].append(
                    dict(
                        partition=partition,
                        expected_rows=len(expected),
                        actual_rows=len(actual),
                        expected_digest=expected_digest,
                        actual_digest=actual_digest,
                        expected_sample=expected[:3],
                        actual_sample=actual[:3],
                    ))

    expected_digest=expected_hasher.hexdigest()
    source_total=int(
        source_total_rows(source))
    source_uncovered=(
        source_total-int(expected_rows))
    tables_out={}
    for table,value in checks.items():
        digest=value["hasher"].hexdigest()
        total_rows=int(
            target_total_rows(cfg,table))
        uncovered_rows=(
            total_rows-int(value["rows"]))
        tables_out[table]=dict(
            rows=int(value["rows"]),
            total_rows=total_rows,
            uncovered_rows=uncovered_rows,
            digest=digest,
            match=(
                source_uncovered==0
                and uncovered_rows==0
                and not value["mismatches"]
                and int(value["rows"])==expected_rows
                and digest==expected_digest
            ),
            mismatches=value["mismatches"],
        )
    coverage_complete=(
        source_uncovered==0
        and bool(tables_out)
        and all(
            int(item["uncovered_rows"])==0
            for item in tables_out.values()
        )
    )
    return dict(
        comparison="partitioned_full_rows_v2",
        partitions=len(partitions),
        expected_rows=int(expected_rows),
        source_total_rows=source_total,
        source_uncovered_rows=source_uncovered,
        expected_digest=expected_digest,
        coverage_complete=coverage_complete,
        tables=tables_out,
        all_match=(
            coverage_complete
            and all(
                item["match"]
                for item in tables_out.values()
            )
        ),
    )


def _event_rows_source(source,bucket):
    with source.cursor() as cur:
        cur.execute(
            "SELECT id,bucket,v,payload "
            "FROM "+DATABASE+".events "
            "WHERE bucket=%s ORDER BY id",
            (int(bucket),))
        return [
            (
                int(row[0]),int(row[1]),
                int(row[2]),
                None if row[3] is None else str(row[3]),
            )
            for row in cur.fetchall()
        ]


def _event_rows_target(cfg,table,bucket):
    rows,_=execute(
        cfg,
        "SELECT id,bucket,v,payload FROM "
        +DATABASE+"."+str(table)
        +" WHERE bucket="+str(int(bucket))
        +" ORDER BY id")
    return [
        (
            int(row[0]),int(row[1]),
            int(row[2]),
            None if row[3] is None else str(row[3]),
        )
        for row in rows
    ]


def _event_source_total_rows(source):
    with source.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM "
            +DATABASE+".events")
        return int(cur.fetchone()[0])


def _target_total_rows(cfg,table):
    rows,_=execute(
        cfg,
        "SELECT COUNT(*) FROM "
        +DATABASE+"."+str(table))
    if len(rows)!=1:
        raise RuntimeError(
            "exactness target count returned "
            +str(len(rows))+" rows")
    return int(rows[0][0])


def event_exactness(
        source,cfg,tables=("events",),partitions=None
):
    return _partitioned_exactness(
        source,cfg,tables,
        _event_rows_source,_event_rows_target,
        _event_source_total_rows,_target_total_rows,
        partitions=partitions)


def aggregate_exactness(source,cfg,tables):
    expected=_aggregate_rows_source(source)
    expected_digest=_rows_digest(expected)
    checks={}
    for table in tables:
        actual=_aggregate_rows_target(
            cfg,table)
        digest=_rows_digest(actual)
        checks[str(table)]=dict(
            rows=len(actual),
            digest=digest,
            match=(
                len(actual)==len(expected)
                and digest==expected_digest
                and actual==expected
            ),
        )
    return dict(
        expected_rows=len(expected),
        expected_digest=expected_digest,
        tables=checks,
        all_match=all(
            item["match"] for item in checks.values()
        ),
    )


def _join_rows_source(source,bucket):
    with source.cursor() as cur:
        cur.execute(
            "SELECT e.id,e.bucket,d.label,e.v "
            "FROM "+DATABASE+".events e "
            "INNER JOIN "+DATABASE+".dimensions d "
            "ON e.bucket=d.bucket "
            "WHERE e.bucket=%s ORDER BY e.id",
            (int(bucket),))
        return [
            (
                int(row[0]),int(row[1]),
                str(row[2]),int(row[3]),
            )
            for row in cur.fetchall()
        ]


def _join_rows_target(cfg,table,bucket):
    rows,_=execute(
        cfg,
        "SELECT event_id,bucket,label,v FROM "
        +DATABASE+"."+str(table)
        +" WHERE bucket="+str(int(bucket))
        +" ORDER BY event_id")
    return [
        (
            int(row[0]),int(row[1]),
            str(row[2]),int(row[3]),
        )
        for row in rows
    ]


def _join_source_total_rows(source):
    with source.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) "
            "FROM "+DATABASE+".events e "
            "INNER JOIN "+DATABASE+".dimensions d "
            "ON e.bucket=d.bucket")
        return int(cur.fetchone()[0])


def join_exactness(
        source,cfg,tables,partitions=None
):
    return _partitioned_exactness(
        source,cfg,tables,
        _join_rows_source,_join_rows_target,
        _join_source_total_rows,_target_total_rows,
        partitions=partitions)


def collect_final_debt(state_path):
    state_path=Path(state_path)
    summary_path=Path(
        str(state_path)+".summary.json")
    if not summary_path.exists():
        raise RuntimeError(
            "longhaul final daemon summary is missing")
    summary=json.loads(
        summary_path.read_text(
            encoding="utf-8"))
    if summary.get("event")!="run_summary":
        raise RuntimeError(
            "longhaul final daemon summary is not run_summary")

    tables=dict(summary.get("tables") or {})
    per_table_rowset={}
    per_table_version_recovery={}
    for table,value in sorted(tables.items()):
        per_table_rowset[str(table)]=int(
            (value or {}).get("max_rowset",-1))
        per_table_version_recovery[str(table)]=bool(
            (value or {}).get("version_recovery",False))
    max_rowset=(
        max(per_table_rowset.values())
        if per_table_rowset else -1)

    stateful=dict(summary.get("stateful") or {})
    physical=dict(stateful.get("physical") or {})
    sizes=dict(physical.get("sizes") or {})
    stateful_rows=0
    stateful_payload_bytes=0
    for value in sizes.values():
        value=dict(value or {})
        stateful_rows+=int(value.get("rows",0))
        stateful_payload_bytes+=int(
            value.get("payload_bytes",0))

    state_storage_bytes=0
    for suffix in ("","-wal","-shm"):
        candidate=Path(str(state_path)+suffix)
        if candidate.exists():
            state_storage_bytes+=candidate.stat().st_size

    state=dict(summary.get("state") or {})
    return dict(
        state_storage_bytes=int(
            state_storage_bytes),
        stateful_rows=int(stateful_rows),
        stateful_payload_bytes=int(
            stateful_payload_bytes),
        max_rowset=int(max_rowset),
        rowset_red=700,
        per_table_max_rowset=per_table_rowset,
        per_table_version_recovery=per_table_version_recovery,
        version_recovery_active=any(
            per_table_version_recovery.values()),
        pending_bytes=int(
            state.get("pending_bytes",0)),
        prepared_budget_used=int(
            state.get("prepared_budget_used",0)),
        field_overflow_rows=int(
            state.get("field_overflow_rows",0)),
    )



@contextlib.contextmanager
def work_directory(path=None):
    """Use a disposable workspace unless an explicit empty directory is supplied.

    Formal 72h runs can pass --work-directory so SQLite state, daemon logs and
    catalog artifacts survive a workload-driver failure. Existing non-empty
    directories are rejected because this harness drops and recreates its
    isolated databases and must never silently reuse stale run state.
    """
    if path is None:
        with tempfile.TemporaryDirectory(
            prefix="m2s-longhaul-"
        ) as td:
            yield Path(td)
        return

    directory=Path(path).expanduser().resolve()
    if directory.exists():
        if not directory.is_dir():
            raise ValueError(
                "longhaul work directory is not a directory")
        if any(directory.iterdir()):
            raise RuntimeError(
                "longhaul work directory must be empty: "
                +str(directory))
    else:
        directory.mkdir(
            parents=True,exist_ok=False)
    (directory/".m2s-longhaul-workdir").write_text(
        "format_version=1\n",
        encoding="utf-8")
    yield directory


def write_checkpoint(output,payload):
    output=Path(output)
    path=output.with_name(
        output.stem+"-checkpoint.json")
    path.parent.mkdir(
        parents=True,exist_ok=True)
    temporary=path.with_name(
        path.name+".tmp")
    data=json.dumps(
        payload,indent=2,
        sort_keys=True)+"\n"
    with temporary.open(
        "w",encoding="utf-8"
    ) as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary,path)
    return path


def copy_evidence(directory,output):
    state_path=directory/"state.sqlite3"
    summary=Path(
        str(state_path)+".summary.json")
    metrics=Path(
        str(state_path)+".metrics.jsonl")
    output.parent.mkdir(
        parents=True,exist_ok=True)
    copied={}
    if summary.exists():
        target=output.parent/(
            "longhaul-daemon-summary.json")
        shutil.copyfile(summary,target)
        copied["daemon_summary"]=str(target)
    if metrics.exists():
        target=output.parent/(
            "longhaul-daemon-metrics.jsonl")
        shutil.copyfile(metrics,target)
        copied["daemon_metrics"]=str(target)
    return copied


def run(args):
    cfg=configuration()
    cfg["sr"]["database"]=DATABASE
    starrocks_version=wait_ready(cfg)
    opts=source_options()
    source=j4.pymysql.connect(**opts)
    software=software_fingerprint(
        source,starrocks_version)
    if (
        getattr(args,"certification_profile",False)
        and software.get("code_worktree_clean") is not True
    ):
        source.close()
        raise RuntimeError(
            "formal P11 certification requires a clean git worktree")
    topology_preflight=runtime_topology_resource_preflight(
        args)
    print(
        "longhaul topology resource preflight "
        +json.dumps(
            topology_preflight,
            sort_keys=True),
        flush=True)
    proc=handle=log=None
    try:
        seed_seconds=setup_databases(
            source,cfg,args.rows,args.seed_chunk)

        mysql_selection=service_resource_probe.resolve_service_pid(
            explicit_pid=getattr(args,"mysql_resource_pid",None),
            port=opts["port"])
        starrocks_fe_selection=service_resource_probe.resolve_service_pid(
            explicit_pid=getattr(args,"starrocks_fe_resource_pid",None),
            port=cfg["sr"]["port"])
        starrocks_be_selection=service_resource_probe.resolve_service_pid(
            explicit_pid=getattr(args,"starrocks_be_resource_pid",None),
            port=int(getattr(
                args,"starrocks_be_resource_port",
                os.environ.get(
                    "M2S_TEST_STARROCKS_BE_HTTP_PORT","8040"))))
        service_selections=dict(
            mysql=mysql_selection,
            starrocks_fe=starrocks_fe_selection,
            starrocks_be=starrocks_be_selection,
        )
        service_trackers={
            name:service_resource_probe.new_tracker(selection)
            for name,selection in service_selections.items()
        }

        with work_directory(
            getattr(args,"work_directory",None)
        ) as directory:
            catalog,env=setup_catalog(
                directory,cfg,opts,args.load_mode,
                args.snapshot_rows,args.memory_mb,
                args.share_mode)
            state_path=directory/"state.sqlite3"
            resource_tracker=process_resource_probe.new_tracker()
            next_resource_sample=0.0

            def sample_daemon_resources(force=False):
                nonlocal next_resource_sample
                now=time.monotonic()
                if (
                    not force
                    and now<next_resource_sample
                ):
                    return
                if proc is not None:
                    process_resource_probe.observe(
                        resource_tracker,
                        process_resource_probe.process_tree_sample(
                            proc.pid))
                for name,selection in service_selections.items():
                    pid=selection.get("pid")
                    service_resource_probe.observe(
                        service_trackers[name],
                        (
                            None
                            if pid is None
                            else service_resource_probe.service_sample(
                                int(pid))
                        ))
                next_resource_sample=now+1.0

            proc,handle,log=start_daemon(
                directory,env,1)
            wait_started(
                proc,log,state_path,
                progress=sample_daemon_resources)
            sample_daemon_resources(force=True)

            started=time.monotonic()
            deadline=started+float(
                args.duration_seconds)
            next_tick=started
            next_sample=started
            next_fault=(
                started+args.fault_every_seconds
                if args.fault_every_seconds>0
                else None
            )
            task_due=[
                started
                +(
                    (index+1)
                    *float(args.duration_seconds)
                    /(args.dynamic_tasks+1)
                )
                for index in range(
                    args.dynamic_tasks)
            ]
            task_ready={}
            task_publish={}
            added_tasks=[]
            marker_start=int(args.rows)
            sequence=0
            commit_times={}
            latency=[]
            recovery_latency=[]
            daemon_index=1
            faults=[]
            fault_unavailable_seconds=0.0
            source_ready_at=None
            next_checkpoint=started

            def deploy_due_tasks(
                    current,now=None,force=False
            ):
                if not source_ready(current):
                    return 0
                now=(
                    time.monotonic()
                    if now is None else float(now))
                deployed=0
                while (
                    task_due
                    and (force or now>=task_due[0])
                ):
                    task_due.pop(0)
                    task_index=len(
                        added_tasks)+1
                    kind=dynamic_task_kind(
                        args.dynamic_task_mix,
                        task_index)
                    sink_key=(
                        "starrocks.agg_%03d"
                        % task_index
                        if kind=="aggregate"
                        else "starrocks.join_%03d"
                        % task_index)
                    task_publish[sink_key]=(
                        time.monotonic())
                    sink=(
                        add_aggregate_task(
                            directory,env,task_index)
                        if kind=="aggregate"
                        else add_join_task(
                            directory,env,task_index)
                    )
                    if sink!=sink_key:
                        raise RuntimeError(
                            "longhaul dynamic "+kind+" "
                            "sink identity changed")
                    added_tasks.append(sink)
                    deployed+=1
                return deployed

            def observe_task_ready(current):
                statuses={}
                if current is not None:
                    statuses.update(dict(
                        current["aggregate_tasks"]))
                    statuses.update(dict(
                        current["join_tasks"]))
                observed=time.monotonic()
                for sink in added_tasks:
                    if (
                        sink not in task_ready
                        and statuses.get(sink)=="active"
                    ):
                        task_ready[sink]=(
                            observed
                            -task_publish[sink]
                        )
                return statuses

            def persist_checkpoint(now=None,force=False):
                nonlocal next_checkpoint
                interval=float(
                    getattr(args,"checkpoint_seconds",300.0))
                if interval<=0 and not force:
                    return None
                now=(
                    time.monotonic()
                    if now is None else float(now))
                if not force and now<next_checkpoint:
                    return None
                sample_daemon_resources(force=True)
                payload=dict(
                    format_version=1,
                    kind="m2s_longhaul_checkpoint",
                    complete=False,
                    workload_profile=(
                        p11_profile.NAME
                        if getattr(
                            args,"certification_profile",False)
                        else "custom"),
                    protocol=args.load_mode,
                    initial_rows=int(args.rows),
                    rows_per_second=int(
                        args.rows_per_second),
                    duration_seconds=max(
                        0.0,now-started),
                    memory_mb=int(args.memory_mb),
                    topology_resource_preflight=topology_preflight,
                    work_directory_persistent=bool(
                        getattr(
                            args,"work_directory",None)),
                    code_revision=str(
                        software.get(
                            "code_revision") or ""),
                    live_rows=int(sequence),
                    latency_samples=len(latency),
                    latency_p95_seconds=percentile(
                        latency,.95),
                    latency_p99_seconds=percentile(
                        latency,.99),
                    recovery_latency_samples=len(
                        recovery_latency),
                    dynamic_tasks_requested=int(
                        args.dynamic_tasks),
                    dynamic_task_mix=str(
                        args.dynamic_task_mix),
                    dynamic_tasks_added=len(
                        added_tasks),
                    dynamic_tasks_ready=len(
                        task_ready),
                    faults=list(faults),
                    current_state=read_state(
                        state_path),
                    daemon_resources=(
                        process_resource_probe.report(
                            resource_tracker)),
                    service_resources={
                        name:service_resource_probe.report(
                            tracker)
                        for name,tracker in sorted(
                            service_trackers.items())
                    },
                )
                path=write_checkpoint(
                    args.output,payload)
                next_checkpoint=(
                    now+interval
                    if interval>0
                    else float("inf"))
                return path

            persist_checkpoint(
                started,force=True)

            def pump_source(now=None):
                nonlocal sequence,next_tick
                now=(
                    time.monotonic()
                    if now is None else float(now))
                produced=0
                while (
                    now>=next_tick
                    and next_tick<deadline
                ):
                    count=max(
                        1,int(args.rows_per_second))
                    rows=[]
                    for _ in range(count):
                        marker=marker_start+sequence
                        rows.append((
                            marker,
                            marker%1024,
                            marker,
                            "live-%d" % sequence,
                        ))
                        sequence+=1
                    with source.cursor() as cur:
                        cur.executemany(
                            "INSERT INTO "
                            +DATABASE+".events "
                            "VALUES(%s,%s,%s,%s)",
                            rows)
                    source.commit()
                    committed=time.monotonic()
                    sentinel=int(rows[-1][0])
                    commit_times[sentinel]=committed
                    next_tick+=1.0
                    produced+=len(rows)
                return produced

            while time.monotonic()<deadline:
                now=time.monotonic()
                assert_live(proc,log)
                sample_daemon_resources()
                persist_checkpoint(now)
                current=read_state(
                    state_path)
                if (
                    source_ready_at is None
                    and source_ready(current)
                ):
                    source_ready_at=now-started

                deploy_due_tasks(
                    current,now=now)
                observe_task_ready(current)

                if (
                    next_fault is not None
                    and now>=next_fault
                ):
                    before=sequence
                    fault_started=time.monotonic()
                    sample_daemon_resources(force=True)
                    stop_daemon(
                        proc,handle,kill=True)
                    proc=handle=log=None
                    right_update=None
                    if args.dynamic_task_mix in {
                        "mixed","join"
                    }:
                        right_update=mutate_join_right(
                            source,
                            len(faults)+1,
                            args.rows)
                    daemon_index+=1
                    proc,handle,log=start_daemon(
                        directory,env,daemon_index)

                    def fault_progress():
                        pump_source()
                        sample_daemon_resources()

                    wait_started(
                        proc,log,state_path,
                        progress=fault_progress)
                    sample_daemon_resources(force=True)
                    restart_seconds=(
                        time.monotonic()-fault_started)
                    recovered=recover_after_fault(
                        proc,log,state_path,cfg,
                        commit_times,
                        args.fault_recovery_timeout_seconds,
                        progress=fault_progress)
                    fault_recovered_at=time.monotonic()
                    fault_unavailable_seconds+=interval_overlap_seconds(
                        fault_started,fault_recovered_at,
                        started,deadline)
                    sample_daemon_resources(force=True)
                    recovery_latency.extend(
                        recovered["latencies"])
                    faults.append(dict(
                        sequence=before,
                        sequence_after=int(sequence),
                        source_rows_during_fault=int(
                            sequence-before),
                        restart_seconds=restart_seconds,
                        catchup_seconds=recovered["seconds"],
                        join_right_update=(
                            None
                            if right_update is None
                            else dict(
                                bucket=right_update[
                                    "bucket"],
                                revision=right_update[
                                    "revision"],
                            )
                        ),
                        source_frontier=dict(
                            log_durable_seq=recovered[
                                "state"]["log_durable_seq"],
                            base_applied_seq=recovered[
                                "state"]["base_applied_seq"],
                        ),
                    ))
                    current=recovered["state"]
                    # A long recovery can cross one or more dynamic-task due
                    # times. Deploy them immediately after catch-up instead of
                    # silently losing them when wall time has passed deadline.
                    deploy_due_tasks(
                        current,now=time.monotonic())
                    observe_task_ready(current)
                    persist_checkpoint(force=True)
                    next_fault=(
                        time.monotonic()
                        +args.fault_every_seconds)

                pump_source(now)

                if now>=next_sample:
                    visible=visible_markers(
                        cfg,commit_times.keys())
                    if visible:
                        observed=time.monotonic()
                        for marker in visible:
                            committed=commit_times.pop(
                                marker,None)
                            if committed is not None:
                                latency.append(
                                    observed-committed)
                    next_sample=now+float(
                        args.sample_seconds)

                sleep_for=min(
                    .1,
                    max(
                        0.0,
                        min(
                            next_tick,
                            next_sample,
                            deadline,
                        )-time.monotonic(),
                    ),
                )
                if sleep_for:
                    time.sleep(sleep_for)

            # Drain every committed marker and online task before graceful stop.
            drain_deadline=time.monotonic()+float(
                args.drain_timeout_seconds)
            while time.monotonic()<drain_deadline:
                assert_live(proc,log)
                sample_daemon_resources()
                persist_checkpoint()
                current=read_state(
                    state_path)
                # Drain is after the scheduled workload window. Any task whose
                # deployment was delayed by a restart/catch-up must still be
                # created and brought to active before final evidence is taken.
                deploy_due_tasks(
                    current,
                    now=time.monotonic(),
                    force=True)
                visible=visible_markers(
                    cfg,commit_times.keys())
                if visible:
                    observed=time.monotonic()
                    for marker in visible:
                        committed=commit_times.pop(
                            marker,None)
                        if committed is not None:
                            latency.append(
                                observed-committed)
                statuses=observe_task_ready(
                    current)
                tasks_ready=all(
                    statuses.get(sink)=="active"
                    for sink in added_tasks)
                if (
                    not task_due
                    and len(added_tasks)==int(
                        args.dynamic_tasks)
                    and not commit_times
                    and current is not None
                    and current["pending"]==0
                    and current["deliveries"]==0
                    and current["log_durable_seq"]
                        ==current["base_applied_seq"]
                    and tasks_ready
                ):
                    break
                time.sleep(.5)
            else:
                raise RuntimeError(
                    "longhaul drain timed out state="
                    +repr(read_state(state_path)))

            expected,actual=source_target_totals(
                source,cfg)
            if expected!=actual:
                raise AssertionError(
                    "longhaul source/target totals differ "
                    "expected=%r actual=%r"
                    % (expected,actual))
            event_checks=event_exactness(
                source,cfg,["events"])
            if not event_checks["all_match"]:
                raise AssertionError(
                    "longhaul raw event target differs from source "
                    +repr(event_checks))
            aggregate_checks=aggregate_exactness(
                source,cfg,
                ["agg_000"]+[
                    sink.split(".",1)[1]
                    for sink in added_tasks
                    if sink.startswith(
                        "starrocks.agg_")
                ])
            if not aggregate_checks["all_match"]:
                raise AssertionError(
                    "longhaul aggregate targets differ from source "
                    +repr(aggregate_checks))
            join_checks=join_exactness(
                source,cfg,
                ["join_000"]+[
                    sink.split(".",1)[1]
                    for sink in added_tasks
                    if sink.startswith(
                        "starrocks.join_")
                ])
            if not join_checks["all_match"]:
                raise AssertionError(
                    "longhaul JOIN targets differ from source "
                    +repr(join_checks))

            sample_daemon_resources(force=True)
            persist_checkpoint(force=True)
            stop_daemon(
                proc,handle,kill=False)
            proc=handle=log=None
            debt=collect_final_debt(
                state_path)
            copied=copy_evidence(
                directory,args.output)
            final_state=read_state(
                state_path)
            elapsed=time.monotonic()-started
            report=dict(
                format_version=1,
                kind="m2s_longhaul_workload",
                workload_profile=(
                    p11_profile.NAME
                    if getattr(
                        args,"certification_profile",False)
                    else "custom"),
                protocol=args.load_mode,
                initial_rows=int(args.rows),
                rows_per_second=int(
                    args.rows_per_second),
                duration_seconds=elapsed,
                source_schedule_seconds=float(
                    args.duration_seconds),
                healthy_observation_seconds=max(
                    0.0,
                    float(args.duration_seconds)
                    -fault_unavailable_seconds),
                fault_unavailable_seconds=float(
                    fault_unavailable_seconds),
                memory_mb=int(args.memory_mb),
                topology_resource_preflight=topology_preflight,
                work_directory_persistent=bool(
                    getattr(args,"work_directory",None)),
                resource_fingerprint=machine_resource_fingerprint(),
                software_fingerprint=software,
                daemon_resources=process_resource_probe.report(
                    resource_tracker),
                service_resources={
                    name:service_resource_probe.report(tracker)
                    for name,tracker in sorted(
                        service_trackers.items())
                },
                snapshot_rows=int(args.snapshot_rows),
                sample_seconds=float(args.sample_seconds),
                fault_every_seconds=float(
                    args.fault_every_seconds),
                fault_recovery_timeout_seconds=float(
                    args.fault_recovery_timeout_seconds),
                seed_chunk=int(args.seed_chunk),
                drain_timeout_seconds=float(
                    args.drain_timeout_seconds),
                checkpoint_seconds=float(
                    args.checkpoint_seconds),
                seed_seconds=seed_seconds,
                source_ready_seconds=source_ready_at,
                live_rows=int(sequence),
                latency_samples=len(latency),
                latency_p50_seconds=percentile(
                    latency,.50),
                latency_p95_seconds=percentile(
                    latency,.95),
                latency_p99_seconds=percentile(
                    latency,.99),
                latency_max_seconds=(
                    max(latency)
                    if latency else None),
                latency_over_5_seconds=sum(
                    1 for value in latency
                    if float(value)>5.0),
                latency_over_10_seconds=sum(
                    1 for value in latency
                    if float(value)>10.0),
                recovery_latency_samples=len(
                    recovery_latency),
                recovery_latency_p95_seconds=percentile(
                    recovery_latency,.95),
                recovery_latency_p99_seconds=percentile(
                    recovery_latency,.99),
                recovery_latency_max_seconds=(
                    max(recovery_latency)
                    if recovery_latency else None),
                dynamic_tasks=int(
                    args.dynamic_tasks),
                dynamic_task_mix=str(
                    args.dynamic_task_mix),
                dynamic_task_ready_seconds=dict(
                    sorted(task_ready.items())),
                join_right_updates=sum(
                    1 for item in faults
                    if item.get("join_right_update")
                    is not None),
                faults=faults,
                final_state=final_state,
                debt=debt,
                source_totals=expected,
                target_totals=actual,
                event_checks=event_checks,
                aggregate_checks=aggregate_checks,
                join_checks=join_checks,
                share_mode=args.share_mode,
                evidence=copied,
                catalog_version=cdc_catalog.load_plan(
                    str(catalog))["version"],
            )
            args.output.parent.mkdir(
                parents=True,exist_ok=True)
            args.output.write_text(
                json.dumps(
                    report,indent=2,
                    sort_keys=True)+"\n",
                encoding="utf-8")
            print(
                json.dumps(
                    report,sort_keys=True),
                flush=True)
            return report
    finally:
        if proc is not None:
            try:
                stop_daemon(
                    proc,handle,kill=True)
            except Exception:
                pass
        source.close()


def certification_mismatches(args):
    mismatch=dict(
        p11_profile.mismatches(args))
    if getattr(args,"work_directory",None) is None:
        mismatch["work_directory"]=dict(
            expected="explicit persistent empty directory",
            actual=None,
        )
    return mismatch


def main():
    parser=argparse.ArgumentParser(
        description=__doc__)
    parser.add_argument(
        "--isolated",action="store_true")
    parser.add_argument(
        "--certification-profile",
        action="store_true",
        help=(
            "require every workload knob to match the canonical "
            +p11_profile.NAME+" profile exactly"))
    parser.add_argument(
        "--load-mode",
        choices=("merge_async","transaction"),
        default="merge_async")
    parser.add_argument(
        "--rows",type=int,
        default=50_000_000)
    parser.add_argument(
        "--rows-per-second",type=int,
        default=50)
    parser.add_argument(
        "--duration-seconds",type=float,
        default=72*3600)
    parser.add_argument(
        "--dynamic-tasks",type=int,
        default=10)
    parser.add_argument(
        "--dynamic-task-mix",
        choices=("aggregate","join","mixed"),
        default="mixed")
    parser.add_argument(
        "--fault-every-seconds",type=float,
        default=6*3600)
    parser.add_argument(
        "--sample-seconds",type=float,
        default=1.0)
    parser.add_argument(
        "--fault-recovery-timeout-seconds",
        type=float,default=1800)
    parser.add_argument(
        "--seed-chunk",type=int,
        default=10000)
    parser.add_argument(
        "--snapshot-rows",type=int,
        default=16384)
    parser.add_argument(
        "--memory-mb",type=int,
        default=8192)
    parser.add_argument(
        "--share-mode",
        choices=("compatible","adaptive","off"),
        default="adaptive")
    parser.add_argument(
        "--mysql-resource-pid",type=int)
    parser.add_argument(
        "--starrocks-fe-resource-pid",type=int)
    parser.add_argument(
        "--starrocks-be-resource-pid",type=int)
    parser.add_argument(
        "--starrocks-be-resource-port",type=int,
        default=int(os.environ.get(
            "M2S_TEST_STARROCKS_BE_HTTP_PORT","8040")))
    parser.add_argument(
        "--drain-timeout-seconds",
        type=float,default=1800)
    parser.add_argument(
        "--checkpoint-seconds",
        type=float,default=300.0,
        help=(
            "seconds between atomic progress checkpoints; "
            "0 disables periodic checkpoints"))
    parser.add_argument(
        "--work-directory",type=Path,
        help=(
            "optional empty directory to preserve catalog/state/logs "
            "after the run; omitted uses a disposable temporary directory"))
    parser.add_argument(
        "--output",type=Path,
        default=Path(
            "benchmark-results/longhaul-workload.json"))
    args=parser.parse_args()
    if not args.isolated:
        parser.error(
            "--isolated is required; this workload drops "
            +DATABASE+" on both configured servers")
    if args.rows<1:
        parser.error("--rows must be positive")
    if not 1<=args.rows_per_second<=100000:
        parser.error(
            "--rows-per-second must be 1..100000")
    if args.duration_seconds<=0:
        parser.error(
            "--duration-seconds must be positive")
    if not 0<=args.dynamic_tasks<=1000:
        parser.error(
            "--dynamic-tasks must be 0..1000")
    if args.fault_every_seconds<0:
        parser.error(
            "--fault-every-seconds cannot be negative")
    if args.sample_seconds<=0:
        parser.error(
            "--sample-seconds must be positive")
    if args.checkpoint_seconds<0:
        parser.error(
            "--checkpoint-seconds cannot be negative")
    if args.fault_recovery_timeout_seconds<=0:
        parser.error(
            "--fault-recovery-timeout-seconds must be positive")
    if args.seed_chunk<1:
        parser.error("--seed-chunk must be positive")
    for name,value in (
        ("--mysql-resource-pid",args.mysql_resource_pid),
        ("--starrocks-fe-resource-pid",args.starrocks_fe_resource_pid),
        ("--starrocks-be-resource-pid",args.starrocks_be_resource_pid),
    ):
        if value is not None and int(value)<=0:
            parser.error(name+" must be positive")
    if not 1<=int(args.starrocks_be_resource_port)<=65535:
        parser.error(
            "--starrocks-be-resource-port must be 1..65535")
    if args.certification_profile:
        mismatch=certification_mismatches(args)
        if mismatch:
            parser.error(
                "--certification-profile requirements differ: "
                +json.dumps(
                    mismatch,sort_keys=True))
    run(args)


if __name__=="__main__":
    main()
