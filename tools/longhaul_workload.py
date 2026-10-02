#!/usr/bin/env python3
"""Fixed-resource MySQL -> j4 -> StarRocks long-haul workload driver.

This is the P11 workload generator, separate from longhaul_gate.py.  The
production certification profile is 50M initial rows, 50 newly inserted source
rows/s and 72h.  The driver keeps seed memory bounded, adds identical aggregate
tasks online to exercise shared compute, can hard-kill/restart the daemon, and
copies the daemon's final durable summary/metrics into benchmark-results.

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
            tasks=con.execute("""
                SELECT sink_key,status
                FROM aggregate_task_descriptors
                ORDER BY sink_key
            """).fetchall()
            shared=int(con.execute(
                "SELECT COUNT(*) "
                "FROM aggregate_shared_followers"
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
                    for row in tasks
                ],
                shared_followers=shared,
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
            source_ready_at=None
            next_checkpoint=started

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
                    protocol=args.load_mode,
                    initial_rows=int(args.rows),
                    rows_per_second=int(
                        args.rows_per_second),
                    duration_seconds=max(
                        0.0,now-started),
                    memory_mb=int(args.memory_mb),
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

                while (
                    task_due
                    and now>=task_due[0]
                    and source_ready(current)
                ):
                    task_due.pop(0)
                    task_index=len(
                        added_tasks)+1
                    task_publish[
                        "starrocks.agg_%03d"
                        % task_index
                    ]=time.monotonic()
                    sink=add_aggregate_task(
                        directory,env,task_index)
                    added_tasks.append(sink)

                statuses=dict(
                    [] if current is None
                    else current[
                        "aggregate_tasks"])
                for sink in added_tasks:
                    if (
                        sink not in task_ready
                        and statuses.get(sink)=="active"
                    ):
                        task_ready[sink]=(
                            time.monotonic()
                            -task_publish[sink]
                        )

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
                        source_frontier=dict(
                            log_durable_seq=recovered[
                                "state"]["log_durable_seq"],
                            base_applied_seq=recovered[
                                "state"]["base_applied_seq"],
                        ),
                    ))
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
                statuses=dict(
                    [] if current is None
                    else current[
                        "aggregate_tasks"])
                tasks_ready=all(
                    statuses.get(sink)=="active"
                    for sink in added_tasks)
                if (
                    not commit_times
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
            aggregate_checks=aggregate_exactness(
                source,cfg,
                ["agg_000"]+[
                    sink.split(".",1)[1]
                    for sink in added_tasks
                ])
            if not aggregate_checks["all_match"]:
                raise AssertionError(
                    "longhaul aggregate targets differ from source "
                    +repr(aggregate_checks))

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
                protocol=args.load_mode,
                initial_rows=int(args.rows),
                rows_per_second=int(
                    args.rows_per_second),
                duration_seconds=elapsed,
                memory_mb=int(args.memory_mb),
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
                dynamic_task_ready_seconds=dict(
                    sorted(task_ready.items())),
                faults=faults,
                final_state=final_state,
                debt=debt,
                source_totals=expected,
                target_totals=actual,
                aggregate_checks=aggregate_checks,
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


def main():
    parser=argparse.ArgumentParser(
        description=__doc__)
    parser.add_argument(
        "--isolated",action="store_true")
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
    run(args)


if __name__=="__main__":
    main()
