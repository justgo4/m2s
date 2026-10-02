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
import json
import os
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


def wait_started(proc,log,state_path,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
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
    wait_ready(cfg)
    opts=source_options()
    source=j4.pymysql.connect(**opts)
    proc=handle=log=None
    try:
        seed_seconds=setup_databases(
            source,cfg,args.rows,args.seed_chunk)
        with tempfile.TemporaryDirectory(
            prefix="m2s-longhaul-"
        ) as td:
            directory=Path(td)
            catalog,env=setup_catalog(
                directory,cfg,opts,args.load_mode,
                args.snapshot_rows,args.memory_mb,
                args.share_mode)
            state_path=directory/"state.sqlite3"
            proc,handle,log=start_daemon(
                directory,env,1)
            wait_started(
                proc,log,state_path)

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
            daemon_index=1
            faults=[]
            source_ready_at=None

            while time.monotonic()<deadline:
                now=time.monotonic()
                assert_live(proc,log)
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
                    stop_daemon(
                        proc,handle,kill=True)
                    proc=handle=log=None
                    daemon_index+=1
                    proc,handle,log=start_daemon(
                        directory,env,daemon_index)
                    wait_started(
                        proc,log,state_path)
                    faults.append(dict(
                        sequence=before,
                        restart_seconds=(
                            time.monotonic()
                            -fault_started),
                    ))
                    next_fault=(
                        now+args.fault_every_seconds)

                if now>=next_tick:
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
                    # One sentinel per source transaction measures the intended
                    # MySQL commit -> StarRocks queryable latency without using
                    # MAX(id), which could hide a slower hash lane.
                    sentinel=int(rows[-1][0])
                    commit_times[sentinel]=committed
                    next_tick+=1.0
                    if next_tick<now-1.0:
                        next_tick=now+1.0

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

            stop_daemon(
                proc,handle,kill=False)
            proc=handle=log=None
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
                dynamic_tasks=int(
                    args.dynamic_tasks),
                dynamic_task_ready_seconds=dict(
                    sorted(task_ready.items())),
                faults=faults,
                final_state=final_state,
                source_totals=expected,
                target_totals=actual,
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
        default=0)
    parser.add_argument(
        "--sample-seconds",type=float,
        default=1.0)
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
        "--drain-timeout-seconds",
        type=float,default=1800)
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
    if args.seed_chunk<1:
        parser.error("--seed-chunk must be positive")
    run(args)


if __name__=="__main__":
    main()
