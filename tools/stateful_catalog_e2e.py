#!/usr/bin/env python3
"""Actual catalog SQL -> j4 daemon -> aggregate/JOIN -> StarRocks contract.

Runs only against disposable MySQL/StarRocks services. It proves that stateful
catalog manifests are no longer merely persisted: daemon startup expands shared
source capture, creates durable descriptors/generations, maintains aggregate
and INNER JOIN results, survives a hard restart, and drains a dropped task.
"""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
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


DATABASE="m2s_stateful_catalog_contract"


def source_options():
    return dict(
        host=os.environ.get("M2S_TEST_MYSQL_HOST","127.0.0.1"),
        port=int(os.environ.get("M2S_TEST_MYSQL_PORT","3306")),
        user=os.environ.get("M2S_TEST_MYSQL_USER","root"),
        password=os.environ.get("M2S_TEST_MYSQL_PASSWORD",""),
        charset="utf8mb4",autocommit=True,
        connect_timeout=3,read_timeout=15,write_timeout=15,
    )


def literal(value):
    return "'" + str(value).replace("'","''") + "'"


def wait_create(cfg,ddl):
    deadline=time.monotonic()+90
    while True:
        try:
            execute(cfg,ddl)
            return
        except j4.pymysql.err.ProgrammingError as exc:
            if (
                "backends without enough disk space" not in str(exc).lower()
                or time.monotonic()>=deadline
            ):
                raise
            time.sleep(1)


def create_targets(cfg):
    wait_create(cfg,(
        "CREATE TABLE "+DATABASE+".agg("
        "category VARCHAR(32) NOT NULL,"
        "n BIGINT NOT NULL,"
        "total DECIMAL(38,2) NULL,"
        "mean DOUBLE NULL"
        ") PRIMARY KEY(category) "
        "DISTRIBUTED BY HASH(category) BUCKETS 1 "
        'PROPERTIES("replication_num"="1")'
    ))
    wait_create(cfg,(
        "CREATE TABLE "+DATABASE+".joined("
        "_j4_pair_id VARCHAR(1024) NOT NULL,"
        "order_id BIGINT NULL,"
        "customer_name VARCHAR(64) NULL,"
        "amount DECIMAL(18,2) NULL"
        ") PRIMARY KEY(_j4_pair_id) "
        "DISTRIBUTED BY HASH(_j4_pair_id) BUCKETS 1 "
        'PROPERTIES("replication_num"="1")'
    ))


def setup_catalog(directory,cfg,source,mode):
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
        CDC_SERVER_ID=188621,
        CDC_STATE_FILE=str(directory/"state.sqlite3"),
        CDC_LOAD_MODE=mode,
        CDC_NATIVE_BINLOG_PATH=str(
            ROOT/"build/native/mysql_arrow_reader"),
        CDC_NATIVE_EVENT_GROUP_EVENTS=128,
        CDC_KEY_PARTITIONS=4,
        CDC_WRITE_WORKERS_MIN=1,
        CDC_WRITE_WORKERS_INITIAL=1,
        CDC_WRITE_WORKERS_MAX=2,
        CDC_SNAPSHOT_WORKERS=2,
        CDC_SNAPSHOT_ROWS=64,
        CDC_SNAPSHOT_READ_AHEAD_GROUPS=1,
        CDC_SNAPSHOT_BUNDLE_MAX_LANES=4,
        CDC_COMMIT_INTERVAL_MS=200,
        CDC_BATCH_MS=100,
        CDC_QUERY_TIMEOUT=15,
        CDC_LOAD_TIMEOUT=60,
        CDC_STATUS_SECONDS=5,
        CDC_IDLE_STATUS_SECONDS=10,
        CDC_COMPRESSION="",
        CDC_DETAIL_LOGS=True,
        CDC_SHARED_SOURCE_STATE=True,
        CDC_RESOURCE_MEMORY_MB=2048,
        CDC_DUCKDB_MEMORY="64MB",
    )
    catalog=directory/"catalog.sqlite3"
    commands=[
        "SET VARIABLE "+key+" = "+literal(value)
        for key,value in values.items()
    ]
    commands.extend([
        (
            "CREATE TABLE starrocks.agg AS "
            "SELECT category, COUNT(*) AS n, "
            "SUM(amount) AS total, AVG(amount) AS mean "
            "FROM mysql.orders GROUP BY category"
        ),
        (
            "CREATE TABLE starrocks.joined AS "
            "SELECT o.id AS order_id,c.name AS customer_name,"
            "o.amount AS amount "
            "FROM mysql.orders o INNER JOIN mysql.customers c "
            "ON o.customer_id=c.id"
        ),
    ])
    os.environ["CDC_CATALOG_FILE"]=str(catalog)
    os.environ["CDC_CATALOG_SOCKET"]=str(directory/"control.sock")
    result=cdc_catalog.execute_batch(
        str(catalog),commands,
        publish_callback=j4.validate_local_catalog_publish)
    if not result.get("publish"):
        raise AssertionError(
            "stateful catalog fixture was not published")
    activation=result["publish"].get("activation") or {}
    if activation.get("status")!="restart_required":
        raise AssertionError(
            "stateful catalog fixture did not request restart activation: "
            +repr(activation))
    tasks=result["publish"].get("stateful_tasks",())
    if [item["kind"] for item in tasks]!=[
        "aggregate","inner_join"
    ]:
        raise AssertionError(
            "unexpected stateful manifest order/content: "+repr(tasks))
    env=dict(
        os.environ,
        CDC_CATALOG_FILE=str(catalog),
        CDC_CATALOG_SOCKET=str(directory/"control.sock"),
    )
    return catalog,env


def start(directory,env,index):
    log=directory/("daemon-%d.log" % int(index))
    handle=log.open("wb")
    proc=subprocess.Popen(
        [sys.executable,str(ROOT/"j4.py")],
        env=env,stdout=handle,stderr=subprocess.STDOUT,
        start_new_session=True)
    return proc,handle,log


def stop(proc,handle,kill=False):
    if proc is None:
        return
    if proc.poll() is None:
        os.killpg(
            proc.pid,
            signal.SIGKILL if kill else signal.SIGTERM)
    try:
        proc.wait(timeout=45)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid,signal.SIGKILL)
        proc.wait()
        raise RuntimeError("stateful daemon did not stop")
    finally:
        handle.close()
    if not kill and proc.returncode!=0:
        raise RuntimeError(
            "stateful daemon graceful stop rc=%d" % proc.returncode)


def run_catalog_sql(directory,env,name,sql):
    path=directory/(str(name)+".sql")
    path.write_text(str(sql).rstrip()+"\n",encoding="utf-8")
    result=subprocess.run(
        [sys.executable,str(ROOT/"j4.py"),"sql",str(path)],
        env=env,capture_output=True,timeout=120)
    try:
        response=json.loads(result.stdout.decode())
    except Exception as exc:
        raise RuntimeError(
            "catalog SQL returned invalid JSON rc=%d output=%s"
            % (
                result.returncode,
                result.stdout.decode(errors="replace")[-3000:],
            )
        ) from exc
    activation=(
        ((response.get("result") or {}).get("publish") or {})
        .get("activation") or {})
    return result,response,activation


def live(proc,log):
    if proc.poll() is None:
        return
    text=log.read_text(errors="replace") if log.exists() else ""
    raise RuntimeError(
        "stateful daemon exited rc=%d diagnostics=%s"
        % (proc.returncode,(text[:2500]+"\\n...\\n"+text[-5000:])))


def state(path):
    if not path.exists():
        return None
    try:
        con=sqlite3.connect(
            "file:"+str(path)+"?mode=ro",
            uri=True,timeout=2)
        try:
            agg=con.execute("""
                SELECT sink_key,status FROM aggregate_task_descriptors
                ORDER BY task_id
            """).fetchall()
            joins=con.execute("""
                SELECT sink_key,status FROM join_task_descriptors
                ORDER BY task_id
            """).fetchall()
            generations=con.execute("""
                SELECT sink_key,status,source_pin_released
                FROM task_generations
                ORDER BY sink_key,plan_version
            """).fetchall()
            pending=int(con.execute(
                "SELECT COUNT(*) FROM active_jobs"
            ).fetchone()[0])
            deliveries=int(con.execute(
                "SELECT COUNT(*) FROM deliveries"
            ).fetchone()[0])
            consumers=int(con.execute("""
                SELECT COUNT(*) FROM source_consumers
            """).fetchone()[0])
            source=con.execute("""
                SELECT table_name,complete_seq
                FROM source_relations ORDER BY table_name
            """).fetchall()
            return dict(
                aggregate=[row[1] for row in agg],
                join=[row[1] for row in joins],
                aggregate_tasks=[
                    (str(row[0]),str(row[1])) for row in agg],
                join_tasks=[
                    (str(row[0]),str(row[1])) for row in joins],
                generations=[
                    (str(row[0]),str(row[1]),bool(row[2]))
                    for row in generations],
                pending=pending,deliveries=deliveries,
                consumers=consumers,
                source=[
                    (str(row[0]),None if row[1] is None else int(row[1]))
                    for row in source],
            )
        finally:
            con.close()
    except sqlite3.Error:
        return None


def normalize_decimal(value):
    if value is None:
        return None
    return str(value)


def aggregate_expected(source):
    with source.cursor() as cur:
        cur.execute(
            "SELECT category,COUNT(*),SUM(amount),AVG(amount) "
            "FROM "+DATABASE+".orders "
            "GROUP BY category ORDER BY category")
        rows=cur.fetchall()
    return [
        (
            str(row[0]),int(row[1]),
            normalize_decimal(row[2]),
            None if row[3] is None else round(float(row[3]),9),
        )
        for row in rows
    ]


def aggregate_actual(cfg,table="agg"):
    rows,_=execute(
        cfg,
        "SELECT category,n,total,mean FROM "
        +DATABASE+"."+str(table)+" ORDER BY category")
    return [
        (
            str(row[0]),int(row[1]),
            normalize_decimal(row[2]),
            None if row[3] is None else round(float(row[3]),9),
        )
        for row in rows
    ]


def join_expected(source):
    with source.cursor() as cur:
        cur.execute(
            "SELECT o.id,c.name,o.amount "
            "FROM "+DATABASE+".orders o "
            "INNER JOIN "+DATABASE+".customers c "
            "ON o.customer_id=c.id "
            "ORDER BY o.id,c.id")
        rows=cur.fetchall()
    return [
        (int(row[0]),str(row[1]),normalize_decimal(row[2]))
        for row in rows
    ]


def join_actual(cfg):
    rows,_=execute(
        cfg,
        "SELECT order_id,customer_name,amount "
        "FROM "+DATABASE+".joined "
        "ORDER BY order_id,customer_name,_j4_pair_id")
    return [
        (int(row[0]),str(row[1]),normalize_decimal(row[2]))
        for row in rows
    ]


def equal_results(source,cfg):
    expected_agg=aggregate_expected(source)
    actual_agg=aggregate_actual(cfg)
    expected_join=join_expected(source)
    actual_join=join_actual(cfg)
    if len({
        row[0] for row in execute(
            cfg,
            "SELECT _j4_pair_id FROM "+DATABASE+".joined"
        )[0]
    })!=len(actual_join):
        raise AssertionError(
            "JOIN pair identity collapsed duplicate bag members")
    return (
        expected_agg==actual_agg
        and expected_join==actual_join,
        dict(
            expected_agg=expected_agg,actual_agg=actual_agg,
            expected_join=expected_join,actual_join=actual_join,
        )
    )


def wait_ready_exact(proc,log,directory,source,cfg,timeout=240):
    deadline=time.monotonic()+timeout
    last=None
    while time.monotonic()<deadline:
        live(proc,log)
        current=state(directory/"state.sqlite3")
        if current is not None:
            ready=(
                current["aggregate"]==["active"]
                and current["join"]==["active"]
                and all(
                    status=="ready" and released
                    for _,status,released in current["generations"]
                )
                and current["pending"]==0
                and current["deliveries"]==0
                and sorted(
                    name for name,complete in current["source"]
                    if complete is not None
                )==[
                    DATABASE+".customers",
                    DATABASE+".orders",
                ]
            )
            if ready:
                ok,detail=equal_results(source,cfg)
                if ok:
                    return current,detail
                last=detail
        time.sleep(.2)
    raise AssertionError(
        "stateful catalog daemon did not reach exact ready state "
        "state=%r result=%r" % (
            state(directory/"state.sqlite3"),last))


def wait_hot_aggregate_exact(
        proc,log,directory,source,cfg,
        sink="starrocks.agg_hot",table="agg_hot",timeout=240
):
    deadline=time.monotonic()+timeout
    last=None
    while time.monotonic()<deadline:
        live(proc,log)
        current=state(directory/"state.sqlite3")
        if current is not None:
            tasks=dict(current.get("aggregate_tasks",()))
            if (
                tasks.get(sink)=="active"
                and current["pending"]==0
                and current["deliveries"]==0
            ):
                expected=aggregate_expected(source)
                actual=aggregate_actual(cfg,table)
                base_ok,base_detail=equal_results(
                    source,cfg)
                if expected==actual and base_ok:
                    return current,dict(
                        expected=expected,actual=actual,
                        base=base_detail)
                last=dict(
                    expected=expected,actual=actual,
                    base=base_detail)
        time.sleep(.2)
    raise AssertionError(
        "hot aggregate did not become exact state=%r result=%r"
        % (state(directory/"state.sqlite3"),last))


def wait_stateful_retired(
        proc,log,directory,sink,kind="aggregate",timeout=180
):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        live(proc,log)
        current=state(directory/"state.sqlite3")
        if current is not None:
            tasks=dict(current[
                "aggregate_tasks" if kind=="aggregate"
                else "join_tasks"])
            if (
                tasks.get(sink)=="retired"
                and current["pending"]==0
                and current["deliveries"]==0
            ):
                return current
        time.sleep(.2)
    raise AssertionError(
        "stateful task did not retire online sink=%s state=%r"
        % (sink,state(directory/"state.sqlite3")))


def mutate(source):
    source.begin()
    try:
        with source.cursor() as cur:
            # Same MySQL transaction changes both JOIN sides and aggregate input.
            cur.execute(
                "UPDATE "+DATABASE+".customers "
                "SET name='renamed' WHERE id=10")
            cur.execute(
                "UPDATE "+DATABASE+".orders "
                "SET amount=amount+1.00,category='b' WHERE id=1")
            cur.execute(
                "DELETE FROM "+DATABASE+".orders WHERE id=2")
            cur.execute(
                "INSERT INTO "+DATABASE+".orders "
                "VALUES(5,'a',10,7.00)")
        source.commit()
    except BaseException:
        source.rollback()
        raise


def mutate_after_restart(source):
    source.begin()
    try:
        with source.cursor() as cur:
            cur.execute(
                "UPDATE "+DATABASE+".orders "
                "SET customer_id=11,amount=9.50 WHERE id=3")
            cur.execute(
                "UPDATE "+DATABASE+".customers "
                "SET name='alice2' WHERE id=11")
            cur.execute(
                "INSERT INTO "+DATABASE+".orders "
                "VALUES(6,'c',11,NULL)")
        source.commit()
    except BaseException:
        source.rollback()
        raise


def mutate_after_hot_add(source):
    source.begin()
    try:
        with source.cursor() as cur:
            cur.execute(
                "UPDATE "+DATABASE+".orders "
                "SET amount=amount+2.25 WHERE id=5")
            cur.execute(
                "INSERT INTO "+DATABASE+".orders "
                "VALUES(7,'c',11,4.25)")
        source.commit()
    except BaseException:
        source.rollback()
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--isolated",action="store_true",required=True)
    parser.add_argument(
        "--load-mode",
        choices=("transaction","merge_async"),
        default="transaction")
    parser.add_argument(
        "--output",type=Path,
        default=Path(
            "benchmark-results/stateful-catalog-contract.json"))
    args=parser.parse_args()

    cfg=configuration()
    wait_ready(cfg)
    cfg["sr"]["database"]=DATABASE
    execute(cfg,"DROP DATABASE IF EXISTS "+DATABASE)
    execute(cfg,"CREATE DATABASE "+DATABASE)

    opts=source_options()
    source=j4.pymysql.connect(**opts)
    proc=handle=log=None
    try:
        with source.cursor() as cur:
            cur.execute(
                "DROP DATABASE IF EXISTS "+DATABASE)
            cur.execute(
                "CREATE DATABASE "+DATABASE
                +" CHARACTER SET utf8mb4")
            cur.execute(
                "CREATE TABLE "+DATABASE+".customers("
                "id BIGINT NOT NULL,name VARCHAR(64),"
                "PRIMARY KEY(id)) ENGINE=InnoDB")
            cur.execute(
                "CREATE TABLE "+DATABASE+".orders("
                "id BIGINT NOT NULL,"
                "category VARCHAR(32) NOT NULL,"
                "customer_id BIGINT NULL,"
                "amount DECIMAL(18,2) NULL,"
                "PRIMARY KEY(id)) ENGINE=InnoDB")
            cur.executemany(
                "INSERT INTO "+DATABASE+".customers VALUES(%s,%s)",
                [(10,"same"),(11,"alice")])
            cur.executemany(
                "INSERT INTO "+DATABASE+".orders VALUES(%s,%s,%s,%s)",
                [
                    (1,"a",10,Decimal("7.00")),
                    (2,"a",10,Decimal("7.00")),
                    (3,"b",10,Decimal("5.00")),
                    (4,"b",None,Decimal("2.00")),
                ])

        with tempfile.TemporaryDirectory(
            prefix="m2s-stateful-catalog-e2e-"
        ) as td:
            directory=Path(td)
            catalog,env=setup_catalog(
                directory,cfg,opts,args.load_mode)

            proc,handle,log=start(directory,env,1)
            first_state,first=wait_ready_exact(
                proc,log,directory,source,cfg)

            mutate(source)
            second_state,second=wait_ready_exact(
                proc,log,directory,source,cfg)

            stop(proc,handle,kill=True)
            proc=handle=log=None
            mutate_after_restart(source)
            proc,handle,log=start(directory,env,2)
            third_state,third=wait_ready_exact(
                proc,log,directory,source,cfg)

            # Drop aggregate via catalog. Stateful topology changes are validated
            # online but intentionally activate only after restart.
            drop=directory/"drop-agg.sql"
            drop.write_text(
                "DROP TABLE starrocks.agg;\n",
                encoding="utf-8")
            result=subprocess.run(
                [sys.executable,str(ROOT/"j4.py"),"sql",str(drop)],
                env=env,capture_output=True,timeout=120)
            try:
                response=json.loads(
                    result.stdout.decode())
            except Exception as exc:
                raise RuntimeError(
                    "stateful DROP returned invalid JSON: "
                    +result.stdout.decode(errors="replace")[-3000:]
                ) from exc
            activation=(
                ((response.get("result") or {}).get("publish") or {})
                .get("activation") or {})
            if (
                result.returncode==0
                or activation.get("status")!="restart_required"
            ):
                raise AssertionError(
                    "stateful DROP must commit with restart_required: "
                    +json.dumps(response,sort_keys=True))

            stop(proc,handle,kill=False)
            proc=handle=log=None
            proc,handle,log=start(directory,env,3)
            deadline=time.monotonic()+180
            retired=False
            while time.monotonic()<deadline:
                live(proc,log)
                current=state(directory/"state.sqlite3")
                if (
                    current
                    and current["aggregate"]==["retired"]
                    and current["join"]==["active"]
                    and current["consumers"]==1
                    and current["pending"]==0
                    and current["deliveries"]==0
                ):
                    if join_expected(source)==join_actual(cfg):
                        retired=True
                        break
                time.sleep(.2)
            if not retired:
                raise AssertionError(
                    "dropped aggregate task did not retire cleanly: "
                    +repr(state(directory/"state.sqlite3")))

            stop(proc,handle,kill=False)
            proc=handle=log=None
            report=dict(
                format_version=1,
                kind="stateful_catalog_daemon_contract",
                protocol=args.load_mode,
                catalog_plan=cdc_catalog.load_plan(str(catalog))["version"],
                initial_exact=True,
                same_transaction_update_exact=True,
                hard_restart_exact=True,
                aggregate_status_before_drop=third_state["aggregate"],
                join_status_before_drop=third_state["join"],
                dropped_aggregate_retired=True,
                remaining_join_exact=True,
                automatic_stateful_target_creation=True,
                source_relations=third_state["source"],
                first=first,
                second=second,
                third=third,
            )
            args.output.parent.mkdir(
                parents=True,exist_ok=True)
            args.output.write_text(
                json.dumps(report,indent=2,sort_keys=True)+"\n")
            print(
                json.dumps({
                    key:value for key,value in report.items()
                    if key not in {"first","second","third"}
                },sort_keys=True),
                flush=True)
    finally:
        if proc is not None:
            stop(proc,handle,kill=True)
        source.close()
        execute(cfg,"DROP DATABASE IF EXISTS "+DATABASE)


if __name__=="__main__":
    main()
