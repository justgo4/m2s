#!/usr/bin/env python3
"""Real StarRocks 4.1.1 contract for the candidate aggregate runtime.

The MySQL/binlog -> shared source-state boundary is already exercised by the
daemon matrix. This focused contract starts at the durable source-state API and
proves the new P8B path through fixed-W bootstrap, catch-up, aggregate outbox,
ordinary j4 durable jobs, transaction Stream Load, VISIBLE acknowledgement,
SQLite restart, and continued maintenance after ready.

Only disposable isolated services are allowed.
"""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_outbox
import aggregate_runtime
import aggregate_state
import aggregate_target_mapping
import aggregate_task_catalog
import j4
import source_state
from starrocks_contract import configuration, execute, wait_ready


DATABASE="m2s_aggregate_contract"
SOURCE_RELATION="db.orders"
SINK_KEY="agg_sink"
TARGET_TABLE="agg_result"
PLAN_VERSION=51
TASK_ID="aggregate-contract"
STATE_ID="agg-state"
CONSUMER_ID="agg-consumer"


SCHEMA_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("category","varchar","varchar(64)","NO","utf8mb4_bin",""),
    ("amount","decimal","decimal(18,2)","YES",None,""),
    ("active","tinyint","tinyint","NO",None,""),
]


def source_schema():
    return pa.schema([
        pa.field("id",pa.int64(),nullable=False),
        pa.field("category",pa.string(),nullable=False),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("active",pa.int8(),nullable=False),
    ])


def source_table(rows):
    return pa.Table.from_pylist([
        dict(id=r[0],category=r[1],amount=r[2],active=r[3])
        for r in rows
    ],schema=source_schema())


def source_batch(rows):
    return pa.Table.from_pylist([
        dict(
            id=r[0],category=r[1],amount=r[2],active=r[3],
            _sync_op=r[4],_sync_order=index)
        for index,r in enumerate(rows)
    ],schema=pa.schema(list(source_schema())+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def plan():
    return aggregate_ir.compile_sql(
        SOURCE_RELATION,SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",'
        'SUM("amount") AS "total",AVG("amount") AS "mean" '
        'FROM arrow_batch WHERE "active"=1 GROUP BY "category"',
        source_filter='"amount" IS NULL OR "amount">-1000',
    )


def public_schema():
    return pa.schema([
        pa.field("category",pa.string(),nullable=False),
        pa.field("n",pa.int64(),nullable=False),
        pa.field("total",pa.decimal128(38,2)),
        pa.field("mean",pa.float64()),
    ])


def descriptor_schema():
    return [
        dict(name="category",type="VARCHAR(64)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
        dict(name="mean",type="DOUBLE",nullable=True,key=False),
    ]


def config(path):
    cfg=configuration()
    cfg["sr"]["database"]=DATABASE
    cfg.update(
        state=str(path),
        key_partitions=4,
        batch_rows=10000,
        batch_bytes=1024*1024,
        max_row_bytes=1024*1024,
        txn_rows=10000,
        txn_bytes=8*1024*1024,
        max_inflight_deliveries=8,
        max_prepared_bytes=64*1024*1024,
        compression="",
        load_timeout=60,
        query_timeout=15,
        retry_max=6,
        pressure_max_seconds=30,
        commit_interval_ms=200,
        duckdb_memory="64MB",
        catalog_macros=(),
        catalog_udfs=(),
    )
    return cfg


def create_target(cfg):
    wait_ready(cfg)
    execute(cfg,"CREATE DATABASE IF NOT EXISTS "+DATABASE)
    execute(cfg,"DROP TABLE IF EXISTS "+DATABASE+"."+TARGET_TABLE)
    ddl=(
        "CREATE TABLE "+DATABASE+"."+TARGET_TABLE+"("
        "category VARCHAR(64) NOT NULL,"
        "n BIGINT NOT NULL,"
        "total DECIMAL(38,2) NULL,"
        "mean DOUBLE NULL"
        ") PRIMARY KEY(category) "
        "DISTRIBUTED BY HASH(category) BUCKETS 1 "
        'PROPERTIES("replication_num"="1")'
    )
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


def bind_mapping(cfg):
    mapping=aggregate_target_mapping.build(
        SINK_KEY,TARGET_TABLE,public_schema(),"category")
    rows,_=execute(
        cfg,"SHOW COLUMNS FROM "+DATABASE+"."+TARGET_TABLE)
    return aggregate_target_mapping.bind_target(
        mapping,{str(row[0]):row for row in rows})


def add_commit(con,rows,pos):
    parts=[]
    if rows is not None:
        parts.append(source_state.prepare_part(
            SOURCE_RELATION,source_batch(rows)))
    seq=source_state.log_commit(
        con,"source-contract",
        ("binlog.000001",int(pos)),None,parts)
    source_state.apply_pending(con)
    return seq


def open_state(path):
    con=j4.init_state(str(path))
    aggregate_state.install(con)
    return con


def register_descriptor(con,ir):
    return aggregate_task_catalog.register_task(
        con,TASK_ID,SINK_KEY,PLAN_VERSION,ir,TARGET_TABLE,
        STATE_ID,CONSUMER_ID,descriptor_schema())


def drain_target(con,mapping,cfg):
    table=j4.mapping_key(mapping)
    stop=threading.Event()
    runtime=dict(
        stop=stop,
        pressure_until={table:0},
        table_interval={table:cfg["commit_interval_ms"]/1000},
    )
    engine=j4.transform_engine(cfg)
    handle=j4.pycurl.Curl()
    deliveries=0
    try:
        while True:
            delivery=j4.claim_table_delivery(
                con,table,cfg)
            if delivery is None:
                pending=con.execute(
                    "SELECT COUNT(*) FROM active_jobs WHERE table_name=?",
                    (table,)).fetchone()[0]
                if not int(pending):
                    break
                raise RuntimeError(
                    "aggregate target has pending jobs but no claimable delivery")
            if not j4.prepare_delivery(
                con,engine,mapping,delivery,cfg):
                raise RuntimeError(
                    "aggregate delivery could not reserve deterministic payload")
            j4.load_transaction(
                handle,con,mapping,delivery,cfg,runtime)
            invisible=con.execute(
                "SELECT COUNT(*) FROM load_parts "
                "WHERE delivery_id=? AND visible=0",
                (delivery,)).fetchone()[0]
            if int(invisible):
                raise RuntimeError(
                    "aggregate transaction returned before every part was VISIBLE")
            j4.acknowledge_delivery(con,delivery)
            deliveries+=1
    finally:
        handle.close()
        engine.close()
    return deliveries


def normalized_target(cfg):
    rows,_=execute(
        cfg,
        "SELECT category,n,total,mean FROM "
        +DATABASE+"."+TARGET_TABLE+" ORDER BY category")
    return [
        [
            str(row[0]),int(row[1]),
            None if row[2] is None else format(row[2],"f"),
            None if row[3] is None else round(float(row[3]),8),
        ]
        for row in rows
    ]


def assert_target(cfg,expected):
    actual=normalized_target(cfg)
    if actual!=expected:
        raise AssertionError(
            "aggregate StarRocks result differs expected=%r actual=%r"
            % (expected,actual))
    return actual


def run_contract(output):
    with tempfile.TemporaryDirectory(
        prefix="m2s-aggregate-starrocks-") as td:
        path=Path(td)/"state.sqlite3"
        cfg=config(path)
        create_target(cfg)
        mapping=bind_mapping(cfg)
        ir=plan()

        con=open_state(path)
        descriptor=register_descriptor(con,ir)
        if descriptor["status"]!="candidate":
            raise AssertionError("new aggregate task is not candidate")

        source_state.register_relation(
            con,SOURCE_RELATION,"source-contract",
            source_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,SOURCE_RELATION,
            source_table([
                (1,"a",Decimal("10.00"),1),
                (2,"b",Decimal("5.00"),1),
            ]),
            cursor=(2,),is_last=True)
        if add_commit(con,[
            (2,"b",Decimal("5.00"),1,1),
            (2,"a",Decimal("20.00"),1,0),
        ],100)!=1:
            raise AssertionError("unexpected first source sequence")

        first=aggregate_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,ir,
            STATE_ID,mapping,cfg,bootstrap_limit=1)
        if first["phase"]!="bootstrap":
            raise AssertionError("aggregate fixed-W bootstrap did not start")
        fixed_w=int(first["generation"]["fixed_w"])
        if fixed_w!=1:
            raise AssertionError("aggregate fixed-W changed unexpectedly")
        con.close()

        # The source advances after W while bootstrap resumes from durable cursor.
        con=open_state(path)
        persisted=aggregate_task_catalog.task_info(
            con,TASK_ID)
        if persisted["descriptor_hash"]!=descriptor["descriptor_hash"]:
            raise AssertionError("aggregate descriptor changed across restart")
        if add_commit(con,[
            (3,"c",Decimal("30.00"),1,0),
        ],120)!=2:
            raise AssertionError("unexpected second source sequence")

        status=None
        for _ in range(40):
            status=aggregate_runtime.step(
                con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,ir,
                STATE_ID,mapping,cfg,bootstrap_limit=1)
            consumer=status["consumer"]
            if (
                consumer is not None
                and int(consumer["watermark"])==2
                and status["phase"]!="bootstrap"
            ):
                break
        if status is None or int(status["consumer"]["watermark"])!=2:
            raise AssertionError("aggregate runtime did not catch source seq 2")
        if status["generation"]["status"]!="history_staged":
            raise AssertionError(
                "aggregate generation published before target visibility")
        if not con.execute(
            "SELECT 1 FROM active_jobs "
            "WHERE table_name=? LIMIT 1",(SINK_KEY,)).fetchone():
            raise AssertionError("aggregate runtime staged no durable target jobs")
        con.close()

        # Crash/restart boundary: jobs exist durably, no target acknowledgement yet.
        con=open_state(path)
        if int(task_generation_fixed_w(con))!=fixed_w:
            raise AssertionError("aggregate generation reacquired a new W")
        first_deliveries=drain_target(
            con,mapping,cfg)
        if first_deliveries<1:
            raise AssertionError("aggregate target produced no real delivery")
        status=aggregate_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,ir,
            STATE_ID,mapping,cfg)
        if status["generation"]["status"]!="ready":
            raise AssertionError(
                "aggregate generation did not become ready after VISIBLE")
        aggregate_task_catalog.set_status(
            con,TASK_ID,"active")
        visible_after_bootstrap=int(status["visible_frontier"])
        if visible_after_bootstrap<2:
            raise AssertionError("aggregate target frontier did not cover seq 2")
        first_rows=assert_target(cfg,[
            ["a",2,"30.00",15.0],
            ["c",1,"30.00",30.0],
        ])

        # Ready is not terminal. Move one source row from group a to group b.
        if add_commit(con,[
            (1,"a",Decimal("10.00"),1,1),
            (1,"b",Decimal("40.00"),1,0),
        ],140)!=3:
            raise AssertionError("unexpected third source sequence")
        for _ in range(10):
            status=aggregate_runtime.step(
                con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,ir,
                STATE_ID,mapping,cfg)
            if int(status["consumer"]["watermark"])==3:
                break
        if int(status["consumer"]["watermark"])!=3:
            raise AssertionError("ready aggregate did not consume seq 3")
        con.close()

        # A second restart proves staged incremental jobs are enough; no recompute.
        con=open_state(path)
        if aggregate_task_catalog.task_info(
            con,TASK_ID)["status"]!="active":
            raise AssertionError("aggregate task active status was not durable")
        second_deliveries=drain_target(
            con,mapping,cfg)
        if second_deliveries<1:
            raise AssertionError(
                "aggregate incremental target produced no real delivery")
        status=aggregate_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,ir,
            STATE_ID,mapping,cfg)
        if int(status["visible_frontier"])<3:
            raise AssertionError(
                "aggregate target frontier did not cover seq 3")
        final_rows=assert_target(cfg,[
            ["a",1,"20.00",20.0],
            ["b",1,"40.00",40.0],
            ["c",1,"30.00",30.0],
        ])

        # Zero-output source transaction advances all durable frontiers with no
        # StarRocks row change and no synthetic target write.
        if add_commit(con,None,160)!=4:
            raise AssertionError("unexpected empty source sequence")
        status=aggregate_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,ir,
            STATE_ID,mapping,cfg)
        if (
            int(status["consumer"]["watermark"])!=4
            or int(status["visible_frontier"])!=4
        ):
            raise AssertionError(
                "zero-output source transaction did not advance aggregate frontier")
        if normalized_target(cfg)!=final_rows:
            raise AssertionError(
                "zero-output source transaction changed StarRocks rows")

        report=dict(
            format_version=1,
            kind="aggregate_starrocks_contract",
            starrocks_version=str(wait_ready(cfg)),
            protocol="transaction",
            fixed_w=fixed_w,
            generation_id=status["generation"]["generation_id"],
            descriptor_hash=descriptor["descriptor_hash"],
            restart_boundaries=2,
            first_deliveries=first_deliveries,
            second_deliveries=second_deliveries,
            visible_frontier=int(status["visible_frontier"]),
            first_rows=first_rows,
            final_rows=final_rows,
            zero_output_frontier=4,
        )
        con.close()

    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(
        json.dumps(report,indent=2,sort_keys=True)+"\n")
    print(json.dumps(report,sort_keys=True),flush=True)


def task_generation_fixed_w(con):
    row=con.execute("""
        SELECT fixed_w FROM task_generations
        WHERE sink_key=? AND plan_version=?
    """,(SINK_KEY,PLAN_VERSION)).fetchone()
    if not row or row[0] is None:
        raise RuntimeError("aggregate generation is missing fixed-W")
    return int(row[0])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--isolated",action="store_true",
        help="required acknowledgement that services are disposable")
    parser.add_argument(
        "--output",type=Path,
        default=Path(
            "benchmark-results/aggregate-starrocks-contract.json"))
    args=parser.parse_args()
    if not args.isolated:
        raise SystemExit(
            "refuse to run aggregate StarRocks contract without --isolated")
    run_contract(args.output)


if __name__=="__main__":
    main()
