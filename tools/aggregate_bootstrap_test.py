#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import sys
import tempfile

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import aggregate_bootstrap
import aggregate_ir
import aggregate_log_consumer
import aggregate_physical_state
import aggregate_state
import physical_state_catalog
import source_state


SCHEMA_SIG=[
    ("id","bigint","bigint",None,None,False),
    ("category","varchar","varchar(16)",None,"utf8mb4_bin",True),
    ("amount","decimal","decimal(18,2)",None,None,True),
    ("active","tinyint","tinyint",None,None,False),
]


def source_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("active",pa.int8()),
    ])


def table(rows):
    return pa.Table.from_pylist([
        dict(id=r[0],category=r[1],amount=r[2],active=r[3])
        for r in rows
    ],schema=source_schema())


def batch(rows):
    return pa.Table.from_pylist([
        dict(
            id=r[0],category=r[1],amount=r[2],active=r[3],
            _sync_op=r[4],_sync_order=i)
        for i,r in enumerate(rows)
    ],schema=pa.schema(list(source_schema())+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    aggregate_state.install(con)
    return con


def plan():
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",'
        'SUM("amount") AS "total",AVG("amount") AS "mean" '
        'FROM arrow_batch WHERE "active"=1 GROUP BY "category"',
        source_filter='"amount" IS NULL OR "amount">-1000',
    )


def add_commit(con,rows,pos):
    parts=[]
    if rows is not None:
        parts.append(source_state.prepare_part(
            "db.orders",batch(rows)))
    seq=source_state.log_commit(
        con,"source-1",("binlog.000001",pos),None,parts)
    source_state.apply_pending(con)
    return seq


def groups(con):
    return {
        row["category"]:row
        for row in aggregate_state.read_rows(con,"agg-state")
    }


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(prefix="m2s-agg-bootstrap-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.orders","source-1",source_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            table([
                (1,"a",Decimal("10.00"),1),
                (2,"a",None,1),
                (3,"b",Decimal("5.00"),0),
            ]),
            cursor=(3,),is_last=True)

        assert add_commit(con,[
            (1,"a",Decimal("10.00"),1,1),
            (1,"b",Decimal("30.00"),1,0),
        ],100)==1
        assert add_commit(con,[
            (4,"a",Decimal("20.00"),1,0),
        ],120)==2

        pin=source_state.acquire_pin(
            con,"aggregate-build",["db.orders"])
        assert pin["watermark"]==2

        # Live source continues after W; bootstrap must stay on S(W).
        assert add_commit(con,[
            (4,"a",Decimal("20.00"),1,1),
            (4,"c",Decimal("50.00"),1,0),
        ],140)==3

        build=aggregate_bootstrap.ensure_build(
            con,"agg-state",ir,pin["pin_id"])
        assert build["watermark"]==2
        assert not build["bootstrap_complete"]
        assert build["input_semantic_id"]==aggregate_ir.semantic_id(ir)
        physical=physical_state_catalog.state_info(
            con,aggregate_physical_state.instance_id("agg-state"))
        assert physical["health"]=="building"
        assert physical["watermark"]==2
        assert physical["min_readable_watermark"]==2

        def crash():
            raise RuntimeError("synthetic bootstrap crash")

        try:
            aggregate_bootstrap.process_next_chunk(
                con,"agg-state",ir,pin["pin_id"],limit=1,
                fault_after_changes=crash)
            raise AssertionError("bootstrap crash injection did not fire")
        except RuntimeError as exc:
            assert "synthetic bootstrap crash" in str(exc)
        state=aggregate_state.state_info(con,"agg-state")
        assert not state["bootstrap_complete"]
        assert state["bootstrap_cursor"] is None
        con.close()

        con=open_db(path)
        # Resume exactly from durable cursor; each committed chunk advances it.
        seen=[]
        while True:
            result=aggregate_bootstrap.process_next_chunk(
                con,"agg-state",ir,pin["pin_id"],limit=1)
            seen.append(result["cursor"])
            if result["done"]:
                break
        assert len(seen)>=2
        at_w=groups(con)
        assert at_w["a"]==dict(
            category="a",n=2,total=Decimal("20.00"),
            mean=20.0,_row_count=2)
        assert at_w["b"]==dict(
            category="b",n=1,total=Decimal("30.00"),
            mean=30.0,_row_count=1)
        assert "c" not in at_w

        consumer=aggregate_bootstrap.activate_consumer(
            con,"agg-task","db.orders",11,ir,
            "agg-state",pin["pin_id"])
        assert consumer["watermark"]==2
        physical=physical_state_catalog.state_info(
            con,aggregate_physical_state.instance_id("agg-state"))
        assert physical["health"]=="ready"
        assert physical["watermark"]==2
        assert physical["min_readable_watermark"]==2
        try:
            source_state.pin_watermark(con,pin["pin_id"])
            raise AssertionError("aggregate bootstrap pin leaked")
        except KeyError:
            pass

        try:
            aggregate_physical_state.acquire_current(
                con,"agg-state",ir,"reuse-test")
            raise AssertionError(
                "current-only aggregate state accepted a logical fixed-W pin")
        except RuntimeError as exc:
            assert "current-only" in str(exc)
        assert aggregate_state.state_info(
            con,"agg-state")["watermark"]==2
        assert source_state.consumer_info(
            con,"agg-task")["watermark"]==2

        next_commit=aggregate_log_consumer.process_next(
            con,"agg-task",ir)
        assert next_commit["source_seq"]==3
        now=groups(con)
        assert now["a"]==dict(
            category="a",n=1,total=None,mean=None,_row_count=1)
        assert now["b"]==dict(
            category="b",n=1,total=Decimal("30.00"),
            mean=30.0,_row_count=1)
        assert now["c"]==dict(
            category="c",n=1,total=Decimal("50.00"),
            mean=50.0,_row_count=1)
        assert aggregate_state.state_info(
            con,"agg-state")["watermark"]==3
        assert source_state.consumer_info(
            con,"agg-task")["watermark"]==3
        physical=physical_state_catalog.state_info(
            con,aggregate_physical_state.instance_id("agg-state"))
        assert physical["watermark"]==3
        assert physical["min_readable_watermark"]==3
        con.close()

    print(
        "aggregate_bootstrap_test ok fixed_w chunk_resume "
        "activate_consumer current_only_pin_fence catchup",
        flush=True,
    )


if __name__=="__main__":
    main()
