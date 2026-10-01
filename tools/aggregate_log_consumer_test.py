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
import aggregate_ir
import aggregate_log_consumer
import aggregate_state
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


def batch(rows):
    return pa.Table.from_pylist([
        dict(
            id=row[0],category=row[1],amount=row[2],active=row[3],
            _sync_op=row[4],_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(source_schema())+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def open_db(path):
    con=sqlite3.connect(path,timeout=30,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    aggregate_state.install(con)
    return con


def ir():
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",'
        'COUNT("amount") AS "nn",SUM("amount") AS "total",'
        'AVG("amount") AS "mean" '
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


def by_group(con):
    return {
        row["category"]:row
        for row in aggregate_log_consumer.state_rows(con,"agg-task")
    }


def main():
    plan=ir()
    with tempfile.TemporaryDirectory(prefix="m2s-agg-consumer-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.orders","source-1",source_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            pa.Table.from_pylist([],schema=source_schema()),
            cursor=None,is_last=True)

        assert add_commit(con,[
            (1,"a",Decimal("10.00"),1,0),
            (2,"a",None,1,0),
            (3,"b",Decimal("5.00"),0,0),
        ],100)==1
        assert add_commit(con,[
            (1,"a",Decimal("10.00"),1,1),
            (1,"b",Decimal("30.00"),1,0),
        ],120)==2
        # Empty source transaction must still advance aggregate/consumer W.
        assert add_commit(con,None,140)==3

        aggregate_log_consumer.ensure_consumer(
            con,"agg-task","db.orders",9,plan,"agg-state",0)

        first=aggregate_log_consumer.process_next(
            con,"agg-task",plan)
        assert first["source_seq"]==1 and first["nrows"]==2
        assert by_group(con)["a"]==dict(
            category="a",n=2,nn=1,total=Decimal("10.00"),
            mean=10.0,_row_count=2)

        def crash(seq):
            assert seq==2
            raise RuntimeError("synthetic crash after aggregate state")

        try:
            aggregate_log_consumer.process_next(
                con,"agg-task",plan,fault_after_state=crash)
            raise AssertionError("aggregate fault injection did not abort")
        except RuntimeError as exc:
            assert "synthetic crash" in str(exc)
        assert aggregate_state.state_info(
            con,"agg-state")["watermark"]==1
        assert source_state.consumer_info(
            con,"agg-task")["watermark"]==1
        assert "b" not in by_group(con)
        con.close()

        con=open_db(path)
        second=aggregate_log_consumer.process_next(
            con,"agg-task",plan)
        assert second["source_seq"]==2 and second["nrows"]==2
        groups=by_group(con)
        assert groups["a"]==dict(
            category="a",n=1,nn=0,total=None,
            mean=None,_row_count=1)
        assert groups["b"]==dict(
            category="b",n=1,nn=1,total=Decimal("30.00"),
            mean=30.0,_row_count=1)

        third=aggregate_log_consumer.process_next(
            con,"agg-task",plan)
        assert third["source_seq"]==3 and third["nrows"]==0
        assert aggregate_state.state_info(
            con,"agg-state")["watermark"]==3
        assert source_state.consumer_info(
            con,"agg-task")["watermark"]==3
        assert aggregate_log_consumer.process_next(
            con,"agg-task",plan) is None
        assert source_state.retention_floor(con)==3
        con.close()

        con=open_db(path)
        assert aggregate_state.state_info(
            con,"agg-state")["watermark"]==3
        assert source_state.consumer_info(
            con,"agg-task")["watermark"]==3
        changed=aggregate_ir.compile_sql(
            "db.orders",SCHEMA_SIG,
            'SELECT "category",COUNT(*) AS "n" '
            'FROM arrow_batch GROUP BY "category"',
            source_filter='"amount" IS NULL OR "amount">-1000',
        )
        try:
            aggregate_log_consumer.ensure_consumer(
                con,"agg-task","db.orders",9,
                changed,"agg-state",3)
            raise AssertionError("aggregate semantic drift was accepted")
        except RuntimeError:
            pass

        # Same aggregate state shape but different upstream filter must also
        # fail, even with a fresh consumer id. State content is bound to the
        # full aggregate IR, not merely GROUP BY/aggregate operator shape.
        same_shape_different_input=aggregate_ir.compile_sql(
            "db.orders",SCHEMA_SIG,
            'SELECT "category",COUNT(*) AS "n",'
            'COUNT("amount") AS "nn",SUM("amount") AS "total",'
            'AVG("amount") AS "mean" '
            'FROM arrow_batch WHERE "active"=1 GROUP BY "category"',
            source_filter='"amount" IS NULL OR "amount">0',
        )
        try:
            aggregate_log_consumer.ensure_consumer(
                con,"agg-task-new","db.orders",10,
                same_shape_different_input,"agg-state",3)
            raise AssertionError(
                "aggregate state accepted different upstream semantics")
        except RuntimeError:
            pass
        con.close()

    print(
        "aggregate_log_consumer_test ok atomic_state_watermark "
        "filter retract zero_output restart",
        flush=True,
    )


if __name__=="__main__":
    main()
