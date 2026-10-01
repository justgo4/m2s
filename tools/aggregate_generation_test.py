#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import aggregate_generation
import aggregate_ir
import aggregate_log_consumer
import aggregate_outbox
import aggregate_state
import physical_state_catalog
import source_state
import task_generation


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


def plan():
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",SUM("amount") AS "total" '
        'FROM arrow_batch WHERE "active"=1 GROUP BY "category"',
        source_filter='"amount" IS NULL OR "amount">-1000',
    )


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    aggregate_state.install(con)
    aggregate_outbox.install(con)
    task_generation.install(con)
    physical_state_catalog.install(con)
    return con


def add_commit(con,rows,pos):
    parts=[]
    if rows is not None:
        parts.append(source_state.prepare_part(
            "db.orders",batch(rows)))
    seq=source_state.log_commit(
        con,"source-1",("binlog.000001",pos),None,parts)
    source_state.apply_pending(con)
    return seq


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(prefix="m2s-agg-generation-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.orders","source-1",source_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            table([
                (1,"a",Decimal("10.00"),1),
                (2,"b",Decimal("5.00"),1),
            ]),
            cursor=(2,),is_last=True)
        assert add_commit(con,[
            (2,"b",Decimal("5.00"),1,1),
            (2,"a",Decimal("20.00"),1,0),
        ],100)==1

        started=aggregate_generation.begin(
            con,"agg-sink",21,ir,"agg-state")
        assert started["phase"]=="bootstrap"
        generation=started["generation"]
        assert generation["fixed_w"]==1
        assert generation["source_pin_id"]==started["pin"]["pin_id"]
        assert not generation["source_pin_released"]

        # Live source advances after the chosen W.
        assert add_commit(con,[
            (3,"c",Decimal("30.00"),1,0),
        ],120)==2

        while True:
            result=aggregate_generation.process_next_chunk(
                con,"agg-sink",21,ir,"agg-state",limit=1)
            if result["done"]:
                break
        assert aggregate_state.state_info(
            con,"agg-state")["watermark"]==1

        def crash():
            raise RuntimeError("synthetic activation crash")

        try:
            aggregate_generation.activate_catchup(
                con,"agg-sink",21,"agg-consumer",
                ir,"agg-state",fault_after_consumer=crash)
            raise AssertionError("activation fault did not abort")
        except RuntimeError as exc:
            assert "synthetic activation crash" in str(exc)
        generation=task_generation.info(con,"agg-sink",21)
        assert generation["status"]=="building"
        assert not generation["source_pin_released"]
        assert source_state.pin_watermark(
            con,generation["source_pin_id"])==1
        try:
            source_state.consumer_info(con,"agg-consumer")
            raise AssertionError("activation rollback leaked consumer")
        except KeyError:
            pass
        con.close()

        con=open_db(path)
        resumed=aggregate_generation.begin(
            con,"agg-sink",21,ir,"agg-state")
        assert resumed["phase"]=="bootstrap"
        assert resumed["pin"]["pin_id"]==generation["source_pin_id"]

        activated=aggregate_generation.activate_catchup(
            con,"agg-sink",21,"agg-consumer",ir,"agg-state")
        assert activated["generation"]["status"]=="history_staged"
        assert activated["generation"]["source_pin_released"]
        assert activated["consumer"]["watermark"]==1
        try:
            source_state.pin_watermark(
                con,generation["source_pin_id"])
            raise AssertionError("generation activation leaked source pin")
        except KeyError:
            pass

        resumed=aggregate_generation.begin(
            con,"agg-sink",21,ir,"agg-state")
        assert resumed["phase"]=="catchup"
        assert resumed["pin"] is None

        catchup=aggregate_log_consumer.process_next(
            con,"agg-consumer",ir)
        assert catchup["source_seq"]==2
        assert source_state.consumer_info(
            con,"agg-consumer")["watermark"]==2

        # Activation retry after catch-up must validate the existing consumer,
        # not reacquire a new W or demand the old consumer watermark.
        retried=aggregate_generation.activate_catchup(
            con,"agg-sink",21,"agg-consumer",ir,"agg-state")
        assert retried["consumer"]["watermark"]==2
        assert retried["generation"]["fixed_w"]==1

        ready=task_generation.mark_ready_if_exists(
            con,"agg-sink",21)
        assert ready["status"]=="ready"

        # A semantically identical second task at the current fixed-W reuses
        # the already-maintained aggregate bytes. No source snapshot rows are
        # scanned for the follower generation.
        with patch.object(
            source_state,"read_snapshot_batch",
            side_effect=AssertionError(
                "physical aggregate reuse fell back to source snapshot")
        ) as snapshot_read:
            follower=aggregate_generation.begin(
                con,"agg-sink-copy",22,ir,"agg-state-copy")
            assert follower["phase"]=="bootstrap"
            assert follower["generation"]["fixed_w"]==2
            assert follower["reused_physical"]
            copied=aggregate_generation.process_next_chunk(
                con,"agg-sink-copy",22,ir,"agg-state-copy",limit=1)
            assert copied["done"]
            snapshot_read.assert_not_called()
        assert aggregate_state.read_rows(
            con,"agg-state-copy"
        )==aggregate_state.read_rows(
            con,"agg-state")
        follower_active=aggregate_generation.activate_catchup(
            con,"agg-sink-copy",22,"agg-consumer-copy",
            ir,"agg-state-copy")
        assert follower_active["consumer"]["watermark"]==2
        assert follower_active["generation"]["source_pin_released"]
        con.close()

    print(
        "aggregate_generation_test ok fixed_w resume "
        "atomic_handoff catchup retry fixed_w_physical_clone",
        flush=True,
    )


if __name__=="__main__":
    main()
