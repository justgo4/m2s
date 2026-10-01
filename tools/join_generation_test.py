#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_generation
import join_ir
import join_log_consumer
import join_outbox
import join_state
import source_state
import task_generation


LEFT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","bigint","bigint","YES",None,""),
]
RIGHT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def left_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("customer_id",pa.int64()),
        pa.field("amount",pa.int64()),
    ])


def right_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("name",pa.string()),
    ])


def plan():
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        'SELECT r.name AS customer_name,l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.customer_id=r.id',
    )


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    join_state.install(con)
    join_outbox.install(con)
    task_generation.install(con)
    return con


def table(schema,rows):
    return pa.Table.from_pylist(
        rows,schema=schema)


def batch(schema,rows):
    return pa.Table.from_pylist([
        dict(row,_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(schema)+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def add_commit(con,left_rows,right_rows,pos):
    parts=[]
    if left_rows:
        parts.append(source_state.prepare_part(
            "db.orders",batch(
                left_schema(),left_rows)))
    if right_rows:
        parts.append(source_state.prepare_part(
            "db.customers",batch(
                right_schema(),right_rows)))
    seq=source_state.log_commit(
        con,"source-join",
        ("binlog.000001",int(pos)),
        None,parts)
    source_state.apply_pending(con)
    return seq


def rows(con):
    return sorted(
        (
            item["customer_name"],
            item["amount"],
        )
        for item in join_state.read_rows(
            con,"join-state")
    )


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(
        prefix="m2s-join-generation-"
    ) as td:
        path=os.path.join(
            td,"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.orders","source-join",
            left_schema(),["id"])
        source_state.register_relation(
            con,"db.customers","source-join",
            right_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            table(left_schema(),[
                dict(
                    id=1,customer_id=10,
                    amount=5),
            ]),
            cursor=(1,),is_last=True)
        source_state.stage_snapshot_batch(
            con,"db.customers",
            table(right_schema(),[
                dict(id=10,name="alice"),
            ]),
            cursor=(10,),is_last=True)

        current=join_generation.begin(
            con,"join-sink",71,ir,
            "join-state")
        generation=current["generation"]
        pin=current["pin"]
        assert current["phase"]=="bootstrap"
        assert generation["fixed_w"]==0
        assert task_generation.source_relations(
            con,"join-sink",71
        )==["db.orders","db.customers"]
        assert source_state.pin_watermark(
            con,pin["pin_id"])==0

        # Source moves after W. Bootstrap must still materialize alice.
        assert add_commit(
            con,None,[
                dict(
                    id=10,name="alice",
                    _sync_op=1),
                dict(
                    id=10,name="alicia",
                    _sync_op=0),
            ],100)==1

        while True:
            result=join_generation.process_next_chunk(
                con,"join-sink",71,ir,
                "join-state",limit=1)
            if result["done"]:
                break
        assert rows(con)==[("alice",5)]
        assert join_state.state_info(
            con,"join-state")["watermark"]==0

        def crash():
            raise RuntimeError(
                "synthetic JOIN generation handoff crash")

        try:
            join_generation.activate_catchup(
                con,"join-sink",71,
                "join-consumer",ir,
                "join-state",
                fault_after_consumer=crash)
            raise AssertionError(
                "JOIN generation crash injection did not fire")
        except RuntimeError as exc:
            assert "synthetic JOIN generation" in str(exc)

        generation=task_generation.info(
            con,"join-sink",71)
        assert generation["status"]=="building"
        assert not generation["source_pin_released"]
        assert source_state.pin_watermark(
            con,pin["pin_id"])==0
        try:
            source_state.consumer_info(
                con,"join-consumer")
            raise AssertionError(
                "JOIN consumer survived rolled-back handoff")
        except KeyError:
            pass
        try:
            join_outbox.stream_info(
                con,"join-consumer")
            raise AssertionError(
                "JOIN outbox survived rolled-back handoff")
        except KeyError:
            pass

        con.close()
        con=open_db(path)
        resumed=join_generation.begin(
            con,"join-sink",71,ir,
            "join-state")
        assert resumed["phase"]=="bootstrap"
        assert resumed["pin"]==pin

        activated=join_generation.activate_catchup(
            con,"join-sink",71,
            "join-consumer",ir,
            "join-state")
        generation=activated["generation"]
        assert generation["status"]=="history_staged"
        assert generation["source_pin_released"]
        assert activated["consumer"]["watermark"]==0
        try:
            source_state.pin_watermark(
                con,pin["pin_id"])
            raise AssertionError(
                "JOIN generation pin leaked after activation")
        except KeyError:
            pass

        resumed=join_generation.begin(
            con,"join-sink",71,ir,
            "join-state")
        assert resumed["phase"]=="catchup"
        assert resumed["pin"] is None

        caught=join_log_consumer.process_next(
            con,"join-consumer",ir)
        assert caught["source_seq"]==1
        assert len(caught["deltas"])==1
        assert caught["deltas"][0]["op"]==0
        assert caught["deltas"][0]["row"]==dict(
            customer_name="alicia",
            amount=5)
        assert rows(con)==[("alicia",5)]
        assert source_state.consumer_info(
            con,"join-consumer")["watermark"]==1
        assert join_state.state_info(
            con,"join-state")["watermark"]==1
        assert join_outbox.commit_info(
            con,"join-consumer",0)["kind"]=="bootstrap"
        assert join_outbox.commit_info(
            con,"join-consumer",1)["kind"]=="incremental"

        # A second identical JOIN at current W clones durable pair/row state
        # atomically and never scans either source snapshot.
        with patch.object(
            source_state,"read_snapshot_batch",
            side_effect=AssertionError(
                "physical JOIN reuse fell back to source snapshot")
        ) as snapshot_read:
            follower=join_generation.begin(
                con,"join-sink-copy",72,ir,
                "join-state-copy")
            assert follower["phase"]=="bootstrap"
            assert follower["generation"]["fixed_w"]==1
            assert follower["reused_physical"]
            copied=join_generation.process_next_chunk(
                con,"join-sink-copy",72,ir,
                "join-state-copy",limit=1)
            assert copied["done"]
            snapshot_read.assert_not_called()
        assert sorted(
            (
                item["customer_name"],
                item["amount"],
            )
            for item in join_state.read_rows(
                con,"join-state-copy")
        )==rows(con)
        follower_active=join_generation.activate_catchup(
            con,"join-sink-copy",72,
            "join-consumer-copy",ir,
            "join-state-copy")
        assert follower_active["consumer"]["watermark"]==1
        assert follower_active["generation"]["source_pin_released"]
        con.close()

    print(
        "join_generation_test ok multi_source_fixed_w "
        "atomic_handoff rollback restart catchup fixed_w_physical_clone",
        flush=True,
    )


if __name__=="__main__":
    main()
