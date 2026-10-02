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
import incremental_ir
import relational_ir
import source_log_consumer
import source_state


def open_db(path):
    con=sqlite3.connect(path,timeout=30,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    source_log_consumer.install(con)
    return con


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("status",pa.string()),
    ])


def batch(rows):
    return pa.Table.from_pylist([
        dict(
            id=row[0],amount=row[1],status=row[2],
            _sync_op=row[3],_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema([
        pa.field("id",pa.int64()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("status",pa.string()),
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def plan():
    rel=relational_ir.mapping_ir(dict(
        src_table="orders",
        primary_key=["id"],
        _sql=(
            'SELECT "id","amount"*2 AS "double_amount" '
            'FROM arrow_batch WHERE "status"=\'paid\''
        ),
        _filter_sql='"amount">0',
        _schema_signature=[
            ("id","bigint","bigint",None,None,False),
            ("amount","decimal","decimal(18,2)",None,None,True),
            ("status","varchar","varchar(16)",None,"utf8mb4_bin",True),
        ],
    ))
    return rel,incremental_ir.compile_ir(rel)


def add_commit(con,seq_rows,pos):
    part=source_state.prepare_part(
        "db.orders",batch(seq_rows))
    seq=source_state.log_commit(
        con,"source-1",("binlog.000001",pos),None,[part])
    source_state.apply_pending(con)
    return seq


def main():
    rel,delta=plan()
    with tempfile.TemporaryDirectory(prefix="m2s-source-consumer-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.orders","source-1",schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            pa.Table.from_pylist([],schema=schema()),
            cursor=None,is_last=True)

        assert add_commit(con,[
            (1,Decimal("2.50"),"paid",0),
            (2,Decimal("3.00"),"open",0),
        ],100)==1
        assert add_commit(con,[
            (3,None,"paid",0),
        ],120)==2
        assert add_commit(con,[
            (1,Decimal("2.50"),"paid",1),
            (1,Decimal("4.00"),"paid",0),
        ],140)==3

        consumer=source_log_consumer.ensure_consumer(
            con,"task-a","db.orders",7,rel,delta,0)
        assert consumer["watermark"]==0

        first=source_log_consumer.process_next(
            con,"task-a",rel,delta)
        assert first["source_seq"]==1 and first["nrows"]==1
        rows=source_log_consumer.outbox_rows(con,"task-a")
        assert rows[0]["table"].select(
            ["id","double_amount","_sync_op"]).to_pylist()==[
                dict(
                    id=1,double_amount=Decimal("5.00"),
                    _sync_op=0)
            ]
        assert source_state.consumer_info(
            con,"task-a")["watermark"]==1

        second=source_log_consumer.process_next(
            con,"task-a",rel,delta)
        assert second["source_seq"]==2 and second["nrows"]==0
        rows=source_log_consumer.outbox_rows(con,"task-a")
        assert rows[1]["table"] is None
        assert source_state.consumer_info(
            con,"task-a")["watermark"]==2

        def crash(seq):
            assert seq==3
            raise RuntimeError("synthetic crash after outbox insert")

        try:
            source_log_consumer.process_next(
                con,"task-a",rel,delta,fault_after_outbox=crash)
            raise AssertionError("fault injection did not abort")
        except RuntimeError as exc:
            assert "synthetic crash" in str(exc)
        assert source_state.consumer_info(
            con,"task-a")["watermark"]==2
        assert [
            row["source_seq"]
            for row in source_log_consumer.outbox_rows(con,"task-a")
        ]==[1,2]
        con.close()

        con=open_db(path)
        third=source_log_consumer.process_next(
            con,"task-a",rel,delta)
        assert third["source_seq"]==3 and third["nrows"]==2
        out=source_log_consumer.outbox_rows(
            con,"task-a")[-1]["table"]
        assert out.select(
            ["id","double_amount","_sync_op","_sync_order"]
        ).to_pylist()==[
            dict(
                id=1,double_amount=Decimal("5.00"),
                _sync_op=1,_sync_order=0),
            dict(
                id=1,double_amount=Decimal("8.00"),
                _sync_op=0,_sync_order=1),
        ]
        assert source_state.consumer_info(
            con,"task-a")["watermark"]==3
        assert source_log_consumer.process_next(
            con,"task-a",rel,delta) is None

        assert source_state.retention_floor(con)==3
        result=source_state.gc(con)
        assert result["floor"]==3
        assert [
            item["seq"] for item in source_state.read_commits(con,0,allow_truncated=True)
        ]==[3]

        changed=relational_ir.mapping_ir(dict(
            src_table="orders",
            primary_key=["id"],
            _sql='SELECT "id","amount" FROM arrow_batch',
            _filter_sql='"amount">0',
            _schema_signature=[
                ("id","bigint","bigint",None,None,False),
                ("amount","decimal","decimal(18,2)",None,None,True),
                ("status","varchar","varchar(16)",None,"utf8mb4_bin",True),
            ],
        ))
        changed_delta=incremental_ir.compile_ir(changed)
        try:
            source_log_consumer.ensure_consumer(
                con,"task-a","db.orders",7,
                changed,changed_delta,3)
            raise AssertionError("consumer semantic drift was accepted")
        except RuntimeError:
            pass
        con.close()

    print(
        "source_log_consumer_test ok atomic_outbox_watermark "
        "zero_output restart retract gc",
        flush=True)


if __name__=="__main__":
    main()
