#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
import sys
import tempfile

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_bootstrap
import join_ir
import join_state
import source_state


LEFT_SCHEMA_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant_id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","bigint","bigint","YES",None,""),
]
RIGHT_SCHEMA_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant_id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def left_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("tenant_id",pa.int64()),
        pa.field("customer_id",pa.int64()),
        pa.field("amount",pa.int64()),
    ])


def right_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("tenant_id",pa.int64()),
        pa.field("name",pa.string()),
    ])


def table(schema,rows):
    return pa.Table.from_pylist(rows,schema=schema)


def batch(schema,rows):
    fields=list(schema)+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]
    return pa.Table.from_pylist([
        dict(row,_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(fields))


def plan():
    return join_ir.compile_sql(
        "db.orders",LEFT_SCHEMA_SIG,"id",
        "db.customers",RIGHT_SCHEMA_SIG,["tenant_id","id"],
        'SELECT r.name AS customer_name,l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.customer_id=r.id AND l.tenant_id=r.tenant_id',
    )


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    join_state.install(con)
    return con


def add_commit(con,left_rows,right_rows,pos):
    parts=[]
    if left_rows:
        parts.append(source_state.prepare_part(
            "db.orders",batch(left_schema(),left_rows)))
    if right_rows:
        parts.append(source_state.prepare_part(
            "db.customers",batch(right_schema(),right_rows)))
    seq=source_state.log_commit(
        con,"source-1",("binlog.000001",int(pos)),None,parts)
    source_state.apply_pending(con)
    return seq


def rows(con):
    return sorted(
        (row["customer_name"],row["amount"])
        for row in join_state.read_rows(con,"join-state")
    )


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(prefix="m2s-join-bootstrap-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.orders","source-1",left_schema(),["id"])
        source_state.register_relation(
            con,"db.customers","source-1",right_schema(),
            ["tenant_id","id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            table(left_schema(),[
                dict(id=1,tenant_id=1,customer_id=10,amount=5),
                dict(id=2,tenant_id=2,customer_id=10,amount=9),
            ]),
            cursor=(2,),is_last=True)
        source_state.stage_snapshot_batch(
            con,"db.customers",
            table(right_schema(),[
                dict(id=10,tenant_id=1,name="alice"),
                dict(id=10,tenant_id=2,name="bob"),
            ]),
            cursor=(2,10),is_last=True)

        # W=1 contains an updated amount on the left.
        assert add_commit(
            con,
            [
                dict(
                    id=1,tenant_id=1,customer_id=10,
                    amount=5,_sync_op=1),
                dict(
                    id=1,tenant_id=1,customer_id=10,
                    amount=6,_sync_op=0),
            ],
            None,100)==1
        pin=source_state.acquire_or_resume_pin(
            con,"join-build",["db.orders","db.customers"])
        assert pin["watermark"]==1

        build=join_bootstrap.ensure_build(
            con,"join-state",ir,pin["pin_id"])
        assert build["watermark"]==1
        assert not build["bootstrap_complete"]

        # Source advances after W. The right-side name at S(W) is still alice.
        assert add_commit(
            con,
            None,
            [
                dict(
                    id=10,tenant_id=1,name="alice",_sync_op=1),
                dict(
                    id=10,tenant_id=1,name="alicia",_sync_op=0),
            ],
            120)==2

        def crash():
            raise RuntimeError("synthetic JOIN bootstrap crash")

        try:
            join_bootstrap.process_next_chunk(
                con,"join-state",ir,pin["pin_id"],
                limit=1,fault_after_rows=crash)
            raise AssertionError("JOIN bootstrap crash injection did not fire")
        except RuntimeError as exc:
            assert "synthetic JOIN bootstrap crash" in str(exc)
        state=join_state.state_info(con,"join-state")
        assert state["left_cursor"] is None
        assert not state["left_complete"]
        assert not state["right_complete"]
        con.close()

        con=open_db(path)
        resumed=source_state.acquire_or_resume_pin(
            con,"join-build",["db.orders","db.customers"])
        assert resumed==pin

        seen=[]
        while True:
            result=join_bootstrap.process_next_chunk(
                con,"join-state",ir,pin["pin_id"],limit=1)
            seen.append((result["side"],result["nrows"]))
            if result["done"]:
                break
        assert any(side=="left" for side,_ in seen)
        assert any(side=="right" for side,_ in seen)
        state=join_state.state_info(con,"join-state")
        assert state["bootstrap_complete"]
        assert state["left_complete"] and state["right_complete"]
        assert state["watermark"]==1
        assert rows(con)==[
            ("alice",6),("bob",9)]
        assert source_state.base_applied_seq(con)==2
        assert source_state.pin_watermark(pin_id=pin["pin_id"],con=con)==1
        con.close()

    print(
        "join_bootstrap_test ok shared_fixed_w two_relation "
        "crash_cursor_resume source_advanced_after_w",
        flush=True,
    )


if __name__=="__main__":
    main()
