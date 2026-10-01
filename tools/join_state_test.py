#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_ir
import join_state


LEFT_SCHEMA=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant_id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","bigint","bigint","YES",None,""),
]
RIGHT_SCHEMA=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant_id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def plan():
    return join_ir.compile_sql(
        "db.orders",LEFT_SCHEMA,"id",
        "db.customers",RIGHT_SCHEMA,["tenant_id","id"],
        'SELECT r.name AS customer_name,l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.customer_id=r.id AND l.tenant_id=r.tenant_id',
    )


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def sorted_rows(con):
    rows=join_state.read_rows(con,"join-1")
    return sorted(
        (row["customer_name"],row["amount"])
        for row in rows
    )


def main():
    con=sqlite3.connect(":memory:",isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    join_state.install(con)
    spec=join_ir.state_spec(plan())
    state=join_state.begin_bootstrap(
        con,"join-1",spec,5)
    assert state["watermark"]==5
    assert not state["bootstrap_complete"]

    left_rows=[
        dict(id=1,tenant_id=1,customer_id=10,amount=5),
        dict(id=2,tenant_id=1,customer_id=None,amount=7),
        dict(id=3,tenant_id=2,customer_id=10,amount=9),
        dict(id=4,tenant_id=2,customer_id=10,amount=9),
    ]
    right_rows=[
        dict(id=10,tenant_id=1,name="alice"),
        dict(id=10,tenant_id=2,name="bob"),
        dict(id=11,tenant_id=1,name="unused"),
    ]
    assert join_state.apply_bootstrap_chunk(
        con,"join-1",5,"left",left_rows[:2],b"left-2",False)
    assert join_state.apply_bootstrap_chunk(
        con,"join-1",5,"right",right_rows,b"right-last",True)
    expect_error(
        lambda: join_state.read_rows(con,"join-1"),
        RuntimeError)
    assert join_state.apply_bootstrap_chunk(
        con,"join-1",5,"left",left_rows[2:],b"left-last",True)
    assert join_state.state_info(
        con,"join-1")["bootstrap_complete"]
    assert sorted_rows(con)==[
        ("alice",5),("bob",9),("bob",9)]

    # Both relations change in one source transaction. Sequential execution
    # would briefly create an "alicia" pair for order 1, but the durable JOIN
    # emits only the before->after net pair diff for source_seq 6.
    seq6=[
        ("left",dict(
            id=1,tenant_id=1,customer_id=10,amount=5,_sync_op=1)),
        ("left",dict(
            id=1,tenant_id=1,customer_id=11,amount=5,_sync_op=0)),
        ("right",dict(
            id=10,tenant_id=1,name="alice",_sync_op=1)),
        ("right",dict(
            id=10,tenant_id=1,name="alicia",_sync_op=0)),
    ]
    result=join_state.apply_transaction(
        con,"join-1",6,seq6)
    assert result["applied"]
    assert len(result["deltas"])==2
    assert sorted(
        (item["op"],item["row"]["customer_name"])
        for item in result["deltas"]
    )==[(0,"unused"),(1,"alice")]
    assert sorted_rows(con)==[
        ("bob",9),("bob",9),("unused",5)]

    retry=join_state.apply_transaction(
        con,"join-1",6,seq6)
    assert not retry["applied"] and retry["deltas"]==[]
    changed_retry=list(seq6)
    changed_retry[-1]=(
        "right",dict(
            id=10,tenant_id=1,name="different",_sync_op=0))
    expect_error(
        lambda: join_state.apply_transaction(
            con,"join-1",6,changed_retry),
        RuntimeError)
    expect_error(
        lambda: join_state.apply_transaction(
            con,"join-1",8,[]),
        RuntimeError)

    # One right-side update fans out to two left source rows that project to
    # identical values. Pair identity must retain two independent bag members.
    seq7=[
        ("right",dict(
            id=10,tenant_id=2,name="bob",_sync_op=1)),
        ("right",dict(
            id=10,tenant_id=2,name="robert",_sync_op=0)),
    ]
    result=join_state.apply_transaction(
        con,"join-1",7,seq7)
    assert len(result["deltas"])==2
    assert len({
        item["pair_id"] for item in result["deltas"]
    })==2
    assert all(item["op"]==0 for item in result["deltas"])
    assert sorted_rows(con)==[
        ("robert",9),("robert",9),("unused",5)]

    # A row whose join key was NULL begins matching only after its after image.
    seq8=[
        ("left",dict(
            id=2,tenant_id=1,customer_id=None,amount=7,_sync_op=1)),
        ("left",dict(
            id=2,tenant_id=1,customer_id=10,amount=7,_sync_op=0)),
    ]
    result=join_state.apply_transaction(
        con,"join-1",8,seq8)
    assert len(result["deltas"])==1
    assert result["deltas"][0]["op"]==0
    assert result["deltas"][0]["row"]==dict(
        customer_name="alicia",amount=7)

    before=sorted_rows(con)
    def crash(_):
        raise RuntimeError("synthetic crash")
    expect_error(
        lambda: join_state.apply_transaction(
            con,"join-1",9,[
                ("left",dict(
                    id=3,tenant_id=2,customer_id=10,
                    amount=9,_sync_op=1)),
            ],fault_after_rows=crash),
        RuntimeError)
    assert join_state.state_info(
        con,"join-1")["watermark"]==8
    assert sorted_rows(con)==before

    result=join_state.apply_transaction(
        con,"join-1",9,[
            ("left",dict(
                id=3,tenant_id=2,customer_id=10,
                amount=9,_sync_op=1)),
        ])
    assert len(result["deltas"])==1
    assert result["deltas"][0]["op"]==1
    assert sorted_rows(con)==[
        ("alicia",7),("robert",9),("unused",5)]

    con.close()
    print(
        "join_state_test ok fixed_w_two_side bootstrap "
        "same_txn_net_diff sql_null bag_pair_identity crash_rollback",
        flush=True,
    )


if __name__=="__main__":
    main()
