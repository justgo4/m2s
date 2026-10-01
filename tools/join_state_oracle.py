#!/usr/bin/env python3
"""Randomized incremental/full-state oracle for durable INNER JOIN state."""
from pathlib import Path
import base64
import json
import random
import sqlite3
import sys

import duckdb
import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_ir
import join_state


SEED=20261001
TRANSACTIONS=2000
LEFT_KEYS=90
RIGHT_KEYS=45
CHECK_EVERY=25


LEFT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant","bigint","bigint","NO",None,""),
    ("fk","bigint","bigint","YES",None,""),
    ("value","bigint","bigint","YES",None,""),
]
RIGHT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant","bigint","bigint","NO",None,""),
    ("label","varchar","varchar(16)","YES","utf8mb4_bin",""),
]


def plan():
    return join_ir.compile_sql(
        "db.lefts",LEFT_SIG,"id",
        "db.rights",RIGHT_SIG,"id",
        'SELECT l.value AS left_value,r.label AS right_label '
        'FROM left_batch l JOIN right_batch r '
        'ON l.tenant=r.tenant AND l.fk=r.id',
    )


def left_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("tenant",pa.int64()),
        pa.field("fk",pa.int64()),
        pa.field("value",pa.int64()),
    ])


def right_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("tenant",pa.int64()),
        pa.field("label",pa.string()),
    ])


def _tag_int(value):
    return ["int",str(int(value))]


def _pk_blob(value):
    return json.dumps(
        [_tag_int(value)],
        ensure_ascii=False,sort_keys=True,
        separators=(",",":"),
    ).encode("utf-8")


def pair_id(left_id,right_id):
    left=_pk_blob(left_id)
    right=_pk_blob(right_id)
    return json.dumps([
        ["left",base64.b64encode(left).decode("ascii")],
        ["right",base64.b64encode(right).decode("ascii")],
    ],ensure_ascii=False,sort_keys=True,
      separators=(",",":")).encode("utf-8")


def random_left(rng,key):
    return dict(
        id=int(key),
        tenant=rng.randrange(1,5),
        fk=(
            None
            if rng.randrange(7)==0
            else rng.randrange(1,RIGHT_KEYS+1)
        ),
        value=(
            None
            if rng.randrange(9)==0
            else rng.randrange(-3,7)
        ),
    )


def random_right(rng,key):
    return dict(
        id=int(key),
        tenant=rng.randrange(1,5),
        label=rng.choice([
            "same","same","x","y","z",None
        ]),
    )


def change(side,row,op):
    value=dict(row)
    value["_sync_op"]=int(op)
    return side,value


def expected_duck(con,ir,left,right):
    left_table=pa.Table.from_pylist(
        [left[key] for key in sorted(left)],
        schema=left_schema())
    right_table=pa.Table.from_pylist(
        [right[key] for key in sorted(right)],
        schema=right_schema())
    con.register("_left",left_table)
    con.register("_right",right_table)
    try:
        rows=con.execute(
            'SELECT l.id,r.id,l.value,r.label '
            'FROM _left l INNER JOIN _right r '
            'ON l.tenant=r.tenant AND l.fk=r.id '
            'ORDER BY l.id,r.id'
        ).fetchall()
    finally:
        con.unregister("_left")
        con.unregister("_right")
    return {
        pair_id(int(left_id),int(right_id)):dict(
            left_value=left_value,
            right_label=right_label,
        )
        for left_id,right_id,left_value,right_label in rows
    }


def actual(db):
    return {
        bytes(item["pair_id"]):dict(item["row"])
        for item in join_state.read_pairs(db,"join")
    }


def mutate_one(rng,side,state,key_space):
    key=rng.randrange(1,key_space+1)
    old=state.get(key)
    action=rng.randrange(10)
    if old is None:
        new=(
            random_left(rng,key)
            if side=="left"
            else random_right(rng,key)
        )
    elif action<2:
        new=None
    else:
        new=(
            random_left(rng,key)
            if side=="left"
            else random_right(rng,key)
        )
    changes=[]
    if old is not None:
        changes.append(change(side,old,1))
    if new is None:
        state.pop(key,None)
    else:
        changes.append(change(side,new,0))
        state[key]=new
    return changes


def main():
    rng=random.Random(SEED)
    ir=plan()
    spec=join_ir.state_spec(ir)
    db=sqlite3.connect(":memory:",isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON")
    join_state.install(db)
    join_state.create_state(
        db,"join",spec,watermark=0,
        bootstrap_complete=True)
    duck=duckdb.connect(":memory:")
    left={}
    right={}

    for seq in range(1,TRANSACTIONS+1):
        changes=[]
        mode=rng.randrange(12)
        if mode==0:
            # Source commit unrelated to either input.
            pass
        else:
            # A source transaction may contain several ordered row-events,
            # including repeated mutations of the same source PK and changes
            # on both sides. Build changes against the progressively updated
            # reference state so every before-image is transaction-valid.
            event_groups=1+rng.randrange(4)
            for _ in range(event_groups):
                if mode<4:
                    side=rng.choice(["left","right"])
                elif mode<8:
                    side="left"
                else:
                    side="right"
                if side=="left":
                    changes+=mutate_one(
                        rng,"left",left,LEFT_KEYS)
                else:
                    changes+=mutate_one(
                        rng,"right",right,RIGHT_KEYS)

        result=join_state.apply_transaction(
            db,"join",seq,changes)
        if not result["applied"]:
            raise AssertionError(
                "next randomized JOIN source seq treated as retry")

        if seq%CHECK_EVERY==0 or seq==TRANSACTIONS:
            got=actual(db)
            want=expected_duck(
                duck,ir,left,right)
            if got!=want:
                missing=[
                    key for key in sorted(set(want)-set(got))
                ][:10]
                extra=[
                    key for key in sorted(set(got)-set(want))
                ][:10]
                changed=[
                    key for key in sorted(set(got)&set(want))
                    if got[key]!=want[key]
                ][:10]
                raise AssertionError(
                    "JOIN incremental/full-state mismatch "
                    "seq=%d missing=%r extra=%r changed=%r"
                    % (
                        seq,
                        [base64.b64encode(x).decode("ascii") for x in missing],
                        [base64.b64encode(x).decode("ascii") for x in extra],
                        [base64.b64encode(x).decode("ascii") for x in changed],
                    )
                )

    if join_state.state_info(
        db,"join")["watermark"]!=TRANSACTIONS:
        raise AssertionError(
            "JOIN randomized watermark did not reach final transaction")
    final=actual(db)
    duck.close()
    db.close()
    print(
        "join_state_oracle ok seed=%d transactions=%d "
        "left=%d right=%d pairs=%d"
        % (
            SEED,TRANSACTIONS,
            len(left),len(right),len(final),
        ),
        flush=True,
    )


if __name__=="__main__":
    main()
