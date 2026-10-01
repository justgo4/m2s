#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import random
import sqlite3
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import aggregate_state


SEED = 20261001
TRANSACTIONS = 1500
KEY_SPACE = 120
CHECK_EVERY = 50


def schema():
    return pa.schema([
        pa.field("row_id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
    ])


def expected(con, source):
    rows = [
        dict(row_id=key,category=value[0],amount=value[1])
        for key,value in sorted(source.items())
    ]
    table = pa.Table.from_pylist(rows,schema=schema())
    con.register("_source",table)
    result = con.execute("""
        SELECT category,COUNT(*),COUNT(amount),SUM(amount),AVG(amount)
        FROM _source
        GROUP BY category
    """).fetchall()
    con.unregister("_source")
    return {
        row[0]:dict(
            category=row[0],n=int(row[1]),nn=int(row[2]),
            total=row[3],mean=row[4],_row_count=int(row[1]))
        for row in result
    }


def actual(con):
    return {
        row["category"]:row
        for row in aggregate_state.read_rows(con,"agg")
    }


def random_value(rng):
    category = rng.choice(["a","b","c","d",None])
    amount = (
        None
        if rng.randrange(7) == 0
        else Decimal(rng.randrange(-10000,20001)).scaleb(-2)
    )
    return category,amount


def main():
    rng = random.Random(SEED)
    state = {}
    db = sqlite3.connect(":memory:",isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON")
    aggregate_state.install(db)
    spec = aggregate_state.aggregate_spec(
        ["category"],
        [
            dict(output="n",function="count",input="*"),
            dict(output="nn",function="count",input="amount"),
            dict(output="total",function="sum",input="amount"),
            dict(output="mean",function="avg",input="amount"),
        ],
    )
    aggregate_state.create_state(db,"agg",spec,watermark=0)
    duck = duckdb.connect(":memory:")

    for seq in range(1,TRANSACTIONS+1):
        changes = []
        # Some source commits do not affect this relation/operator; the
        # aggregate watermark must still advance exactly once.
        if rng.randrange(12) != 0:
            key = rng.randrange(1,KEY_SPACE+1)
            old = state.get(key)
            action = rng.randrange(10)
            if old is None:
                new = random_value(rng)
            elif action < 2:
                new = None
            else:
                new = random_value(rng)

            if old is not None:
                changes.append(dict(
                    category=old[0],amount=old[1],_sync_op=1))
            if new is not None:
                changes.append(dict(
                    category=new[0],amount=new[1],_sync_op=0))
                state[key] = new
            else:
                state.pop(key,None)

        assert aggregate_state.apply_transaction(db,"agg",seq,changes)
        if seq % CHECK_EVERY == 0 or seq == TRANSACTIONS:
            got = actual(db)
            want = expected(duck,state)
            if got != want:
                groups = sorted(
                    set(got)|set(want),
                    key=lambda value:(value is not None,str(value)))
                changed = [
                    dict(group=group,actual=got.get(group),expected=want.get(group))
                    for group in groups
                    if got.get(group) != want.get(group)
                ][:10]
                raise AssertionError(
                    "aggregate incremental/full mismatch "
                    f"seq={seq} changed={changed}")

    assert aggregate_state.state_info(db,"agg")["watermark"] == TRANSACTIONS
    duck.close()
    db.close()
    print(
        f"aggregate_state_oracle ok seed={SEED} "
        f"transactions={TRANSACTIONS} rows={len(state)}",
        flush=True,
    )


if __name__=="__main__":
    main()
