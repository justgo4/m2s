#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import aggregate_state


def open_db(path):
    con = sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    aggregate_state.install(con)
    return con


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def rows_by_group(con):
    return {
        row["category"]:row
        for row in aggregate_state.read_rows(con,"agg")
    }


def main():
    spec = aggregate_state.aggregate_spec(
        ["category"],
        [
            dict(output="n",function="count",input="*"),
            dict(output="nn",function="count",input="amount"),
            dict(output="total",function="sum",input="amount"),
            dict(output="mean",function="avg",input="amount"),
        ],
    )
    with tempfile.TemporaryDirectory(prefix="m2s-aggregate-") as td:
        path = os.path.join(td,"aggregate.sqlite3")
        con = open_db(path)
        assert aggregate_state.create_state(
            con,"agg",spec,watermark=10)["watermark"] == 10

        assert aggregate_state.apply_transaction(con,"agg",11,[
            dict(category="a",amount=Decimal("10.00"),_sync_op=0),
            dict(category="a",amount=None,_sync_op=0),
            dict(category="a",amount=Decimal("20.00"),_sync_op=0),
            dict(category="b",amount=Decimal("5.00"),_sync_op=0),
        ])
        current = rows_by_group(con)
        assert current["a"] == dict(
            category="a",n=3,nn=2,total=Decimal("30.00"),
            mean=15.0,_row_count=3)
        assert current["b"] == dict(
            category="b",n=1,nn=1,total=Decimal("5.00"),
            mean=5.0,_row_count=1)

        assert aggregate_state.apply_transaction(con,"agg",12,[
            dict(category="a",amount=Decimal("10.00"),_sync_op=1),
            dict(category="b",amount=Decimal("30.00"),_sync_op=0),
        ])
        current = rows_by_group(con)
        assert current["a"] == dict(
            category="a",n=2,nn=1,total=Decimal("20.00"),
            mean=20.0,_row_count=2)
        assert current["b"] == dict(
            category="b",n=2,nn=2,total=Decimal("35.00"),
            mean=17.5,_row_count=2)

        assert aggregate_state.apply_transaction(con,"agg",13,[
            dict(category="a",amount=Decimal("20.00"),_sync_op=1),
        ])
        assert rows_by_group(con)["a"] == dict(
            category="a",n=1,nn=0,total=None,mean=None,_row_count=1)

        assert aggregate_state.apply_transaction(con,"agg",14,[
            dict(category="a",amount=None,_sync_op=1),
        ])
        assert "a" not in rows_by_group(con)

        assert aggregate_state.apply_transaction(con,"agg",15,[])
        assert aggregate_state.state_info(con,"agg")["watermark"] == 15
        assert not aggregate_state.apply_transaction(con,"agg",15,[])
        expect_error(
            lambda: aggregate_state.apply_transaction(con,"agg",15,[
                dict(category="c",amount=Decimal("1.00"),_sync_op=0),
            ]),
            RuntimeError,
        )
        expect_error(
            lambda: aggregate_state.apply_transaction(con,"agg",17,[]),
            RuntimeError,
        )

        before = aggregate_state.read_rows(con,"agg")
        expect_error(
            lambda: aggregate_state.apply_transaction(con,"agg",16,[
                dict(category="c",amount=Decimal("2.00"),_sync_op=0),
                dict(category="missing",amount=Decimal("1.00"),_sync_op=1),
            ]),
            RuntimeError,
        )
        assert aggregate_state.state_info(con,"agg")["watermark"] == 15
        assert aggregate_state.read_rows(con,"agg") == before
        expect_error(
            lambda: aggregate_state.apply_transaction(con,"agg",16,[
                dict(category="c",_sync_op=0),
            ]),
            ValueError,
        )
        assert aggregate_state.state_info(con,"agg")["watermark"] == 15
        assert aggregate_state.read_rows(con,"agg") == before

        assert aggregate_state.apply_transaction(con,"agg",16,[
            dict(category="c",amount=Decimal("2.00"),_sync_op=0),
        ])
        con.close()

        con = open_db(path)
        assert aggregate_state.state_info(con,"agg")["watermark"] == 16
        assert rows_by_group(con)["c"] == dict(
            category="c",n=1,nn=1,total=Decimal("2.00"),
            mean=2.0,_row_count=1)
        expect_error(
            lambda: aggregate_state.ensure_state(
                con,"agg",
                aggregate_state.aggregate_spec(
                    ["category"],
                    [dict(output="n",function="count",input="*")]),
                watermark=16),
            RuntimeError,
        )
        con.close()

    print("aggregate_state_test ok",flush=True)


if __name__=="__main__":
    main()
