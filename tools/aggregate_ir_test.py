#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import aggregate_ir


SCHEMA=[
    ("id","bigint","bigint",None,None,False),
    ("category","varchar","varchar(16)",None,"utf8mb4_bin",True),
    ("amount","decimal","decimal(18,2)",None,None,True),
    ("active","tinyint","tinyint",None,None,False),
]


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def compile_query(sql=None):
    return aggregate_ir.compile_sql(
        "orders",SCHEMA,
        sql or (
            'SELECT "category",COUNT(*) AS "n",'
            'COUNT("amount") AS "nn",SUM("amount") AS "total",'
            'AVG("amount") AS "mean" '
            'FROM arrow_batch WHERE "active"=1 GROUP BY "category"'
        ),
        source_filter='"amount" IS NULL OR "amount">-1000',
    )


def main():
    first=compile_query()
    second=compile_query(
        'select "category", count(*) as "n", '
        'count("amount") as "nn", sum("amount") as "total", '
        'avg("amount") as "mean" from arrow_batch '
        'where "active" = 1 group by "category"'
    )
    assert aggregate_ir.semantic_id(first)==aggregate_ir.semantic_id(second)
    assert first["group_keys"]==["category"]
    assert first["aggregates"]==[
        dict(output="n",function="count",input="*"),
        dict(output="nn",function="count",input="amount"),
        dict(output="total",function="sum",input="amount"),
        dict(output="mean",function="avg",input="amount"),
    ]
    assert aggregate_ir.state_spec(first)["aggregates"]==first["aggregates"]
    assert aggregate_ir.input_columns(first)==["category","amount"]

    raw=pa.Table.from_pylist([
        dict(
            id=1,category="a",amount=Decimal("2.00"),active=1,
            _sync_op=0,_sync_order=0),
        dict(
            id=2,category="a",amount=None,active=1,
            _sync_op=0,_sync_order=1),
        dict(
            id=3,category="b",amount=Decimal("4.00"),active=0,
            _sync_op=0,_sync_order=2),
        dict(
            id=4,category="c",amount=Decimal("-2000.00"),active=1,
            _sync_op=1,_sync_order=3),
    ],schema=pa.schema([
        pa.field("id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("active",pa.int8()),
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))
    con=duckdb.connect(":memory:")
    con.register("_sync_raw",raw)
    out=con.execute(
        aggregate_ir.to_duckdb_input_sql(first)).fetch_arrow_table()
    assert out.to_pylist()==[
        dict(
            category="a",amount=Decimal("2.00"),
            _sync_op=0,_sync_order=0),
        dict(
            category="a",amount=None,
            _sync_op=0,_sync_order=1),
    ]
    con.close()

    expect_error(lambda: compile_query(
        'SELECT "category",COUNT(*) AS "n" FROM arrow_batch'))
    expect_error(lambda: compile_query(
        'SELECT "category",COUNT(DISTINCT "amount") AS "n" '
        'FROM arrow_batch GROUP BY "category"'))
    expect_error(lambda: compile_query(
        'SELECT "category",SUM("amount"+1) AS "s" '
        'FROM arrow_batch GROUP BY "category"'))
    expect_error(lambda: compile_query(
        'SELECT "category",MIN("amount") AS "m" '
        'FROM arrow_batch GROUP BY "category"'))
    expect_error(lambda: compile_query(
        'SELECT "category","amount",COUNT(*) AS "n" '
        'FROM arrow_batch GROUP BY "category"'))
    expect_error(lambda: compile_query(
        'SELECT "category",COUNT(*) AS "n" '
        'FROM arrow_batch GROUP BY "category" HAVING COUNT(*)>1'))
    expect_error(lambda: compile_query(
        'SELECT "category",COUNT(*) '
        'FROM arrow_batch GROUP BY "category"'))

    print("aggregate_ir_test ok",flush=True)


if __name__=="__main__":
    main()
