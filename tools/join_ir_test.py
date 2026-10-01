#!/usr/bin/env python3
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_ir


LEFT_SCHEMA=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant_id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","decimal","decimal(18,2)","YES",None,""),
]
RIGHT_SCHEMA=[
    ("id","bigint","bigint","NO",None,""),
    ("tenant_id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def compile_plan(sql):
    return join_ir.compile_sql(
        "db.orders",LEFT_SCHEMA,"id",
        "db.customers",RIGHT_SCHEMA,["tenant_id","id"],
        sql,
    )


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def main():
    sql=(
        'SELECT l.id AS order_id,r.name AS customer_name,'
        'l.amount AS amount '
        'FROM left_batch AS l INNER JOIN right_batch AS r '
        'ON l.customer_id=r.id AND l.tenant_id=r.tenant_id'
    )
    plan=compile_plan(sql)
    assert plan["kind"]=="inner_join"
    assert plan["sources"]["left"]["relation"]=="db.orders"
    assert plan["sources"]["right"]["relation"]=="db.customers"
    assert plan["sources"]["left"]["primary_key"]==["id"]
    assert plan["sources"]["right"]["primary_key"]==["tenant_id","id"]
    assert plan["join_pairs"]==[
        dict(left="customer_id",right="id"),
        dict(left="tenant_id",right="tenant_id"),
    ]
    assert join_ir.output_columns(plan)==[
        "order_id","customer_name","amount"]
    assert join_ir.input_columns(plan,"left")==[
        "id","customer_id","tenant_id","amount"]
    assert join_ir.input_columns(plan,"right")==[
        "tenant_id","id","name"]
    assert join_ir.state_spec(plan)["semantics"]["nulls"]=="sql"

    reordered=compile_plan(
        'SELECT l.id AS order_id,r.name AS customer_name,'
        'l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.tenant_id=r.tenant_id AND r.id=l.customer_id'
    )
    assert join_ir.semantic_id(reordered)==join_ir.semantic_id(plan)

    left=pa.Table.from_pylist([
        dict(id=1,tenant_id=1,customer_id=10,amount="5.00"),
        dict(id=2,tenant_id=1,customer_id=None,amount="7.00"),
        dict(id=3,tenant_id=2,customer_id=10,amount="9.00"),
    ],schema=pa.schema([
        pa.field("id",pa.int64()),
        pa.field("tenant_id",pa.int64()),
        pa.field("customer_id",pa.int64()),
        pa.field("amount",pa.string()),
    ]))
    right=pa.Table.from_pylist([
        dict(id=10,tenant_id=1,name="alice"),
        dict(id=10,tenant_id=2,name="bob"),
        dict(id=11,tenant_id=1,name="unused"),
    ])
    engine=duckdb.connect(":memory:")
    try:
        engine.register("_left",left)
        engine.register("_right",right)
        rows=engine.execute(
            join_ir.to_duckdb_sql(plan)).fetchall()
    finally:
        engine.close()
    assert rows==[
        (1,"alice","5.00"),
        (3,"bob","9.00"),
    ]

    bad_sql=[
        (
            'SELECT l.id AS order_id FROM left_batch l '
            'LEFT JOIN right_batch r ON l.customer_id=r.id'
        ),
        (
            'SELECT l.id AS order_id FROM left_batch l '
            'JOIN right_batch r ON l.customer_id=r.id OR '
            'l.tenant_id=r.tenant_id'
        ),
        (
            'SELECT l.id AS order_id FROM left_batch l '
            'JOIN right_batch r ON l.customer_id>r.id'
        ),
        (
            'SELECT l.id+1 AS order_id FROM left_batch l '
            'JOIN right_batch r ON l.customer_id=r.id'
        ),
        (
            'SELECT id AS order_id FROM left_batch l '
            'JOIN right_batch r ON l.customer_id=r.id'
        ),
        (
            'SELECT l.id AS value,r.id AS value FROM left_batch l '
            'JOIN right_batch r ON l.customer_id=r.id'
        ),
        (
            'SELECT l.id AS order_id FROM left_batch l '
            'JOIN right_batch r ON l.customer_id=r.id WHERE r.id>0'
        ),
    ]
    for statement in bad_sql:
        expect_error(lambda statement=statement: compile_plan(statement),ValueError)

    expect_error(
        lambda: join_ir.compile_sql(
            "db.orders",LEFT_SCHEMA,"missing",
            "db.customers",RIGHT_SCHEMA,"id",sql),
        ValueError,
    )
    expect_error(
        lambda: join_ir.compile_sql(
            "db.orders",LEFT_SCHEMA,"id",
            "db.orders",RIGHT_SCHEMA,"id",sql),
        ValueError,
    )

    print(
        "join_ir_test ok canonical_composite_equi sql_nulls "
        "pair_identity reject_unsupported",
        flush=True,
    )


if __name__=="__main__":
    main()
