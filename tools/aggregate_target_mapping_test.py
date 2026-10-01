#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import json
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_target_mapping
import aggregate_task_catalog
import j4


SCHEMA_SIG=[
    ("id","bigint","bigint",None,None,False),
    ("category","varchar","varchar(16)",None,"utf8mb4_bin",False),
    ("amount","decimal","decimal(18,2)",None,None,True),
    ("active","tinyint","tinyint",None,None,False),
]


def plan():
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",'
        'SUM("amount") AS "total",AVG("amount") AS "mean" '
        'FROM arrow_batch WHERE "active"=1 GROUP BY "category"',
    )


def contract():
    return [
        dict(name="category",type="VARCHAR(16)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
        dict(name="mean",type="DOUBLE",nullable=True,key=False),
    ]


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def main():
    ir=plan()
    assert aggregate_target_mapping.validate_semantic_target(
        ir,contract())==contract()

    task=aggregate_task_catalog.descriptor(
        "task-1","agg_sink",41,ir,"agg_result",
        "agg-state","agg-consumer",contract())
    mapping=aggregate_target_mapping.mapping_from_descriptor(task)
    assert j4.pk_columns(mapping)==["category"]
    assert mapping["_plan_version"]==41
    assert mapping["_output_columns"]==[
        "category","n","total","mean"]
    assert mapping["_arrow_columns"]==[
        "category","n","total","mean"]
    assert "_row_count" not in mapping["_output_columns"]
    assert str(dict(mapping["_schema"])["total"])=="decimal128(38, 2)"
    assert str(dict(mapping["_schema"])["mean"])=="double"
    assert "category" in mapping["_target_constraints"]

    raw=j4.raw_arrow(mapping,[
        (0,dict(
            category="a",n=2,total=Decimal("30.00"),mean=15.0)),
        (1,dict(category="b")),
    ])
    engine=j4.transform_engine(dict(
        duckdb_memory="64MB",
        catalog_macros=(),catalog_udfs=(),
    ))
    try:
        lines=[]
        for batch,overflows in j4.transformed_line_batches(
            engine,mapping,raw,sequence=7,
            collect_overflow=True,delivery_dense_order=True):
            assert overflows==[]
            lines.extend(batch.to_pylist())
    finally:
        engine.close()
    decoded=[json.loads(line) for line in lines]
    assert decoded[0]["category"]=="a"
    assert decoded[0]["n"]==2
    assert decoded[0]["total"]==30.0
    assert decoded[0]["__op"]==0
    assert decoded[1]["category"]=="b"
    assert decoded[1]["__op"]==1

    ddl=(
        "CREATE TABLE agg_result ("
        "category varchar(16) NOT NULL,"
        "n bigint NOT NULL,"
        "total decimal(38,2) NULL,"
        "mean double NULL"
        ") ENGINE=OLAP PRIMARY KEY(category) "
        "DISTRIBUTED BY HASH(category)"
    )
    rows=[
        ("category","varchar(16)","NO","PRI",None,""),
        ("n","bigint","NO","",None,""),
        ("total","decimal(38,2)","YES","",None,""),
        ("mean","double","YES","",None,""),
    ]
    assert aggregate_target_mapping.target_contract_from_rows(
        ir,ddl,rows)==contract()

    drift=list(rows)
    drift[2]=(
        "total","decimal(38,3)","YES","",None,"")
    expect_error(
        lambda: aggregate_target_mapping.target_contract_from_rows(
            ir,ddl,drift),
        ValueError)
    expect_error(
        lambda: aggregate_target_mapping.target_contract_from_rows(
            ir,ddl,rows+[
                ("extra","bigint","YES","",None,"")]),
        ValueError)

    bad_count=contract()
    bad_count[1]=dict(
        name="n",type="LARGEINT",nullable=False,key=False)
    expect_error(
        lambda: aggregate_target_mapping.validate_semantic_target(
            ir,bad_count),
        ValueError)

    integer_ir=aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",SUM("id") AS "total_id" '
        'FROM arrow_batch GROUP BY "category"',
    )
    integer_contract=[
        dict(name="category",type="VARCHAR(16)",nullable=False,key=True),
        dict(name="total_id",type="LARGEINT",nullable=True,key=False),
    ]
    aggregate_target_mapping.validate_semantic_target(
        integer_ir,integer_contract)
    assert str(
        aggregate_target_mapping.target_arrow_type("LARGEINT")
    )=="decimal128(38, 0)"

    schema=pa.schema([
        pa.field("category",pa.string(),nullable=False),
        pa.field("n",pa.int64()),
    ])
    for bad in (
        lambda: aggregate_target_mapping.build(
            "x","y",schema,"missing"),
        lambda: aggregate_target_mapping.build(
            "x","y",
            pa.schema([pa.field("_sync_bad",pa.int64())]),
            "_sync_bad"),
    ):
        expect_error(bad,ValueError)

    print(
        "aggregate_target_mapping_test ok identity_writer "
        "semantic_types durable_contract schema_drift",
        flush=True,
    )


if __name__=="__main__":
    main()
