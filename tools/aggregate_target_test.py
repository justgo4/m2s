#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_target
import aggregate_task_catalog


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


def schema():
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
    checked=aggregate_target.validate_semantic_target(
        ir,schema())
    assert checked==schema()

    task=aggregate_task_catalog.descriptor(
        "task-1","agg_sink",41,ir,"agg_sink",
        "agg-state","agg-consumer",schema())
    mapping=aggregate_target.mapping_from_descriptor(task)
    assert mapping["primary_key"]=="category"
    assert mapping["_output_columns"]==[
        "category","n","total","mean"]
    assert [name for name,_ in mapping["_schema"]]==[
        "category","n","total","mean"]
    assert "_row_count" not in mapping["_output_columns"]
    assert str(dict(mapping["_schema"])["total"])=="decimal128(38, 2)"
    assert str(dict(mapping["_schema"])["mean"])=="double"
    assert mapping["_arrow_columns"]==[
        "category","n","total","mean"]
    assert mapping["_target_constraints"]["category"]["primary_key"]

    ddl=(
        "CREATE TABLE agg_sink ("
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
    actual=aggregate_target.target_contract_from_rows(
        ir,ddl,rows)
    assert actual==schema()

    drift=list(rows)
    drift[2]=(
        "total","decimal(38,3)","YES","",None,"")
    expect_error(
        lambda: aggregate_target.target_contract_from_rows(
            ir,ddl,drift),
        ValueError)
    bad_count=schema()
    bad_count[1]=dict(
        name="n",type="LARGEINT",nullable=False,key=False)
    expect_error(
        lambda: aggregate_target.validate_semantic_target(
            ir,bad_count),
        ValueError)
    expect_error(
        lambda: aggregate_target.target_contract_from_rows(
            ir,ddl,rows+[
                ("extra","bigint","YES","",None,"")]),
        ValueError)

    integer_ir=aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",SUM("id") AS "total_id" '
        'FROM arrow_batch GROUP BY "category"',
    )
    integer_schema=[
        dict(name="category",type="VARCHAR(16)",nullable=False,key=True),
        dict(name="total_id",type="LARGEINT",nullable=True,key=False),
    ]
    aggregate_target.validate_semantic_target(
        integer_ir,integer_schema)
    assert str(
        aggregate_target.target_arrow_type("LARGEINT")
    )=="decimal128(38, 0)"

    print(
        "aggregate_target_test ok semantic_types target_contract "
        "writer_mapping schema_drift",
        flush=True,
    )


if __name__=="__main__":
    main()
