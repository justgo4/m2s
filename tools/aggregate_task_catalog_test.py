#!/usr/bin/env python3
from pathlib import Path
import json
import os
import sqlite3
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_task_catalog


SCHEMA_SIG=[
    ("id","bigint","bigint",None,None,False),
    ("category","varchar","varchar(16)",None,"utf8mb4_bin",False),
    ("amount","decimal","decimal(18,2)",None,None,True),
    ("active","tinyint","tinyint",None,None,False),
]


def plan(filter_sql='"active"=1'):
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",'
        'SUM("amount") AS "total",AVG("amount") AS "mean" '
        'FROM arrow_batch WHERE '+filter_sql+' GROUP BY "category"',
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


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    aggregate_task_catalog.install(con)
    return con


def main():
    with tempfile.TemporaryDirectory(prefix="m2s-agg-task-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_db(path)
        first=aggregate_task_catalog.register_task(
            con,"task-1","agg_sink",41,plan(),
            "agg_sink","agg-state","agg-consumer",schema())
        assert first["generation_id"]=="sink:agg_sink:plan:41"
        assert first["source_relation"]=="db.orders"
        assert first["status"]=="candidate"
        assert first["ir_id"]==aggregate_ir.semantic_id(plan())
        assert [item["name"] for item in first["target_schema"]]==[
            "category","n","total","mean"]
        assert "_row_count" not in json.dumps(first["target_schema"])

        same=aggregate_task_catalog.register_task(
            con,"task-1","agg_sink",41,plan(),
            "agg_sink","agg-state","agg-consumer",schema())
        assert same["descriptor_hash"]==first["descriptor_hash"]

        expect_error(
            lambda: aggregate_task_catalog.register_task(
                con,"task-1","agg_sink",41,
                plan('"active"=0'),"agg_sink",
                "agg-state","agg-consumer",schema()),
            RuntimeError)
        bad_key=schema()
        bad_key[0]=dict(
            name="category",type="VARCHAR(16)",nullable=False,key=False)
        expect_error(
            lambda: aggregate_task_catalog.descriptor(
                "bad","bad_sink",1,plan(),"bad_sink",
                "bad-state","bad-consumer",bad_key),
            ValueError)
        bad_null=schema()
        bad_null[1]=dict(
            name="n",type="BIGINT",nullable=True,key=False)
        expect_error(
            lambda: aggregate_task_catalog.descriptor(
                "bad2","bad2_sink",1,plan(),"bad2_sink",
                "bad2-state","bad2-consumer",bad_null),
            ValueError)

        active=aggregate_task_catalog.set_status(
            con,"task-1","active")
        assert active["status"]=="active"
        con.close()

        con=open_db(path)
        persisted=aggregate_task_catalog.task_info(con,"task-1")
        assert persisted["descriptor_hash"]==first["descriptor_hash"]
        assert persisted["status"]=="active"
        retired=aggregate_task_catalog.set_status(
            con,"task-1","retired")
        assert retired["status"]=="retired"
        expect_error(
            lambda: aggregate_task_catalog.register_task(
                con,"task-1","agg_sink",41,plan(),
                "agg_sink","agg-state","agg-consumer",schema()),
            RuntimeError)
        assert [item["task_id"] for item in
                aggregate_task_catalog.list_tasks(con,["retired"])]==[
                    "task-1"]
        con.close()

    print(
        "aggregate_task_catalog_test ok canonical_ir target_schema "
        "generation_identity restart_terminal",
        flush=True,
    )


if __name__=="__main__":
    main()
