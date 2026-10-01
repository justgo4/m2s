#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import join_ir
import physical_state_catalog
import source_state
import stateful_physical_registry


ORDER_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("category","varchar","varchar(32)","NO","utf8mb4_bin",""),
    ("amount","decimal","decimal(18,2)","YES",None,""),
]
CUSTOMER_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def order_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("customer_id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
    ])


def customer_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("name",pa.string()),
    ])


def main():
    con=sqlite3.connect(":memory:",isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    physical_state_catalog.install(con)
    source_state.register_relation(
        con,"db.orders","source-epoch-a",
        order_schema(),["id"],schema_epoch=3)
    source_state.register_relation(
        con,"db.customers","source-epoch-a",
        customer_schema(),["id"],schema_epoch=5)

    agg_ir=aggregate_ir.compile_sql(
        "db.orders",ORDER_SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total "
        "FROM arrow_batch GROUP BY category")
    agg_task=dict(
        task_id="agg-task-a",
        sink_key="starrocks.agg_a",
        state_id="agg-state-a",
        generation_id="sink:starrocks.agg_a:plan:1",
        ir=agg_ir,
    )
    first=stateful_physical_registry.sync_ready(
        con,"aggregate",agg_task,10)
    assert first["health"]=="ready"
    assert first["watermark"]==10
    assert first["spec"]["schema_epochs"]==[3]
    assert first["spec"]["relations"]==[
        "source-epoch-a::db.orders"]
    assert len(first["refs"]) if "refs" in first else True

    advanced=stateful_physical_registry.sync_ready(
        con,"aggregate",agg_task,12)
    assert advanced["watermark"]==12
    assert advanced["min_readable_watermark"]==12
    refs=physical_state_catalog.state_refs(
        con,advanced["instance_id"])
    assert [
        (row["owner_id"],row["role"])
        for row in refs
    ]==[("agg-task-a","owner")]

    # A second sink with identical source semantics produces a distinct physical
    # instance but the same semantic identity, which is the prerequisite for a
    # future planner to choose reuse without conflating ownership.
    agg_task_b=dict(
        task_id="agg-task-b",
        sink_key="starrocks.agg_b",
        state_id="agg-state-b",
        generation_id="sink:starrocks.agg_b:plan:2",
        ir=agg_ir,
    )
    second=stateful_physical_registry.sync_ready(
        con,"aggregate",agg_task_b,12)
    assert second["instance_id"]!=advanced["instance_id"]
    assert second["semantic_id"]==advanced["semantic_id"]
    found=physical_state_catalog.find_semantic(
        con,advanced["spec"])
    assert {
        row["instance_id"] for row in found
    }=={
        advanced["instance_id"],second["instance_id"]
    }

    join_plan=join_ir.compile_sql(
        "db.orders",ORDER_SIG,["id"],
        "db.customers",CUSTOMER_SIG,["id"],
        "SELECT l.id AS order_id,r.name AS customer_name "
        "FROM left_batch l INNER JOIN right_batch r "
        "ON l.customer_id=r.id")
    join_task=dict(
        task_id="join-task-a",
        sink_key="starrocks.join_a",
        state_id="join-state-a",
        generation_id="sink:starrocks.join_a:plan:3",
        ir=join_plan,
    )
    joined=stateful_physical_registry.sync_ready(
        con,"inner_join",join_task,12)
    assert joined["spec"]["schema_epochs"]==[3,5]
    assert joined["spec"]["relations"]==[
        "source-epoch-a::db.orders",
        "source-epoch-a::db.customers",
    ]
    assert joined["spec"]["key_exprs"]==[
        "left.customer_id","right.id"]
    assert joined["semantic_id"]!=advanced["semantic_id"]

    retired=stateful_physical_registry.retire(
        con,"aggregate",agg_task)
    assert retired["health"]=="retired"
    assert physical_state_catalog.gc_eligible(
        con,retired["instance_id"])
    assert physical_state_catalog.state_refs(
        con,retired["instance_id"])==[]

    con.close()
    print(
        "stateful_physical_registry_test ok semantic_identity "
        "schema_epoch source_epoch ready_advance retire_gc",
        flush=True,
    )


if __name__=="__main__":
    main()
