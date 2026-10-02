#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
import tempfile
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_state
import aggregate_task_catalog
import join_ir
import join_state
import j4
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
    aggregate_state.install(con)
    join_state.install(con)
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
    aggregate_state.begin_bootstrap(
        con,"agg-state-a",
        aggregate_ir.state_spec(agg_ir),10)
    aggregate_state.bind_input_semantics(
        con,"agg-state-a",aggregate_ir.semantic_id(agg_ir))
    aggregate_state.apply_bootstrap_chunk(
        con,"agg-state-a",10,[],None,True)
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

    aggregate_state.apply_transaction(
        con,"agg-state-a",11,[])
    aggregate_state.apply_transaction(
        con,"agg-state-a",12,[])
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
    assert physical_state_catalog.acquire_reusable_state(
        con,advanced["spec"],"unsafe-pin",12,
        backend=advanced["backend"],
        format_tag=advanced["format_tag"]
    ) is None
    try:
        physical_state_catalog.pin_state(
            con,advanced["instance_id"],"unsafe-pin",12)
        raise AssertionError(
            "current-only physical state accepted a durable logical pin")
    except RuntimeError as exc:
        assert "current-only" in str(exc)

    # A second sink with identical source semantics produces a distinct physical
    # instance but the same semantic identity, which is the prerequisite for a
    # future planner to choose reuse without conflating ownership.
    aggregate_state.begin_bootstrap(
        con,"agg-state-b",
        aggregate_ir.state_spec(agg_ir),12)
    aggregate_state.bind_input_semantics(
        con,"agg-state-b",aggregate_ir.semantic_id(agg_ir))
    aggregate_state.apply_bootstrap_chunk(
        con,"agg-state-b",12,[],None,True)
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
    join_state.begin_bootstrap(
        con,"join-state-a",
        join_ir.state_spec(join_plan),12)
    join_state.apply_bootstrap_chunk(
        con,"join-state-a",12,"left",[],None,True)
    join_state.apply_bootstrap_chunk(
        con,"join-state-a",12,"right",[],None,True)
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
        "left.customer_id=right.id"]
    assert joined["semantic_id"]!=advanced["semantic_id"]

    # One task retirement cannot terminalize state while another durable
    # dependency still references the same physical instance.
    physical_state_catalog.retain_state(
        con,advanced["instance_id"],"shared-dependent","dependency")
    retained=stateful_physical_registry.retire(
        con,"aggregate",agg_task)
    assert retained["health"]=="ready"
    assert not physical_state_catalog.gc_eligible(
        con,retained["instance_id"])
    assert [
        (row["owner_id"],row["role"])
        for row in physical_state_catalog.state_refs(
            con,retained["instance_id"])
    ]==[("shared-dependent","dependency")]
    physical_state_catalog.release_state(
        con,advanced["instance_id"],"shared-dependent","dependency")
    # Re-retirement is idempotent and terminalizes the now-unreferenced state.
    retired=stateful_physical_registry.retire(
        con,"aggregate",agg_task)
    assert retired["health"]=="retired"
    assert physical_state_catalog.gc_eligible(
        con,retired["instance_id"])
    assert physical_state_catalog.state_refs(
        con,retired["instance_id"])==[]

    con.close()

    # Terminal task GC removes only task-private backing state/outbox after all
    # refs, consumers, retirement intents and active writer jobs are gone.
    with tempfile.TemporaryDirectory(
        prefix="m2s-stateful-physical-gc-"
    ) as td:
        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        source_state.register_relation(
            con,"db.orders","source-epoch-a",
            order_schema(),["id"],schema_epoch=3)
        aggregate_state.begin_bootstrap(
            con,"agg-state-gc",
            aggregate_ir.state_spec(agg_ir),0)
        aggregate_state.bind_input_semantics(
            con,"agg-state-gc",aggregate_ir.semantic_id(agg_ir))
        aggregate_state.apply_bootstrap_chunk(
            con,"agg-state-gc",0,[],None,True)
        task=aggregate_task_catalog.register_task(
            con,"agg-task-gc","starrocks.agg_gc",91,agg_ir,
            "agg_gc","agg-state-gc","agg-consumer-gc",[
                dict(
                    name="category",type="VARCHAR(32)",
                    nullable=False,key=True),
                dict(
                    name="n",type="BIGINT",
                    nullable=False,key=False),
                dict(
                    name="total",type="DECIMAL(38,2)",
                    nullable=True,key=False),
            ])
        task=aggregate_task_catalog.set_status(
            con,task["task_id"],"active")
        stateful_physical_registry.sync_ready(
            con,"aggregate",task,0)
        task=aggregate_task_catalog.set_status(
            con,task["task_id"],"retired")
        stateful_physical_registry.retire(
            con,"aggregate",task)
        removed=stateful_physical_registry.gc_retired(
            con,limit=8,kind="aggregate")
        assert [item["task_id"] for item in removed]==[
            "agg-task-gc"]
        try:
            aggregate_state.state_info(
                con,"agg-state-gc")
            raise AssertionError(
                "retired aggregate backing state survived physical GC")
        except KeyError:
            pass
        try:
            physical_state_catalog.state_info(
                con,stateful_physical_registry.instance_id(
                    "aggregate",task))
            raise AssertionError(
                "retired physical catalog row survived GC")
        except KeyError:
            pass
        assert aggregate_task_catalog.task_info(
            con,"agg-task-gc")["status"]=="retired"
        con.close()

    print(
        "stateful_physical_registry_test ok semantic_identity "
        "schema_epoch source_epoch current_only_pin_fence "
        "ready_advance shared_ref_retirement retire_gc backing_gc",
        flush=True,
    )


if __name__=="__main__":
    main()
