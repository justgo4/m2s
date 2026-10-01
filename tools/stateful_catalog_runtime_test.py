#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile
from unittest.mock import patch
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import stateful_catalog_runtime
import stateful_task_plan
import source_state


ORDERS_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("category","varchar","varchar(32)","NO","utf8mb4_bin",""),
    ("amount","bigint","bigint","YES",None,""),
]
CUSTOMERS_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def expect_error(function,error=Exception,contains=None):
    try:
        function()
    except error as exc:
        if contains is not None and contains not in str(exc):
            raise AssertionError(
                "error does not contain %r: %s" % (contains,exc))
        return
    raise AssertionError("expected "+error.__name__)


def checked_mapping():
    return dict(
        src_table="orders",
        primary_key="id",
        _schema=[
            ("id",pa.int64()),
            ("category",pa.large_string()),
            ("amount",pa.int64()),
        ],
        _schema_signature=list(ORDERS_SIG),
        _source_name_set=frozenset(
            {"id","category","amount"}),
        _source_index={
            "id":0,"category":1,"amount":2},
        _json_columns=set(),
        _source_table_comment="",
        _source_column_comments={},
    )


def hidden_customer():
    return dict(
        src_table="customers",
        primary_key="id",
        _source_only=True,
        _schema=[
            ("id",pa.int64()),
            ("name",pa.large_string()),
        ],
        _schema_signature=list(CUSTOMERS_SIG),
        _source_name_set=frozenset({"id","name"}),
        _source_index={"id":0,"name":1},
        _json_columns=set(),
        _source_table_comment="",
        _source_column_comments={},
    )


def main():
    manifests=[
        dict(
            kind="aggregate",
            sink="starrocks.agg",
            target_table="agg",
            source_relations=["orders"],
            task_version=5,
            sql=(
                "SELECT category, COUNT(*) AS n "
                "FROM mysql.orders GROUP BY category")),
        dict(
            kind="inner_join",
            sink="starrocks.joined",
            target_table="joined",
            source_relations=["orders","customers"],
            task_version=6,
            sql=(
                "SELECT o.amount AS amount,c.name AS name "
                "FROM mysql.orders o JOIN mysql.customers c "
                "ON o.id=c.id")),
    ]
    with patch.object(
        stateful_catalog_runtime,
        "_probe_source_mapping",
        side_effect=lambda cfg,table: hidden_customer()
    ) as probe:
        scope=stateful_catalog_runtime.source_scope(
            {},manifests,[checked_mapping()])
    assert scope["required_sources"]==[
        "orders","customers"]
    assert [item["src_table"] for item in scope[
        "capture_mappings"]]==["customers","orders"]
    assert scope["source_metadata"]["orders"][
        "primary_key"]==["id"]
    assert scope["source_metadata"]["customers"][
        "schema_signature"]==CUSTOMERS_SIG
    assert probe.call_count==1

    # Stateful durable writer versions live outside the positive catalog-plan
    # namespace. Engine resolution must therefore never fall through to
    # runtime_plan() for a stateful delivery, including OOM recovery.
    sentinel=object()
    runtime=dict(
        stateful_mappings={
            (
                stateful_task_plan.writer_plan_version(5),
                "starrocks.agg",
            ):dict(src_table="starrocks.agg")
        }
    )
    with patch.object(
        j4,"runtime_plan",
        side_effect=AssertionError(
            "stateful writer attempted catalog plan lookup")
    ), patch.object(
        j4,"transform_engine",
        return_value=sentinel
    ) as transform:
        resolved=j4.transform_engine_for_version(
            runtime,{"duckdb_memory":"64MB"},
            stateful_task_plan.writer_plan_version(5),
            "starrocks.agg")
    assert resolved is sentinel
    transform.assert_called_once_with(
        {"duckdb_memory":"64MB"})

    metadata=scope["source_metadata"]
    aggregate_ir=stateful_task_plan.compile_ir(
        manifests[0],"db",metadata)["ir"]
    inferred_aggregate=stateful_catalog_runtime.infer_target_schema(
        "aggregate",aggregate_ir)
    assert inferred_aggregate==[
        dict(
            name="category",type="VARCHAR(128)",
            nullable=False,key=True),
        dict(
            name="n",type="BIGINT",
            nullable=False,key=False),
    ]

    join_ir=stateful_task_plan.compile_ir(
        manifests[1],"db",metadata)["ir"]
    inferred_join=stateful_catalog_runtime.infer_target_schema(
        "inner_join",join_ir)
    assert inferred_join==[
        dict(
            name="_j4_pair_id",type="VARCHAR(1024)",
            nullable=False,key=True),
        dict(
            name="amount",type="BIGINT",
            nullable=True,key=False),
        dict(
            name="name",type="VARCHAR(256)",
            nullable=True,key=False),
    ]
    ddl=stateful_catalog_runtime.target_ddl(
        "joined",inferred_join)
    assert "PRIMARY KEY(`_j4_pair_id`)" in ddl
    assert "`name` VARCHAR(256) NULL" in ddl

    aggregate_manifest=manifests[0]
    aggregate_target=[
        dict(
            name="category",type="VARCHAR(32)",
            nullable=False,key=True),
        dict(
            name="n",type="BIGINT",
            nullable=False,key=False),
    ]
    compiled=stateful_task_plan.compile_task(
        aggregate_manifest,11,"db",
        metadata,aggregate_target)

    with tempfile.TemporaryDirectory(
        prefix="m2s-stateful-runtime-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        source_state.register_relation(
            con,"db.archived","source-1",
            pa.schema([
                pa.field("id",pa.int64()),
                pa.field("name",pa.large_string()),
            ]),["id"])
        archived=hidden_customer()
        archived["src_table"]="archived"
        with patch.object(
            stateful_catalog_runtime,
            "_probe_source_mapping",
            return_value=archived
        ) as durable_probe:
            monotonic=(
                stateful_catalog_runtime.extend_durable_source_scope(
                    con,{"mysql":{"database":"db"}},
                    [checked_mapping()]))
        assert [item["src_table"] for item in monotonic]==[
            "archived","orders"]
        durable_probe.assert_called_once_with(
            {"mysql":{"database":"db"}},"archived")

        registered=stateful_catalog_runtime.register_compiled(
            con,[compiled])
        assert registered[0]["task"]["status"]=="candidate"
        staged=stateful_catalog_runtime.stage_absent_retirements(
            con,[],17)
        assert staged==[dict(
            task_id=compiled["task"]["task_id"],
            kind="aggregate",
            sink_key=compiled["task"]["sink_key"],
            frontier=17,
        )]
        pending_retire=stateful_catalog_runtime.pending_retirements(
            con)
        assert len(pending_retire)==1
        assert pending_retire[0]["frontier"]==17
        assert pending_retire[0]["task"]["task_id"]==(
            compiled["task"]["task_id"])
        stateful_catalog_runtime.clear_retirement(
            con,compiled["task"]["task_id"])
        assert stateful_catalog_runtime.pending_retirements(
            con)==[]
        durable=stateful_catalog_runtime.durable_mappings(con)
        identity=(
            stateful_task_plan.writer_plan_version(
                compiled["task"]["plan_version"]),
            compiled["task"]["sink_key"])
        assert identity in durable
        retired=stateful_catalog_runtime.retire_absent(
            con,{},[])
        assert len(retired)==1
        assert retired[0]["task"]["status"]=="retired"
        assert stateful_catalog_runtime.durable_mappings(
            con)[identity]["sr_table"]=="agg"
        expect_error(
            lambda: stateful_catalog_runtime.ensure_registration_safe(
                con,{},[compiled]),
            RuntimeError,
            "terminal stateful task cannot be revived")
        corrupt=dict(compiled)
        corrupt["task"]=dict(
            compiled["task"],descriptor_hash="corrupt")
        expect_error(
            lambda: stateful_catalog_runtime.ensure_registration_safe(
                con,{},[corrupt]),
            RuntimeError,
            "different semantics")
        con.close()

    print(
        "stateful_catalog_runtime_test ok source_scope hidden_source "
        "target_inference stateful_engine_namespace monotonic_source_scope "
        "descriptor_register durable_retirement_intent durable_mapping "
        "drop_retire terminal_fence",
        flush=True,
    )


if __name__=="__main__":
    main()
