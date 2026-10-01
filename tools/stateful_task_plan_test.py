#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_target_mapping
import stateful_task_plan


SOURCE_METADATA={
    "orders":dict(
        schema_signature=[
            ("id","bigint","bigint","NO",None,""),
            ("category","varchar","varchar(32)","NO","utf8mb4_bin",""),
            ("customer_id","bigint","bigint","YES",None,""),
            ("amount","decimal","decimal(18,2)","YES",None,""),
        ],
        primary_key=["id"],
    ),
    "customers":dict(
        schema_signature=[
            ("id","bigint","bigint","NO",None,""),
            ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
        ],
        primary_key=["id"],
    ),
}


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def main():
    aggregate_manifest=dict(
        kind="aggregate",
        sink="starrocks.agg",
        target_table="agg",
        source_relations=["orders"],
        primary_key=[],
        sql=(
            "SELECT category, COUNT(*) AS n, "
            "SUM(amount) AS total, AVG(amount) AS mean "
            "FROM mysql.orders GROUP BY category"
        ),
    )
    aggregate_target=[
        dict(name="category",type="VARCHAR(32)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
        dict(name="mean",type="DOUBLE",nullable=True,key=False),
    ]
    agg=stateful_task_plan.compile_task(
        aggregate_manifest,11,"db",
        SOURCE_METADATA,aggregate_target)
    assert agg["kind"]=="aggregate"
    assert agg["task"]["source_relation"]=="db.orders"
    assert agg["task"]["state_id"]=="catalog:starrocks.agg:plan:11:state"
    assert agg["task"]["consumer_id"]=="catalog:starrocks.agg:plan:11:consumer"
    assert agg["task"]["ir"]["group_keys"]==["category"]
    assert agg["mapping"]["sr_table"]=="agg"
    assert agg["mapping"]["_plan_version"]==11

    join_manifest=dict(
        kind="inner_join",
        sink="starrocks.joined",
        target_table="joined",
        source_relations=["orders","customers"],
        primary_key=[],
        sql=(
            "SELECT o.amount AS amount,c.name AS customer_name "
            "FROM mysql.orders AS o INNER JOIN mysql.customers AS c "
            "ON o.customer_id=c.id"
        ),
    )
    join_target=[
        dict(
            name=join_target_mapping.PAIR_COLUMN,
            type="VARCHAR(64)",nullable=False,key=True),
        dict(name="amount",type="DECIMAL(18,2)",nullable=True,key=False),
        dict(name="customer_name",type="VARCHAR(64)",nullable=True,key=False),
    ]
    joined=stateful_task_plan.compile_task(
        join_manifest,12,"db",
        SOURCE_METADATA,join_target)
    assert joined["kind"]=="inner_join"
    assert joined["task"]["source_relations"]==[
        "db.orders","db.customers"]
    assert joined["task"]["state_id"]==(
        "catalog:starrocks.joined:plan:12:state")
    assert joined["task"]["ir"]["join_pairs"]==[
        dict(left="customer_id",right="id")]
    assert joined["mapping"]["_output_columns"]==[
        join_target_mapping.PAIR_COLUMN,
        "amount","customer_name",
    ]
    assert joined["mapping"]["_plan_version"]==12

    next_version=stateful_task_plan.compile_task(
        join_manifest,13,"db",
        SOURCE_METADATA,join_target)
    assert next_version["task"]["state_id"]!=joined["task"]["state_id"]
    assert next_version["task"]["consumer_id"]!=joined["task"]["consumer_id"]
    assert (
        next_version["task"]["descriptor_hash"]
        != joined["task"]["descriptor_hash"]
    )

    expect_error(
        lambda: stateful_task_plan.compile_task(
            join_manifest,12,"db",
            {"orders":SOURCE_METADATA["orders"]},
            join_target),
        ValueError)
    bad_target=list(join_target)
    bad_target[0]=dict(
        name=join_target_mapping.PAIR_COLUMN,
        type="VARCHAR(64)",nullable=True,key=True)
    expect_error(
        lambda: stateful_task_plan.compile_task(
            join_manifest,12,"db",
            SOURCE_METADATA,bad_target),
        ValueError)

    print(
        "stateful_task_plan_test ok aggregate join "
        "version_isolation live_metadata target_contract",
        flush=True,
    )


if __name__=="__main__":
    main()
