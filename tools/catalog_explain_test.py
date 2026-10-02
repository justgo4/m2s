#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import catalog_explain


def main():
    plan=dict(
        version=7,
        revision=11,
        plan_hash="abc",
        mappings=[
            dict(
                _catalog_sink="starrocks.clean",
                src_table="orders",
                sr_table="clean",
                primary_key="id",
                sql='SELECT "id" FROM arrow_batch'),
        ],
        stateful_tasks=[
            dict(
                kind="aggregate",
                sink="starrocks.rollup",
                target_table="rollup",
                source_relations=["orders"],
                primary_key=[],
                task_version=12,
                sql='SELECT bucket, COUNT(*) AS n FROM orders GROUP BY bucket'),
            dict(
                kind="inner_join",
                sink="starrocks.joined",
                target_table="joined",
                source_relations=["orders","customers"],
                primary_key=[],
                task_version=13,
                sql='SELECT o.id FROM orders o INNER JOIN customers c ON o.id=c.id'),
        ],
        macros=["CREATE MACRO x(a) AS a"],
        udfs=[dict(name="f",source="do not expose")],
    )
    status=dict(
        state_exists=True,
        active_plan_version=7,
        aggregate_tasks=[
            dict(
                task_id="agg-old",
                sink_key="starrocks.rollup",
                plan_version=6,
                status="retired",
                target_table="rollup"),
            dict(
                task_id="agg-new",
                sink_key="starrocks.rollup",
                plan_version=7,
                status="active",
                target_table="rollup"),
        ],
        join_tasks=[
            dict(
                task_id="join-new",
                sink_key="starrocks.joined",
                plan_version=7,
                status="building",
                target_table="joined"),
        ],
        sharing=dict(selected=1),
        admission=dict(waiting=0),
        physical=dict(states=2),
        rebuilds=[],
        retirements=[],
        source=dict(base_applied_seq=99),
    )
    value=catalog_explain.explain(plan,status)
    assert value["format_version"]==1
    assert value["plan_version"]==7
    assert value["stateless"][0]["primary_key"]==["id"]
    assert value["stateless"][0]["source"]=="mysql.orders"
    assert value["stateful"][0]["sink"]=="starrocks.joined"
    rollup=next(
        item for item in value["stateful"]
        if item["sink"]=="starrocks.rollup")
    assert [
        item["status"]
        for item in rollup["runtime"]
    ]==["retired","active"]
    assert value["runtime"]["active_plan_version"]==7
    rendered=repr(value)
    assert "do not expose" not in rendered
    assert value["udf_count"]==1
    print(
        "catalog_explain_test ok plan stateless stateful runtime redaction",
        flush=True)


if __name__=="__main__":
    main()
