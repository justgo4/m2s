#!/usr/bin/env python3
"""Contract: aggregate superset state serves a strict aggregate subview."""
from decimal import Decimal
from pathlib import Path
import os
import tempfile
import sys
from unittest.mock import patch

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_log_consumer
import aggregate_outbox
import aggregate_shared_runtime
import aggregate_state
import aggregate_task_catalog
import aggregate_task_runner
import j4
import source_state
import stateful_physical_registry
import task_generation


SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("category","varchar","varchar(32)","NO","utf8mb4_bin",""),
    ("amount","decimal","decimal(18,2)","YES",None,""),
]


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
    ])


def leader_ir():
    return aggregate_ir.compile_sql(
        "db.orders",SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total,"
        "AVG(amount) AS mean FROM arrow_batch GROUP BY category")


def follower_ir():
    return aggregate_ir.compile_sql(
        "db.orders",SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total "
        "FROM arrow_batch GROUP BY category")


def target_schema(ir):
    result=[
        dict(name="category",type="VARCHAR(32)",nullable=False,key=True),
    ]
    for item in ir["aggregates"]:
        if item["function"]=="count":
            result.append(dict(
                name=item["output"],type="BIGINT",
                nullable=False,key=False))
        elif item["function"]=="sum":
            result.append(dict(
                name=item["output"],type="DECIMAL(38,2)",
                nullable=True,key=False))
        else:
            result.append(dict(
                name=item["output"],type="DOUBLE",
                nullable=True,key=False))
    return result


def mapping(task):
    schema_items=[]
    for item in task["target_schema"]:
        name=item["name"]
        if name=="category":
            dtype=pa.string()
        elif name=="n":
            dtype=pa.int64()
        elif name=="total":
            dtype=pa.decimal128(38,2)
        else:
            dtype=pa.float64()
        schema_items.append((name,dtype))
    return dict(
        src_table="orders",
        sr_table=task["target_table"],
        _catalog_sink=task["sink_key"],
        _plan_version=task["plan_version"],
        primary_key="category",
        _output_columns=[
            item["name"] for item in task["target_schema"]
        ],
        _schema=schema_items,
    )


def mutation_table(rows):
    return pa.Table.from_pylist([
        dict(
            id=row[0],category=row[1],amount=row[2],
            _sync_op=row[3],_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(schema())+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def source_commit(con,rows,pos):
    part=source_state.prepare_part(
        "db.orders",mutation_table(rows))
    seq=source_state.log_commit(
        con,"source-subview",
        ("binlog.000001",int(pos)),None,[part])
    source_state.apply_pending(con)
    return seq


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in aggregate_outbox.pending_commits(
        con,consumer_id,limit=1000):
        aggregate_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    leader_plan=leader_ir()
    follower_plan=follower_ir()
    reuse=aggregate_ir.reuse_plan(
        leader_plan,follower_plan)
    assert reuse is not None
    assert reuse["mode"]=="subview"
    assert reuse["surplus_aggregates"]==1

    with tempfile.TemporaryDirectory(
        prefix="m2s-aggregate-subview-"
    ) as td:
        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        source_state.register_relation(
            con,"db.orders","source-subview",
            schema(),["id"],schema_epoch=1)

        aggregate_state.begin_bootstrap(
            con,"leader-state",
            aggregate_ir.state_spec(leader_plan),0)
        aggregate_state.bind_input_semantics(
            con,"leader-state",
            aggregate_ir.semantic_id(leader_plan))
        aggregate_state.apply_bootstrap_chunk(
            con,"leader-state",0,[
                dict(
                    category="a",amount=Decimal("5.00"),
                    _sync_op=0),
            ],None,True)

        leader=aggregate_task_catalog.register_task(
            con,"leader","starrocks.leader",1,
            leader_plan,"leader","leader-state","leader-consumer",
            target_schema(leader_plan))
        task_generation.import_existing(
            con,leader["sink_key"],leader["plan_version"],
            leader["source_relation"],"ready")
        aggregate_log_consumer.ensure_consumer(
            con,leader["consumer_id"],leader["source_relation"],
            leader["plan_version"],leader["ir"],leader["state_id"],0,
            generation_id=leader["generation_id"])
        leader=aggregate_task_catalog.set_status(
            con,leader["task_id"],"active")
        stateful_physical_registry.sync_ready(
            con,"aggregate",leader,0)

        follower=aggregate_task_catalog.register_task(
            con,"follower","starrocks.follower",2,
            follower_plan,"follower","follower-state","follower-consumer",
            target_schema(follower_plan))

        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            first=aggregate_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert first["shared_physical"]
        assert first["shared_reuse_mode"]=="subview"
        assert first["phase"]=="ready"
        try:
            aggregate_state.state_info(
                con,follower["state_id"])
            raise AssertionError(
                "subview follower unexpectedly allocated private state")
        except KeyError:
            pass
        bootstrap_rows=aggregate_outbox.commit_rows(
            con,follower["consumer_id"],0)
        assert len(bootstrap_rows)==1
        assert set(bootstrap_rows[0]["row"])=={
            "category","n","total"}
        assert "mean" not in bootstrap_rows[0]["row"]

        # Derived reuse plan and projected journal survive restart.
        con.close()
        con=j4.init_state(path)
        leader=aggregate_task_catalog.task_info(
            con,leader["task_id"])
        follower=aggregate_task_catalog.task_info(
            con,follower["task_id"])

        assert source_commit(con,[
            (2,"a",Decimal("7.00"),0),
        ],100)==1
        computed=aggregate_log_consumer.process_next(
            con,leader["consumer_id"],leader["ir"])
        assert computed["source_seq"]==1
        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            second=aggregate_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert second["shared_reuse_mode"]=="subview"
        projected=aggregate_outbox.commit_rows(
            con,follower["consumer_id"],1)
        assert len(projected)==1
        assert set(projected[0]["row"])=={
            "category","n","total"}
        assert projected[0]["row"]["n"]==2
        assert projected[0]["row"]["total"]==Decimal("12.00")

        # Owner retirement materializes only the follower's requested
        # accumulator subset, then ordinary incremental maintenance resumes.
        promoted=aggregate_shared_runtime.promote_followers(
            con,leader)
        assert promoted==[
            dict(
                follower_task_id=follower["task_id"],
                state_id=follower["state_id"],
                frontier=1,
            )
        ]
        private=aggregate_state.state_info(
            con,follower["state_id"])
        assert [
            item["output"] for item in private["spec"]["aggregates"]
        ]==["n","total"]
        rows=aggregate_state.read_rows(
            con,follower["state_id"])
        assert rows[0]["n"]==2
        assert rows[0]["total"]==Decimal("12.00")
        assert "mean" not in rows[0]

        assert source_commit(con,[
            (3,"b",Decimal("3.00"),0),
        ],120)==2
        with patch.object(
            aggregate_task_runner.aggregate_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            third=aggregate_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert not third.get("shared_physical",False)
        assert third["consumer"]["watermark"]==2
        by_group={
            row["category"]:row
            for row in aggregate_state.read_rows(
                con,follower["state_id"])
        }
        assert by_group["b"]["n"]==1
        assert by_group["b"]["total"]==Decimal("3.00")
        con.close()

    print(
        "aggregate_subview_shared_runtime_test ok strict_subview "
        "no_private_state projected_journal restart projected_promotion",
        flush=True,
    )


if __name__=="__main__":
    main()
