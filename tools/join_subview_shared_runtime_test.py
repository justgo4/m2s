#!/usr/bin/env python3
"""Contract: JOIN superset state serves a strict projection subview."""
from pathlib import Path
import os
import tempfile
import sys
from unittest.mock import patch

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import join_ir
import join_log_consumer
import join_outbox
import join_shared_runtime
import join_state
import join_target_mapping
import join_task_catalog
import join_task_runner
import source_state
import stateful_physical_registry
import task_generation


LEFT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","bigint","bigint","YES",None,""),
]
RIGHT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def left_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("customer_id",pa.int64()),
        pa.field("amount",pa.int64()),
    ])


def right_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("name",pa.string()),
    ])


def leader_ir():
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        "SELECT l.id AS order_id,r.name AS customer_name,"
        "l.amount AS amount "
        "FROM left_batch l JOIN right_batch r "
        "ON l.customer_id=r.id")


def follower_ir():
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        "SELECT r.name AS customer_name,l.amount AS amount "
        "FROM left_batch l JOIN right_batch r "
        "ON l.customer_id=r.id")


def target_schema(ir):
    result=[
        dict(
            name=join_target_mapping.PAIR_COLUMN,
            type="VARCHAR(1024)",nullable=False,key=True),
    ]
    for item in ir["projections"]:
        result.append(dict(
            name=item["output"],
            type=(
                "VARCHAR(64)"
                if item["column"]=="name"
                else "BIGINT"
            ),
            nullable=True,key=False,
        ))
    return result


def mapping(task):
    return dict(
        src_table="orders",
        sr_table=task["target_table"],
        _catalog_sink=task["sink_key"],
        _plan_version=task["plan_version"],
        primary_key=join_target_mapping.PAIR_COLUMN,
        _output_columns=[
            item["name"] for item in task["target_schema"]
        ],
        _join_pair_identity=join_target_mapping.PAIR_COLUMN,
    )


def batch(schema,rows):
    return pa.Table.from_pylist([
        dict(row,_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(schema)+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def source_commit(con,left_rows,right_rows,pos):
    parts=[]
    if left_rows:
        parts.append(source_state.prepare_part(
            "db.orders",batch(left_schema(),left_rows)))
    if right_rows:
        parts.append(source_state.prepare_part(
            "db.customers",batch(right_schema(),right_rows)))
    seq=source_state.log_commit(
        con,"source-join-subview",
        ("binlog.000001",int(pos)),None,parts)
    source_state.apply_pending(con)
    return seq


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in join_outbox.pending_commits(
        con,consumer_id,limit=1000):
        join_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    leader_plan=leader_ir()
    follower_plan=follower_ir()
    reuse=join_ir.reuse_plan(
        leader_plan,follower_plan)
    assert reuse is not None
    assert reuse["mode"]=="subview"
    assert reuse["surplus_projections"]==1

    with tempfile.TemporaryDirectory(
        prefix="m2s-join-subview-"
    ) as td:
        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        source_state.register_relation(
            con,"db.orders","source-join-subview",
            left_schema(),["id"],schema_epoch=1)
        source_state.register_relation(
            con,"db.customers","source-join-subview",
            right_schema(),["id"],schema_epoch=1)

        join_state.begin_bootstrap(
            con,"leader-state",
            join_ir.state_spec(leader_plan),0)
        join_state.apply_bootstrap_chunk(
            con,"leader-state",0,"left",[
                dict(id=1,customer_id=10,amount=5),
            ],None,True)
        join_state.apply_bootstrap_chunk(
            con,"leader-state",0,"right",[
                dict(id=10,name="alice"),
            ],None,True)

        leader=join_task_catalog.register_task(
            con,"leader","starrocks.join_leader",1,
            leader_plan,"join_leader","leader-state","leader-consumer",
            target_schema(leader_plan))
        task_generation.import_existing_multi(
            con,leader["sink_key"],leader["plan_version"],
            leader["source_relations"],"ready")
        join_log_consumer.ensure_consumer(
            con,leader["consumer_id"],leader["plan_version"],
            leader["ir"],leader["state_id"],0,
            generation_id=leader["generation_id"])
        leader=join_task_catalog.set_status(
            con,leader["task_id"],"active")
        stateful_physical_registry.sync_ready(
            con,"inner_join",leader,0)

        follower=join_task_catalog.register_task(
            con,"follower","starrocks.join_follower",2,
            follower_plan,"join_follower",
            "follower-state","follower-consumer",
            target_schema(follower_plan))

        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            first=join_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert first["shared_physical"]
        assert first["shared_reuse_mode"]=="subview"
        assert first["phase"]=="ready"
        try:
            join_state.state_info(
                con,follower["state_id"])
            raise AssertionError(
                "JOIN subview follower unexpectedly allocated private state")
        except KeyError:
            pass
        bootstrap=join_outbox.commit_rows(
            con,follower["consumer_id"],0)
        assert len(bootstrap)==1
        assert set(bootstrap[0]["row"])=={
            "customer_name","amount"}
        assert "order_id" not in bootstrap[0]["row"]

        con.close()
        con=j4.init_state(path)
        leader=join_task_catalog.task_info(
            con,leader["task_id"])
        follower=join_task_catalog.task_info(
            con,follower["task_id"])

        assert source_commit(con,None,[
            dict(id=10,name="alice",_sync_op=1),
            dict(id=10,name="alicia",_sync_op=0),
        ],100)==1
        computed=join_log_consumer.process_next(
            con,leader["consumer_id"],leader["ir"])
        assert computed["source_seq"]==1
        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            second=join_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert second["shared_reuse_mode"]=="subview"
        projected=join_outbox.commit_rows(
            con,follower["consumer_id"],1)
        assert len(projected)==1
        assert set(projected[0]["row"])=={
            "customer_name","amount"}
        assert projected[0]["row"]==dict(
            customer_name="alicia",amount=5)

        promoted=join_shared_runtime.promote_followers(
            con,leader)
        assert promoted==[
            dict(
                follower_task_id=follower["task_id"],
                state_id=follower["state_id"],
                frontier=1,
            )
        ]
        private=join_state.state_info(
            con,follower["state_id"])
        assert [
            item["output"] for item in private["spec"]["projections"]
        ]==["customer_name","amount"]
        assert join_state.read_rows(
            con,follower["state_id"]
        )==[dict(customer_name="alicia",amount=5)]

        # Private projected state retains only the source columns required by
        # the follower and can continue exact incremental retractions/updates.
        assert source_commit(con,[
            dict(
                id=2,customer_id=10,amount=9,
                _sync_op=0),
        ],None,120)==2
        with patch.object(
            join_task_runner.join_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            third=join_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert not third.get("shared_physical",False)
        assert third["consumer"]["watermark"]==2
        rows=sorted(
            (row["customer_name"],row["amount"])
            for row in join_state.read_rows(
                con,follower["state_id"]))
        assert rows==[("alicia",5),("alicia",9)]
        con.close()

    print(
        "join_subview_shared_runtime_test ok projection_subview "
        "projected_journal restart projected_promotion normal_resume",
        flush=True,
    )


if __name__=="__main__":
    main()
