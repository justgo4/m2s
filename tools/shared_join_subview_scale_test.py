#!/usr/bin/env python3
"""Scale contract: 100 JOIN projection subviews reuse one superset state."""
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


def leader_plan():
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        "SELECT l.id AS order_id,r.name AS customer_name,"
        "l.amount AS amount "
        "FROM left_batch l JOIN right_batch r "
        "ON l.customer_id=r.id")


def follower_plan():
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
        con,"source-join-subview-scale",
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
    leader_ir=leader_plan()
    follower_ir=follower_plan()
    reuse=join_ir.reuse_plan(
        leader_ir,follower_ir)
    assert reuse is not None
    assert reuse["mode"]=="subview"
    assert reuse["surplus_projections"]==1

    with tempfile.TemporaryDirectory(
        prefix="m2s-shared-join-subview-scale-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        source_state.register_relation(
            con,"db.orders","source-join-subview-scale",
            left_schema(),["id"],schema_epoch=1)
        source_state.register_relation(
            con,"db.customers","source-join-subview-scale",
            right_schema(),["id"],schema_epoch=1)

        join_state.begin_bootstrap(
            con,"leader-state",
            join_ir.state_spec(leader_ir),0)
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
            leader_ir,"join_leader","leader-state","leader-consumer",
            target_schema(leader_ir))
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

        followers=[]
        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            for index in range(100):
                task=join_task_catalog.register_task(
                    con,"subview-%03d" % index,
                    "starrocks.join_subview_%03d" % index,
                    index+2,follower_ir,
                    "join_subview_%03d" % index,
                    "state-%03d" % index,
                    "consumer-%03d" % index,
                    target_schema(follower_ir))
                binding=join_shared_runtime.try_bind(
                    con,task)
                assert binding is not None
                result=join_shared_runtime.step(
                    con,task,mapping(task),{})
                assert result["phase"]=="ready"
                assert result["shared_reuse_mode"]=="subview"
                bootstrap=join_outbox.commit_rows(
                    con,task["consumer_id"],0)
                assert len(bootstrap)==1
                assert set(bootstrap[0]["row"])=={
                    "customer_name","amount"}
                followers.append(task)

        assert con.execute(
            "SELECT COUNT(*) FROM join_states"
        ).fetchone()[0]==1
        assert con.execute(
            "SELECT COUNT(*) FROM join_shared_followers"
        ).fetchone()[0]==100
        assert con.execute(
            "SELECT COUNT(*) FROM physical_state_refs "
            "WHERE role='dependency'"
        ).fetchone()[0]==100

        assert source_commit(con,None,[
            dict(id=10,name="alice",_sync_op=1),
            dict(id=10,name="alicia",_sync_op=0),
        ],100)==1
        computed=join_log_consumer.process_next(
            con,leader["consumer_id"],leader_ir)
        assert computed["source_seq"]==1

        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            for task in followers:
                result=join_shared_runtime.step(
                    con,task,mapping(task),{})
                assert result["consumer"]["watermark"]==1
                assert result["shared_reuse_mode"]=="subview"

        assert con.execute(
            "SELECT COUNT(*) FROM join_states"
        ).fetchone()[0]==1
        assert all(
            source_state.consumer_info(
                con,task["consumer_id"])["watermark"]==1
            for task in followers
        )
        assert all(
            join_outbox.commit_rows(
                con,task["consumer_id"],1)[0]["row"]
            ==dict(customer_name="alicia",amount=5)
            for task in followers
        )
        con.close()

    print(
        "shared_join_subview_scale_test ok tasks=100 compute_states=1 "
        "leader_outputs=3 follower_outputs=2 incremental_compute=1",
        flush=True,
    )


if __name__=="__main__":
    main()
