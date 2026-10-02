#!/usr/bin/env python3
"""Scale contract: retired shared JOIN followers reclaim safely."""
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
import physical_state_catalog
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


def plan():
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        "SELECT r.name AS customer_name,l.amount AS amount "
        "FROM left_batch l JOIN right_batch r "
        "ON l.customer_id=r.id")


def target_schema():
    return [
        dict(
            name=join_target_mapping.PAIR_COLUMN,
            type="VARCHAR(1024)",nullable=False,key=True),
        dict(
            name="customer_name",type="VARCHAR(64)",
            nullable=True,key=False),
        dict(
            name="amount",type="BIGINT",
            nullable=True,key=False),
    ]


def mapping(task):
    return dict(
        src_table="orders",
        sr_table=task["target_table"],
        _catalog_sink=task["sink_key"],
        _plan_version=task["plan_version"],
        primary_key=join_target_mapping.PAIR_COLUMN,
        _output_columns=[
            join_target_mapping.PAIR_COLUMN,
            "customer_name","amount",
        ],
        _join_pair_identity=join_target_mapping.PAIR_COLUMN,
    )


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in join_outbox.pending_commits(
        con,consumer_id,limit=1000):
        join_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(
        prefix="m2s-shared-join-follower-gc-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        source_state.register_relation(
            con,"db.orders","source-join-gc",
            left_schema(),["id"],schema_epoch=1)
        source_state.register_relation(
            con,"db.customers","source-join-gc",
            right_schema(),["id"],schema_epoch=1)

        join_state.begin_bootstrap(
            con,"leader-state",
            join_ir.state_spec(ir),0)
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
            ir,"join_leader","leader-state","leader-consumer",
            target_schema())
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
        physical_id=stateful_physical_registry.instance_id(
            "inner_join",leader)

        followers=[]
        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            for index in range(100):
                task=join_task_catalog.register_task(
                    con,"follower-%03d" % index,
                    "starrocks.join_follower_%03d" % index,
                    index+2,ir,
                    "join_follower_%03d" % index,
                    "state-%03d" % index,
                    "consumer-%03d" % index,
                    target_schema())
                assert join_shared_runtime.try_bind(
                    con,task) is not None
                result=join_shared_runtime.step(
                    con,task,mapping(task),{})
                assert result["phase"]=="ready"
                followers.append(task)

        assert len(
            physical_state_catalog.state_refs(
                con,physical_id)
        )==101
        assert con.execute(
            "SELECT COUNT(*) FROM join_shared_followers"
        ).fetchone()[0]==100

        for task in followers:
            join_task_catalog.set_status(
                con,task["task_id"],"retired")
        assert join_shared_runtime.gc_retired_followers(
            con,limit=1000)==[]
        assert con.execute(
            "SELECT COUNT(*) FROM join_shared_followers"
        ).fetchone()[0]==100

        for task in followers:
            source_state.remove_consumer(
                con,task["consumer_id"])
        removed=join_shared_runtime.gc_retired_followers(
            con,limit=1000)
        assert len(removed)==100
        assert con.execute(
            "SELECT COUNT(*) FROM join_shared_followers"
        ).fetchone()[0]==0
        assert con.execute(
            "SELECT COUNT(*) FROM join_output_streams "
            "WHERE consumer_id LIKE 'consumer-%'"
        ).fetchone()[0]==0
        refs=physical_state_catalog.state_refs(
            con,physical_id)
        assert [
            (item["owner_id"],item["role"])
            for item in refs
        ]==[("leader","owner")]
        assert source_state.consumer_info(
            con,leader["consumer_id"])["watermark"]==0
        assert join_state.state_info(
            con,leader["state_id"])["watermark"]==0
        con.close()

    print(
        "shared_join_follower_gc_scale_test ok retired=100 "
        "retention_fence=1 reclaimed=100 owner_preserved=1",
        flush=True,
    )


if __name__=="__main__":
    main()
