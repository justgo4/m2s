#!/usr/bin/env python3
"""Scale contract: retired shared followers release retention and state refs."""
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
import j4
import physical_state_catalog
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


def plan():
    return aggregate_ir.compile_sql(
        "db.orders",SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total "
        "FROM arrow_batch GROUP BY category")


def target_schema():
    return [
        dict(name="category",type="VARCHAR(32)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
    ]


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in aggregate_outbox.pending_commits(
        con,consumer_id,limit=1000):
        aggregate_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(
        prefix="m2s-shared-follower-gc-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        source_state.register_relation(
            con,"db.orders","source-gc",
            schema(),["id"],schema_epoch=1)

        aggregate_state.begin_bootstrap(
            con,"leader-state",
            aggregate_ir.state_spec(ir),0)
        aggregate_state.bind_input_semantics(
            con,"leader-state",aggregate_ir.semantic_id(ir))
        aggregate_state.apply_bootstrap_chunk(
            con,"leader-state",0,[
                dict(
                    category="a",amount=Decimal("1.00"),
                    _sync_op=0),
            ],None,True)
        leader=aggregate_task_catalog.register_task(
            con,"leader","starrocks.leader",1,ir,
            "leader","leader-state","leader-consumer",target_schema())
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
        physical_id=stateful_physical_registry.instance_id(
            "aggregate",leader)

        followers=[]
        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            for index in range(100):
                task=aggregate_task_catalog.register_task(
                    con,"follower-%03d" % index,
                    "starrocks.follower_%03d" % index,
                    index+2,ir,
                    "follower_%03d" % index,
                    "state-%03d" % index,
                    "consumer-%03d" % index,
                    target_schema())
                assert aggregate_shared_runtime.try_bind(
                    con,task) is not None
                result=aggregate_shared_runtime.step(
                    con,task,{}, {})
                assert result["phase"]=="ready"
                followers.append(task)

        assert len(
            physical_state_catalog.state_refs(
                con,physical_id)
        )==101
        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_shared_followers"
        ).fetchone()[0]==100

        # Retirement alone is not enough to reclaim a follower. Its durable
        # source consumer still pins changelog retention, so GC must fail closed.
        for task in followers:
            aggregate_task_catalog.set_status(
                con,task["task_id"],"retired")
        assert aggregate_shared_runtime.gc_retired_followers(
            con,limit=1000)==[]
        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_shared_followers"
        ).fetchone()[0]==100

        # Once the retirement path releases each source consumer and every
        # output commit is visible, one GC pass may reclaim all follower
        # bindings/outboxes and all dependency refs while preserving the owner.
        for task in followers:
            source_state.remove_consumer(
                con,task["consumer_id"])
        removed=aggregate_shared_runtime.gc_retired_followers(
            con,limit=1000)
        assert len(removed)==100
        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_shared_followers"
        ).fetchone()[0]==0
        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_output_streams "
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
        assert aggregate_state.state_info(
            con,leader["state_id"])["watermark"]==0
        con.close()

    print(
        "shared_follower_gc_scale_test ok retired=100 "
        "retention_fence=1 reclaimed=100 owner_preserved=1",
        flush=True,
    )


if __name__=="__main__":
    main()
