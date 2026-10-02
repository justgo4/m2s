#!/usr/bin/env python3
"""Scale contract: 100 aggregate subviews reuse one superset compute state."""
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


def leader_plan():
    return aggregate_ir.compile_sql(
        "db.orders",SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total,"
        "AVG(amount) AS mean FROM arrow_batch GROUP BY category")


def follower_plan():
    return aggregate_ir.compile_sql(
        "db.orders",SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total "
        "FROM arrow_batch GROUP BY category")


def leader_schema():
    return [
        dict(name="category",type="VARCHAR(32)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
        dict(name="mean",type="DOUBLE",nullable=True,key=False),
    ]


def follower_schema():
    return [
        dict(name="category",type="VARCHAR(32)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
    ]


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


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in aggregate_outbox.pending_commits(
        con,consumer_id,limit=1000):
        aggregate_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    leader_ir=leader_plan()
    follower_ir=follower_plan()
    assert aggregate_ir.reuse_plan(
        leader_ir,follower_ir)["mode"]=="subview"

    with tempfile.TemporaryDirectory(
        prefix="m2s-shared-subview-scale-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        source_state.register_relation(
            con,"db.orders","source-subview-scale",
            schema(),["id"],schema_epoch=1)

        aggregate_state.begin_bootstrap(
            con,"leader-state",
            aggregate_ir.state_spec(leader_ir),0)
        aggregate_state.bind_input_semantics(
            con,"leader-state",aggregate_ir.semantic_id(leader_ir))
        aggregate_state.apply_bootstrap_chunk(
            con,"leader-state",0,[
                dict(category="a",amount=Decimal("1.00"),_sync_op=0),
            ],None,True)
        leader=aggregate_task_catalog.register_task(
            con,"leader","starrocks.leader",1,leader_ir,
            "leader","leader-state","leader-consumer",leader_schema())
        task_generation.import_existing(
            con,leader["sink_key"],leader["plan_version"],
            leader["source_relation"],"ready")
        aggregate_log_consumer.ensure_consumer(
            con,leader["consumer_id"],leader["source_relation"],
            leader["plan_version"],leader_ir,leader["state_id"],0,
            generation_id=leader["generation_id"])
        leader=aggregate_task_catalog.set_status(
            con,leader["task_id"],"active")
        stateful_physical_registry.sync_ready(
            con,"aggregate",leader,0)

        followers=[]
        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            for index in range(100):
                task=aggregate_task_catalog.register_task(
                    con,"subview-%03d" % index,
                    "starrocks.subview_%03d" % index,
                    index+2,follower_ir,
                    "subview_%03d" % index,
                    "state-%03d" % index,
                    "consumer-%03d" % index,
                    follower_schema())
                binding=aggregate_shared_runtime.try_bind(
                    con,task)
                assert binding is not None
                result=aggregate_shared_runtime.step(
                    con,task,{}, {})
                assert result["phase"]=="ready"
                assert result["shared_reuse_mode"]=="subview"
                bootstrap=aggregate_outbox.commit_rows(
                    con,task["consumer_id"],0)
                assert set(bootstrap[0]["row"])=={
                    "category","n","total"}
                followers.append(task)

        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_states"
        ).fetchone()[0]==1
        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_shared_followers"
        ).fetchone()[0]==100

        part=source_state.prepare_part(
            "db.orders",mutation_table([
                (2,"a",Decimal("2.00"),0),
            ]))
        assert source_state.log_commit(
            con,"source-subview-scale",
            ("binlog.000001",100),None,[part])==1
        source_state.apply_pending(con)
        computed=aggregate_log_consumer.process_next(
            con,leader["consumer_id"],leader_ir)
        assert computed["source_seq"]==1

        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            for task in followers:
                result=aggregate_shared_runtime.step(
                    con,task,{}, {})
                assert result["consumer"]["watermark"]==1
                assert result["shared_reuse_mode"]=="subview"

        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_states"
        ).fetchone()[0]==1
        assert all(
            set(aggregate_outbox.commit_rows(
                con,task["consumer_id"],1)[0]["row"])=={
                    "category","n","total"}
            for task in followers
        )
        assert all(
            aggregate_outbox.commit_rows(
                con,task["consumer_id"],1)[0]["row"]["total"]
            ==Decimal("3.00")
            for task in followers
        )
        con.close()

    print(
        "shared_subview_scale_test ok tasks=100 compute_states=1 "
        "leader_outputs=3 follower_outputs=2 incremental_compute=1",
        flush=True,
    )


if __name__=="__main__":
    main()
