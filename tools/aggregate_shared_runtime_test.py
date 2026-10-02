#!/usr/bin/env python3
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
import physical_state_catalog
import source_state
import stateful_physical_registry
import task_generation


SCHEMA_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("category","varchar","varchar(32)","NO","utf8mb4_bin",""),
    ("amount","decimal","decimal(18,2)","YES",None,""),
]


def source_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
    ])


def ir():
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        "SELECT category,COUNT(*) AS n,SUM(amount) AS total "
        "FROM arrow_batch GROUP BY category")


def target_schema():
    return [
        dict(
            name="category",type="VARCHAR(32)",
            nullable=False,key=True),
        dict(
            name="n",type="BIGINT",
            nullable=False,key=False),
        dict(
            name="total",type="DECIMAL(38,2)",
            nullable=True,key=False),
    ]


def mutation_table(rows):
    return pa.Table.from_pylist([
        dict(
            id=row[0],category=row[1],amount=row[2],
            _sync_op=row[3],_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(source_schema())+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def source_commit(con,seq_rows,pos):
    part=source_state.prepare_part(
        "db.orders",mutation_table(seq_rows))
    seq=source_state.log_commit(
        con,"source-a",("binlog.000001",int(pos)),
        None,[part])
    source_state.apply_pending(con)
    return seq


def mapping(task):
    return dict(
        src_table="orders",
        sr_table=task["target_table"],
        _catalog_sink=task["sink_key"],
        _plan_version=task["plan_version"],
        primary_key="category",
        _output_columns=["category","n","total"],
        _schema=[
            ("category",pa.string()),
            ("n",pa.int64()),
            ("total",pa.decimal128(38,2)),
        ],
    )


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in aggregate_outbox.pending_commits(
        con,consumer_id,limit=1000):
        aggregate_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    plan=ir()
    with tempfile.TemporaryDirectory(
        prefix="m2s-aggregate-shared-"
    ) as td:
        state_path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(
            state_path)
        source_state.register_relation(
            con,"db.orders","source-a",
            source_schema(),["id"],schema_epoch=1)

        # Leader owns the only compute state at W=0.
        aggregate_state.begin_bootstrap(
            con,"leader-state",
            aggregate_ir.state_spec(plan),0)
        aggregate_state.bind_input_semantics(
            con,"leader-state",aggregate_ir.semantic_id(plan))
        aggregate_state.apply_bootstrap_chunk(
            con,"leader-state",0,[
                dict(
                    category="a",amount=Decimal("5.00"),
                    _sync_op=0),
            ],None,True)
        leader=aggregate_task_catalog.register_task(
            con,"leader-task","starrocks.leader",1,
            plan,"leader","leader-state","leader-consumer",
            target_schema())
        task_generation.import_existing(
            con,leader["sink_key"],leader["plan_version"],
            leader["source_relation"],"ready")
        aggregate_log_consumer.ensure_consumer(
            con,leader["consumer_id"],leader["source_relation"],
            leader["plan_version"],leader["ir"],
            leader["state_id"],0,
            generation_id=leader["generation_id"])
        leader=aggregate_task_catalog.set_status(
            con,leader["task_id"],"active")
        stateful_physical_registry.sync_ready(
            con,"aggregate",leader,0)

        follower=aggregate_task_catalog.register_task(
            con,"follower-task","starrocks.follower",2,
            plan,"follower","follower-state","follower-consumer",
            target_schema())

        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            first=aggregate_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert first["shared_physical"]
        assert first["phase"]=="ready"
        assert first["consumer"]["watermark"]==0
        assert aggregate_task_catalog.task_info(
            con,follower["task_id"])["status"]=="active"
        binding=aggregate_shared_runtime.binding_info(
            con,follower["task_id"])
        assert binding["leader_task_id"]==leader["task_id"]
        assert binding["shared_state_id"]==leader["state_id"]
        try:
            aggregate_state.state_info(
                con,follower["state_id"])
            raise AssertionError(
                "shared follower unexpectedly allocated private state")
        except KeyError:
            pass
        leader_physical=physical_state_catalog.state_info(
            con,aggregate_physical_state_id(leader))
        assert (
            follower["task_id"],"dependency"
        ) in {
            (row["owner_id"],row["role"])
            for row in physical_state_catalog.state_refs(
                con,leader_physical["instance_id"])
        }

        # Crash/restart must preserve the follower binding, physical
        # dependency ref and independent target/source frontier.
        con.close()
        con=j4.init_state(
            state_path)
        leader=aggregate_task_catalog.task_info(
            con,leader["task_id"])
        follower=aggregate_task_catalog.task_info(
            con,follower["task_id"])
        assert aggregate_shared_runtime.binding_info(
            con,follower["task_id"])["leader_task_id"]==leader["task_id"]

        # Leader computes source seq 1 once. Follower copies the exact durable
        # output commit bytes and advances its own retention/target frontier.
        assert source_commit(con,[
            (2,"a",Decimal("7.00"),0),
        ],100)==1
        changes=[
            dict(
                category="a",amount=Decimal("7.00"),
                _sync_op=0),
        ]
        aggregate_state.apply_transaction(
            con,leader["state_id"],1,changes)
        aggregate_outbox.enqueue_incremental(
            con,leader["consumer_id"],
            leader["state_id"],1,changes)
        source_state.advance_consumer(
            con,leader["consumer_id"],1)
        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            second=aggregate_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert second["shared_physical"]
        assert second["consumer"]["watermark"]==1
        assert aggregate_outbox.commit_info(
            con,follower["consumer_id"],1)["digest"]==(
            aggregate_outbox.commit_info(
                con,leader["consumer_id"],1)["digest"])
        assert aggregate_state.read_rows(
            con,leader["state_id"]
        )[0]["total"]==Decimal("12.00")

        # Retiring the owner first does not break the follower. It receives a
        # private exact-W clone and ordinary source consumer atomically.
        promoted=aggregate_shared_runtime.promote_followers(
            con,leader)
        assert promoted==[
            dict(
                follower_task_id=follower["task_id"],
                state_id=follower["state_id"],
                frontier=1,
            )
        ]
        assert aggregate_shared_runtime.maybe_binding(
            con,follower["task_id"]) is None
        private=aggregate_state.state_info(
            con,follower["state_id"])
        assert private["watermark"]==1
        assert aggregate_state.read_rows(
            con,follower["state_id"]
        )==aggregate_state.read_rows(
            con,leader["state_id"])
        follower_consumer=source_state.consumer_info(
            con,follower["consumer_id"])
        assert follower_consumer["watermark"]==1
        assert follower_consumer["metadata"]["kind"]=="group_aggregate_v1"
        assert aggregate_outbox.stream_info(
            con,follower["consumer_id"]
        )["state_id"]==follower["state_id"]

        # After promotion the normal aggregate runtime consumes the source log
        # directly; no shared-owner code is needed.
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
        assert by_group["a"]["total"]==Decimal("12.00")
        assert by_group["b"]["total"]==Decimal("3.00")
        con.close()

    print(
        "aggregate_shared_runtime_test ok attach no_private_state "
        "byte_exact_incremental owner_promotion normal_resume",
        flush=True,
    )


def aggregate_physical_state_id(task):
    import aggregate_physical_state
    return aggregate_physical_state.instance_id(
        task["state_id"])


if __name__=="__main__":
    main()
