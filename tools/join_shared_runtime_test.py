#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile
import time
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
        con,"source-join",("binlog.000001",int(pos)),
        None,parts)
    source_state.apply_pending(con)
    return seq


def make_visible(con,consumer_id,_mapping,_cfg):
    for commit in join_outbox.pending_commits(
        con,consumer_id,limit=1000):
        join_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def main():
    ir=plan()
    with tempfile.TemporaryDirectory(
        prefix="m2s-join-shared-"
    ) as td:
        state_path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(
            state_path)
        source_state.register_relation(
            con,"db.orders","source-join",
            left_schema(),["id"],schema_epoch=1)
        source_state.register_relation(
            con,"db.customers","source-join",
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
            con,"leader-task","starrocks.join_leader",1,
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

        follower=join_task_catalog.register_task(
            con,"follower-task","starrocks.join_follower",2,
            ir,"join_follower","follower-state","follower-consumer",
            target_schema())

        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            first=join_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert first["shared_physical"]
        assert first["phase"]=="ready"
        assert task_generation.source_relations(
            con,follower["sink_key"],follower["plan_version"]
        )==follower["source_relations"]
        try:
            join_state.state_info(
                con,follower["state_id"])
            raise AssertionError(
                "shared JOIN follower unexpectedly allocated private state")
        except KeyError:
            pass
        leader_physical=stateful_physical_registry.instance_id(
            "inner_join",leader)
        assert (
            follower["task_id"],"dependency"
        ) in {
            (row["owner_id"],row["role"])
            for row in physical_state_catalog.state_refs(
                con,leader_physical)
        }

        # Crash/restart must preserve the follower binding, physical
        # dependency ref and independent target/source frontier.
        con.close()
        con=j4.init_state(
            state_path)
        leader=join_task_catalog.task_info(
            con,leader["task_id"])
        follower=join_task_catalog.task_info(
            con,follower["task_id"])
        assert join_shared_runtime.binding_info(
            con,follower["task_id"])["leader_task_id"]==leader["task_id"]

        # One right-side update is computed only by the leader. The follower
        # receives the exact pair-identity/output journal bytes.
        assert source_commit(con,None,[
            dict(id=10,name="alice",_sync_op=1),
            dict(id=10,name="alicia",_sync_op=0),
        ],100)==1
        computed=join_log_consumer.process_next(
            con,leader["consumer_id"],ir)
        assert computed["source_seq"]==1
        with patch.object(
            join_shared_runtime.join_job_bridge,
            "stage_pending",side_effect=make_visible
        ):
            second=join_task_runner.step(
                con,follower["task_id"],{},
                mapping=mapping(follower))
        assert second["shared_physical"]
        assert second["consumer"]["watermark"]==1
        assert join_outbox.commit_info(
            con,follower["consumer_id"],1)["digest"]==(
            join_outbox.commit_info(
                con,leader["consumer_id"],1)["digest"])

        promoted=join_shared_runtime.promote_followers(
            con,leader)
        assert promoted==[
            dict(
                follower_task_id=follower["task_id"],
                state_id=follower["state_id"],
                frontier=1,
            )
        ]
        assert join_shared_runtime.maybe_binding(
            con,follower["task_id"]) is None
        assert join_state.read_rows(
            con,follower["state_id"]
        )==join_state.read_rows(
            con,leader["state_id"])
        follower_consumer=source_state.consumer_info(
            con,follower["consumer_id"])
        assert follower_consumer["metadata"]["kind"]=="inner_join_v1"
        assert join_outbox.stream_info(
            con,follower["consumer_id"]
        )["state_id"]==follower["state_id"]

        # Deterministic stale-result race equivalent to the aggregate case.
        # The second shared step still names the old leader, but durable
        # promotion has already moved the follower stream to private state.
        leader_physical_id=stateful_physical_registry.instance_id(
            "inner_join",leader)
        retired_physical=stateful_physical_registry.retire(
            con,"inner_join",leader)
        assert retired_physical["health"]=="retired"
        assert physical_state_catalog.gc_eligible(
            con,leader_physical_id)
        physical_state_catalog.delete_state(
            con,leader_physical_id)
        resolved=stateful_physical_registry.sync_runtime_result(
            con,dict(kind="inner_join",task=follower),second)
        assert resolved["instance_id"]==stateful_physical_registry.instance_id(
            "inner_join",follower)
        assert (
            follower["task_id"],"owner"
        ) in {
            (row["owner_id"],row["role"])
            for row in physical_state_catalog.state_refs(
                con,resolved["instance_id"])
        }

        now=time.time()
        con.execute("""
            INSERT INTO join_shared_followers(
                follower_task_id,leader_task_id,shared_state_id,
                leader_consumer_id,fixed_w,created,updated)
            VALUES(?,?,?,?,?,?,?)
        """,(
            follower["task_id"],leader["task_id"],
            "missing-shared-state",leader["consumer_id"],
            0,now,now))
        con.execute("""
            UPDATE join_output_streams
            SET state_id=?,updated=?
            WHERE consumer_id=?
        """,(
            "missing-shared-state",now,
            follower["consumer_id"]))
        broken=dict(second)
        broken["shared_state_id"]="missing-shared-state"
        try:
            stateful_physical_registry.sync_runtime_result(
                con,dict(kind="inner_join",task=follower),broken)
            raise AssertionError(
                "dangling shared JOIN binding was accepted")
        except KeyError as exc:
            assert "physical state does not exist" in str(exc)
        con.execute(
            "DELETE FROM join_shared_followers "
            "WHERE follower_task_id=?",
            (follower["task_id"],))
        con.execute("""
            UPDATE join_output_streams
            SET state_id=?,updated=?
            WHERE consumer_id=?
        """,(
            follower["state_id"],time.time(),
            follower["consumer_id"]))

        # After owner promotion the ordinary two-source JOIN consumer resumes
        # at the exact same source frontier.
        assert source_commit(con,[
            dict(
                id=2,customer_id=10,amount=9,
                _sync_op=0),
        ],None,120)==2
        with patch.object(
            join_shared_runtime.join_job_bridge,
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
        "join_shared_runtime_test ok multi_source_import no_private_state "
        "byte_exact_incremental owner_promotion stale_registry_resolution "
        "dangling_shared_fail_closed normal_resume",
        flush=True,
    )


if __name__=="__main__":
    main()
