#!/usr/bin/env python3
"""Deterministic 1/10/100-task structural A/B for private vs shared aggregate state.

Timing is reported, never asserted. The correctness gate is structural:
private mode maintains one aggregate state per task; shared mode maintains one
leader compute state and N-1 follower journals for the same SQL.
"""
import argparse
from decimal import Decimal
import json
from pathlib import Path
import os
import tempfile
import time
from unittest.mock import patch
import sys

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


def state_bytes(con):
    row=con.execute("""
        SELECT
          COALESCE(SUM(length(spec_json)),0)
          +(SELECT COALESCE(SUM(
              length(key_blob)+length(key_payload)+length(accum_payload)),0)
            FROM aggregate_groups)
        FROM aggregate_states
    """).fetchone()
    return int(row[0] or 0)


def visible(con,consumer_id,_mapping,_cfg):
    for commit in aggregate_outbox.pending_commits(
        con,consumer_id,limit=10000
    ):
        aggregate_outbox.mark_visible(
            con,consumer_id,commit["source_seq"])
    return []


def private_case(task_count,ir):
    with tempfile.TemporaryDirectory(
        prefix="m2s-share-ab-private-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        started=time.perf_counter()
        for index in range(task_count):
            state_id="private-%03d" % index
            aggregate_state.begin_bootstrap(
                con,state_id,aggregate_ir.state_spec(ir),0)
            aggregate_state.bind_input_semantics(
                con,state_id,aggregate_ir.semantic_id(ir))
            aggregate_state.apply_bootstrap_chunk(
                con,state_id,0,[
                    dict(
                        category="a",
                        amount=Decimal("1.00"),
                        _sync_op=0),
                ],None,True)
        bootstrap_seconds=time.perf_counter()-started

        change=dict(
            category="a",amount=Decimal("2.00"),
            _sync_op=0)
        started=time.perf_counter()
        for index in range(task_count):
            aggregate_state.apply_transaction(
                con,"private-%03d" % index,1,[change])
        update_seconds=time.perf_counter()-started
        result=dict(
            mode="private",
            tasks=int(task_count),
            compute_states=int(con.execute(
                "SELECT COUNT(*) FROM aggregate_states"
            ).fetchone()[0]),
            group_rows=int(con.execute(
                "SELECT COUNT(*) FROM aggregate_groups"
            ).fetchone()[0]),
            shared_followers=0,
            state_bytes=state_bytes(con),
            bootstrap_seconds=bootstrap_seconds,
            update_seconds=update_seconds,
            compute_updates=int(task_count),
            follower_journal_copies=0,
        )
        con.close()
        return result


def shared_case(task_count,ir):
    with tempfile.TemporaryDirectory(
        prefix="m2s-share-ab-shared-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        source_state.register_relation(
            con,"db.orders","source-ab",
            schema(),["id"],schema_epoch=1)

        started=time.perf_counter()
        aggregate_state.begin_bootstrap(
            con,"leader-state",aggregate_ir.state_spec(ir),0)
        aggregate_state.bind_input_semantics(
            con,"leader-state",aggregate_ir.semantic_id(ir))
        aggregate_state.apply_bootstrap_chunk(
            con,"leader-state",0,[
                dict(
                    category="a",amount=Decimal("1.00"),
                    _sync_op=0),
            ],None,True)
        leader=aggregate_task_catalog.register_task(
            con,"leader","starrocks.ab_leader",1,ir,
            "ab_leader","leader-state","leader-consumer",
            target_schema())
        task_generation.import_existing(
            con,leader["sink_key"],leader["plan_version"],
            leader["source_relation"],"ready")
        aggregate_log_consumer.ensure_consumer(
            con,leader["consumer_id"],leader["source_relation"],
            leader["plan_version"],ir,leader["state_id"],0,
            generation_id=leader["generation_id"])
        leader=aggregate_task_catalog.set_status(
            con,leader["task_id"],"active")
        stateful_physical_registry.sync_ready(
            con,"aggregate",leader,0)

        followers=[]
        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=visible
        ):
            for index in range(max(0,task_count-1)):
                task=aggregate_task_catalog.register_task(
                    con,"follower-%03d" % index,
                    "starrocks.ab_follower_%03d" % index,
                    index+2,ir,
                    "ab_follower_%03d" % index,
                    "follower-state-%03d" % index,
                    "follower-consumer-%03d" % index,
                    target_schema())
                binding=aggregate_shared_runtime.try_bind(
                    con,task)
                if binding is None:
                    raise AssertionError(
                        "structurally compatible follower did not share")
                result=aggregate_shared_runtime.step(
                    con,task,{}, {})
                if result["phase"]!="ready":
                    raise AssertionError(
                        "shared follower did not become ready")
                followers.append(task)
        bootstrap_seconds=time.perf_counter()-started

        part=source_state.prepare_part(
            "db.orders",mutation_table([
                (2,"a",Decimal("2.00"),0),
            ]))
        source_state.log_commit(
            con,"source-ab",("binlog.000001",100),
            None,[part])
        source_state.apply_pending(con)

        started=time.perf_counter()
        result=aggregate_log_consumer.process_next(
            con,leader["consumer_id"],ir)
        if int(result["source_seq"])!=1:
            raise AssertionError(
                "leader did not consume source seq 1")
        with patch.object(
            aggregate_shared_runtime.aggregate_job_bridge,
            "stage_pending",side_effect=visible
        ):
            for task in followers:
                follower=aggregate_shared_runtime.step(
                    con,task,{}, {})
                if int(follower["consumer"]["watermark"])!=1:
                    raise AssertionError(
                        "shared follower did not copy seq 1")
        update_seconds=time.perf_counter()-started

        result=dict(
            mode="shared",
            tasks=int(task_count),
            compute_states=int(con.execute(
                "SELECT COUNT(*) FROM aggregate_states"
            ).fetchone()[0]),
            group_rows=int(con.execute(
                "SELECT COUNT(*) FROM aggregate_groups"
            ).fetchone()[0]),
            shared_followers=int(con.execute(
                "SELECT COUNT(*) FROM aggregate_shared_followers"
            ).fetchone()[0]),
            state_bytes=state_bytes(con),
            bootstrap_seconds=bootstrap_seconds,
            update_seconds=update_seconds,
            compute_updates=1,
            follower_journal_copies=len(followers),
        )
        con.close()
        return result


def run(task_counts):
    ir=plan()
    rows=[]
    for task_count in task_counts:
        private=private_case(task_count,ir)
        shared=shared_case(task_count,ir)
        if private["compute_states"]!=task_count:
            raise AssertionError(
                "private A/B compute-state count mismatch")
        if shared["compute_states"]!=1:
            raise AssertionError(
                "shared A/B did not collapse compute state")
        if shared["shared_followers"]!=max(0,task_count-1):
            raise AssertionError(
                "shared A/B follower count mismatch")
        if shared["compute_updates"]!=1:
            raise AssertionError(
                "shared A/B update computed more than once")
        rows.append(dict(
            tasks=int(task_count),
            private=private,
            shared=shared,
            state_count_reduction=(
                float(private["compute_states"])
                /float(shared["compute_states"])),
            state_byte_ratio=(
                float(private["state_bytes"])
                /max(1.0,float(shared["state_bytes"]))),
        ))
    return dict(
        format_version=1,
        kind="stateful_sharing_ab",
        note="timings are observational; structural counts are the correctness gate",
        cases=rows,
    )


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tasks",default="1,10,100",
        help="comma-separated positive task counts")
    parser.add_argument(
        "--output",type=Path,
        default=Path(
            "benchmark-results/stateful-sharing-ab.json"))
    args=parser.parse_args()
    task_counts=[
        int(value) for value in str(args.tasks).split(",")
        if str(value).strip()
    ]
    if not task_counts or any(value<1 for value in task_counts):
        raise SystemExit("--tasks must contain positive integers")
    report=run(task_counts)
    args.output.parent.mkdir(
        parents=True,exist_ok=True)
    args.output.write_text(
        json.dumps(report,indent=2,sort_keys=True)+"\n")
    print(
        json.dumps(report,sort_keys=True),
        flush=True)


if __name__=="__main__":
    main()
