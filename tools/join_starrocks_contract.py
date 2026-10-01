#!/usr/bin/env python3
"""Real StarRocks 4.1.1 contract for the first INNER JOIN runtime path.

This focused contract proves durable JOIN pair identity, outbox, ordinary j4
jobs, both supported output protocols, StarRocks VISIBLE acknowledgement,
restart of staged jobs, fan-out update, retract, duplicate projected rows, and
zero-output frontier semantics. Only disposable isolated services are allowed.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_target_mapping
import j4
import join_job_bridge
import join_outbox
import join_state
from starrocks_contract import configuration, execute, wait_ready


DATABASE="m2s_join_contract"
SINK_KEY="join_sink"
TARGET_TABLE="join_result"
PLAN_VERSION=61
STATE_ID="join-state"
CONSUMER_ID="join-consumer"
GENERATION_ID="join-generation-61"


def spec():
    return dict(
        format_version=1,
        kind="inner_join_state",
        sources=dict(
            left=dict(
                relation="db.orders",
                primary_key=["id"],
                join_key=["customer_id"],
            ),
            right=dict(
                relation="db.customers",
                primary_key=["id"],
                join_key=["id"],
            ),
        ),
        projections=[
            dict(
                output="customer_name",
                source="right",column="name"),
            dict(
                output="amount",
                source="left",column="amount"),
        ],
        semantics=dict(
            bag=True,nulls="sql",
            retract="source_pk_pair_identity",
        ),
    )


def public_schema():
    return pa.schema([
        pa.field(
            join_target_mapping.PAIR_COLUMN,
            pa.string(),nullable=False),
        pa.field(
            "customer_name",pa.string()),
        pa.field(
            "amount",pa.int64()),
    ])


def config(path,load_mode):
    cfg=configuration()
    cfg["sr"]["database"]=DATABASE
    cfg.update(
        state=str(path),
        load_mode=str(load_mode),
        key_partitions=4,
        batch_rows=10000,
        batch_bytes=1024*1024,
        batch_ms=0,
        max_row_bytes=1024*1024,
        txn_rows=10000,
        txn_bytes=8*1024*1024,
        max_inflight_deliveries=8,
        max_prepared_bytes=64*1024*1024,
        compression="",
        load_timeout=60,
        query_timeout=15,
        retry_max=6,
        pressure_max_seconds=30,
        commit_interval_ms=200,
        writer_min=1,
        writer_initial=1,
        writer_max=1,
        rowset_yellow=600,
        rowset_red=900,
        version_recovery_checks=2,
        duckdb_memory="64MB",
        catalog_macros=(),
        catalog_udfs=(),
    )
    if cfg["load_mode"] not in {
        "transaction","merge_async"
    }:
        raise ValueError(
            "unsupported JOIN contract load mode")
    return cfg


def create_target(cfg):
    wait_ready(cfg)
    execute(
        cfg,"CREATE DATABASE IF NOT EXISTS "+DATABASE)
    execute(
        cfg,"DROP TABLE IF EXISTS "
        +DATABASE+"."+TARGET_TABLE)
    ddl=(
        "CREATE TABLE "+DATABASE+"."+TARGET_TABLE+"("
        +join_target_mapping.PAIR_COLUMN
        +" VARCHAR(1024) NOT NULL,"
        "customer_name VARCHAR(64) NULL,"
        "amount BIGINT NULL"
        ") PRIMARY KEY("
        +join_target_mapping.PAIR_COLUMN+") "
        "DISTRIBUTED BY HASH("
        +join_target_mapping.PAIR_COLUMN+") BUCKETS 1 "
        'PROPERTIES("replication_num"="1")'
    )
    deadline=time.monotonic()+90
    while True:
        try:
            execute(cfg,ddl)
            return
        except j4.pymysql.err.ProgrammingError as exc:
            if (
                "backends without enough disk space"
                not in str(exc).lower()
                or time.monotonic()>=deadline
            ):
                raise
            time.sleep(1)


def bind_mapping(cfg):
    mapping=join_target_mapping.build(
        SINK_KEY,TARGET_TABLE,
        pa.schema([
            pa.field("customer_name",pa.string()),
            pa.field("amount",pa.int64()),
        ]),
        plan_version=PLAN_VERSION)
    rows,_=execute(
        cfg,"SHOW COLUMNS FROM "
        +DATABASE+"."+TARGET_TABLE)
    mapping=join_target_mapping.bind_target(
        mapping,{str(row[0]):row for row in rows})
    return join_job_bridge.validate_mapping(
        mapping)


def open_state(path):
    con=j4.init_state(str(path))
    join_state.install(con)
    return con


def _target_runtime(table,cfg):
    stop=threading.Event()
    now=time.time()
    return dict(
        stop=stop,
        control_lock=threading.RLock(),
        pressure_until={table:0},
        table_interval={
            table:cfg["commit_interval_ms"]/1000},
        active_writers={table:1},
        last_pressure={table:now},
        last_scale={table:now},
        version_recovery={table:False},
        version_recovery_good={table:0},
        resource_writer_cap=1,
        quarantined_tables={},
        load_events={table:threading.Event()},
    )


def _drain_transaction(
        con,engine,handle,mapping,cfg,runtime
):
    table=j4.mapping_key(mapping)
    delivery=j4.claim_table_delivery(
        con,table,cfg)
    if delivery is None:
        return False
    if not j4.prepare_delivery(
        con,engine,mapping,delivery,cfg
    ):
        raise RuntimeError(
            "JOIN transaction could not reserve payload")
    j4.load_transaction(
        handle,con,mapping,delivery,cfg,runtime)
    invisible=con.execute(
        "SELECT COUNT(*) FROM load_parts "
        "WHERE delivery_id=? AND visible=0",
        (delivery,),
    ).fetchone()[0]
    if int(invisible):
        raise RuntimeError(
            "JOIN transaction returned before VISIBLE")
    j4.acknowledge_delivery(
        con,delivery)
    return True


def _drain_merge_async(
        con,engine,handle,mapping,cfg,runtime
):
    table=j4.mapping_key(mapping)
    for lane in j4.merge_candidate_lanes(
        con,table
    ):
        delivery=j4.claim_cdc_bundle(
            con,table,int(lane),cfg,runtime)
        if delivery is None:
            continue
        if not j4.prepare_delivery(
            con,engine,mapping,delivery,cfg
        ):
            raise RuntimeError(
                "JOIN merge delivery could not reserve payload")
        j4.merge_async_delivery(
            handle,con,mapping,delivery,cfg,runtime)
        invisible=con.execute(
            "SELECT COUNT(*) FROM load_parts "
            "WHERE delivery_id=? AND visible=0",
            (delivery,),
        ).fetchone()[0]
        if int(invisible):
            raise RuntimeError(
                "JOIN merge returned before VISIBLE")
        j4.acknowledge_delivery(
            con,delivery)
        return True
    return False


def drain_target(con,mapping,cfg):
    table=j4.mapping_key(mapping)
    runtime=_target_runtime(
        table,cfg)
    engine=j4.transform_engine(
        cfg)
    handle=j4.pycurl.Curl()
    deliveries=0
    try:
        drain=(
            _drain_merge_async
            if cfg["load_mode"]=="merge_async"
            else _drain_transaction)
        while True:
            if drain(
                con,engine,handle,mapping,
                cfg,runtime
            ):
                deliveries+=1
                continue
            pending=con.execute(
                "SELECT COUNT(*) FROM active_jobs "
                "WHERE table_name=?",
                (table,),
            ).fetchone()[0]
            if not int(pending):
                break
            raise RuntimeError(
                "JOIN target has pending jobs but "
                "no claimable delivery")
    finally:
        handle.close()
        engine.close()
    return deliveries


def target_rows(cfg):
    rows,_=execute(
        cfg,
        "SELECT "
        +join_target_mapping.PAIR_COLUMN
        +",customer_name,amount FROM "
        +DATABASE+"."+TARGET_TABLE
        +" ORDER BY "
        +join_target_mapping.PAIR_COLUMN)
    result=[
        dict(
            pair_id=str(row[0]),
            customer_name=(
                None if row[1] is None
                else str(row[1])),
            amount=(
                None if row[2] is None
                else int(row[2])),
        )
        for row in rows
    ]
    if len({
        item["pair_id"] for item in result
    })!=len(result):
        raise AssertionError(
            "JOIN target pair identity is not unique")
    return result


def projected(rows):
    return sorted(
        (
            item["customer_name"],
            item["amount"],
        )
        for item in rows
    )


def stage_and_drain(
        con,mapping,cfg,expect_delivery=True
):
    staged=join_job_bridge.stage_pending(
        con,CONSUMER_ID,mapping,cfg)
    deliveries=drain_target(
        con,mapping,cfg)
    if expect_delivery and deliveries<1:
        raise AssertionError(
            "JOIN target produced no real delivery")
    return staged,deliveries


def run_contract(output,load_mode):
    with tempfile.TemporaryDirectory(
        prefix="m2s-join-starrocks-"
    ) as td:
        path=Path(td)/"state.sqlite3"
        cfg=config(
            path,load_mode)
        create_target(cfg)
        mapping=bind_mapping(cfg)

        con=open_state(path)
        join_state.begin_bootstrap(
            con,STATE_ID,spec(),0)
        join_state.apply_bootstrap_chunk(
            con,STATE_ID,0,"left",[
                dict(
                    id=1,customer_id=10,
                    amount=7),
                dict(
                    id=2,customer_id=10,
                    amount=7),
                dict(
                    id=3,customer_id=11,
                    amount=5),
            ],b"left-last",True)
        join_state.apply_bootstrap_chunk(
            con,STATE_ID,0,"right",[
                dict(id=10,name="same"),
                dict(id=11,name="alice"),
            ],b"right-last",True)
        join_outbox.ensure_stream(
            con,CONSUMER_ID,STATE_ID,
            PLAN_VERSION,GENERATION_ID,0)
        join_outbox.seed_bootstrap(
            con,CONSUMER_ID,STATE_ID,
            PLAN_VERSION,GENERATION_ID,0)

        _,bootstrap_deliveries=stage_and_drain(
            con,mapping,cfg)
        if join_outbox.visible_frontier(
            con,CONSUMER_ID
        )!=0:
            raise AssertionError(
                "JOIN bootstrap target frontier is not visible")
        first_rows=target_rows(
            cfg)
        if projected(first_rows)!=[
            ("alice",5),
            ("same",7),
            ("same",7),
        ]:
            raise AssertionError(
                "JOIN bootstrap duplicate bag rows differ: "
                +repr(first_rows))
        if len(first_rows)!=3:
            raise AssertionError(
                "JOIN duplicate projected rows collapsed")

        # One right row update fans out to both matching left rows.
        result=join_state.apply_transaction(
            con,STATE_ID,1,[
                (
                    "right",
                    dict(
                        id=10,name="same",
                        _sync_op=1),
                ),
                (
                    "right",
                    dict(
                        id=10,name="renamed",
                        _sync_op=0),
                ),
            ])
        if len(result["deltas"])!=2:
            raise AssertionError(
                "JOIN fan-out update did not emit two pair mutations")
        join_outbox.enqueue_incremental(
            con,CONSUMER_ID,STATE_ID,1,
            result["deltas"])
        staged=join_job_bridge.stage_pending(
            con,CONSUMER_ID,mapping,cfg)
        if not any(
            item["job_ids"] for item in staged
        ):
            raise AssertionError(
                "JOIN fan-out update produced no durable jobs")

        # Restart with jobs already staged. No recompute is allowed.
        con.close()
        con=open_state(path)
        update_deliveries=drain_target(
            con,mapping,cfg)
        if update_deliveries<1:
            raise AssertionError(
                "JOIN staged update did not survive restart")
        if join_outbox.visible_frontier(
            con,CONSUMER_ID
        )!=1:
            raise AssertionError(
                "JOIN update frontier did not reach seq 1")
        second_rows=target_rows(
            cfg)
        if projected(second_rows)!=[
            ("alice",5),
            ("renamed",7),
            ("renamed",7),
        ]:
            raise AssertionError(
                "JOIN fan-out target differs: "
                +repr(second_rows))

        # Retract only one left source row; one duplicate must remain.
        result=join_state.apply_transaction(
            con,STATE_ID,2,[
                (
                    "left",
                    dict(
                        id=1,customer_id=10,
                        amount=7,_sync_op=1),
                ),
            ])
        if (
            len(result["deltas"])!=1
            or int(result["deltas"][0]["op"])!=1
        ):
            raise AssertionError(
                "JOIN retract did not emit one pair delete")
        join_outbox.enqueue_incremental(
            con,CONSUMER_ID,STATE_ID,2,
            result["deltas"])
        _,delete_deliveries=stage_and_drain(
            con,mapping,cfg)
        third_rows=target_rows(
            cfg)
        if projected(third_rows)!=[
            ("alice",5),
            ("renamed",7),
        ]:
            raise AssertionError(
                "JOIN pair delete removed wrong bag member: "
                +repr(third_rows))

        # Zero-output source sequence advances the durable visible frontier
        # without manufacturing a StarRocks load.
        result=join_state.apply_transaction(
            con,STATE_ID,3,[])
        join_outbox.enqueue_incremental(
            con,CONSUMER_ID,STATE_ID,3,
            result["deltas"])
        _,zero_deliveries=stage_and_drain(
            con,mapping,cfg,expect_delivery=False)
        if zero_deliveries!=0:
            raise AssertionError(
                "JOIN zero-output commit manufactured a delivery")
        if join_outbox.visible_frontier(
            con,CONSUMER_ID
        )!=3:
            raise AssertionError(
                "JOIN zero-output frontier did not reach seq 3")
        if target_rows(cfg)!=third_rows:
            raise AssertionError(
                "JOIN zero-output commit changed target rows")

        report=dict(
            format_version=1,
            kind="join_starrocks_contract",
            starrocks_version=str(wait_ready(cfg)),
            protocol=cfg["load_mode"],
            pair_key_column=join_target_mapping.PAIR_COLUMN,
            bootstrap_deliveries=bootstrap_deliveries,
            update_deliveries=update_deliveries,
            delete_deliveries=delete_deliveries,
            zero_output_deliveries=zero_deliveries,
            restart_boundaries=1,
            visible_frontier=join_outbox.visible_frontier(
                con,CONSUMER_ID),
            first_projected=projected(first_rows),
            update_projected=projected(second_rows),
            final_projected=projected(third_rows),
        )
        con.close()

    output.parent.mkdir(
        parents=True,exist_ok=True)
    output.write_text(
        json.dumps(
            report,indent=2,sort_keys=True
        )+"\n")
    print(
        json.dumps(report,sort_keys=True),
        flush=True)


def main():
    parser=argparse.ArgumentParser(
        description=__doc__)
    parser.add_argument(
        "--isolated",action="store_true",
        help="required acknowledgement that services are disposable")
    parser.add_argument(
        "--load-mode",
        choices=("transaction","merge_async"),
        default="transaction")
    parser.add_argument(
        "--output",type=Path,
        default=Path(
            "benchmark-results/join-starrocks-contract.json"))
    args=parser.parse_args()
    if not args.isolated:
        raise SystemExit(
            "refuse to run JOIN StarRocks contract without --isolated")
    run_contract(
        args.output,args.load_mode)


if __name__=="__main__":
    main()
