#!/usr/bin/env python3
"""Real StarRocks 4.1.1 contract for the durable INNER JOIN task runtime.

The contract proves the full candidate path through durable source_state,
multi-source fixed-W generation bootstrap, source-log catch-up, JOIN outbox,
ordinary j4 durable jobs, both output protocols, StarRocks VISIBLE
acknowledgement, descriptor-driven restart, duplicate bag rows, fan-out update,
single-pair retract, and zero-output frontier semantics.

Only disposable isolated services are allowed.
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

import j4
import join_ir
import join_outbox
import join_state
import join_target_mapping
import join_task_catalog
import join_task_runner
import source_state
from starrocks_contract import configuration, execute, wait_ready


DATABASE="m2s_join_contract"
LEFT_RELATION="db.orders"
RIGHT_RELATION="db.customers"
SINK_KEY="join_sink"
TARGET_TABLE="join_result"
PLAN_VERSION=61
TASK_ID="join-contract"
STATE_ID="join-state"
CONSUMER_ID="join-consumer"


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
        pa.field("id",pa.int64(),nullable=False),
        pa.field("customer_id",pa.int64()),
        pa.field("amount",pa.int64()),
    ])


def right_schema():
    return pa.schema([
        pa.field("id",pa.int64(),nullable=False),
        pa.field("name",pa.string()),
    ])


def plan():
    return join_ir.compile_sql(
        LEFT_RELATION,LEFT_SIG,"id",
        RIGHT_RELATION,RIGHT_SIG,"id",
        'SELECT r.name AS customer_name,l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.customer_id=r.id',
    )


def target_schema():
    return [
        dict(
            name=join_target_mapping.PAIR_COLUMN,
            type="VARCHAR(1024)",nullable=False,key=True),
        dict(
            name="customer_name",
            type="VARCHAR(64)",nullable=True,key=False),
        dict(
            name="amount",
            type="BIGINT",nullable=True,key=False),
    ]


def snapshot_table(schema,rows):
    return pa.Table.from_pylist(
        rows,schema=schema)


def change_batch(schema,rows):
    return pa.Table.from_pylist([
        dict(row,_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(schema)+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


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


def open_state(path):
    con=j4.init_state(str(path))
    join_state.install(con)
    return con


def register_sources(con):
    source_state.register_relation(
        con,LEFT_RELATION,"source-contract",
        left_schema(),["id"])
    source_state.register_relation(
        con,RIGHT_RELATION,"source-contract",
        right_schema(),["id"])
    source_state.stage_snapshot_batch(
        con,LEFT_RELATION,
        snapshot_table(left_schema(),[
            dict(id=1,customer_id=10,amount=7),
            dict(id=2,customer_id=10,amount=7),
            dict(id=3,customer_id=11,amount=5),
        ]),
        cursor=(3,),is_last=True)
    source_state.stage_snapshot_batch(
        con,RIGHT_RELATION,
        snapshot_table(right_schema(),[
            dict(id=10,name="same"),
            dict(id=11,name="alice"),
        ]),
        cursor=(11,),is_last=True)


def add_commit(
        con,left_rows,right_rows,pos
):
    parts=[]
    if left_rows is not None:
        parts.append(source_state.prepare_part(
            LEFT_RELATION,
            change_batch(left_schema(),left_rows)))
    if right_rows is not None:
        parts.append(source_state.prepare_part(
            RIGHT_RELATION,
            change_batch(right_schema(),right_rows)))
    seq=source_state.log_commit(
        con,"source-contract",
        ("binlog.000001",int(pos)),None,parts)
    source_state.apply_pending(con)
    return seq


def register_descriptor(con,ir):
    return join_task_catalog.register_task(
        con,TASK_ID,SINK_KEY,PLAN_VERSION,
        ir,TARGET_TABLE,STATE_ID,CONSUMER_ID,
        target_schema())


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


def run_until_not_bootstrap(
        con,mapping,cfg,limit=2
):
    status=None
    for _ in range(32):
        status=join_task_runner.step(
            con,TASK_ID,cfg,
            mapping=mapping,
            bootstrap_limit=limit)
        if status["phase"]!="bootstrap":
            return status
    raise RuntimeError(
        "JOIN bootstrap did not finish in bounded steps")


def run_contract(output,load_mode):
    ir=plan()
    with tempfile.TemporaryDirectory(
        prefix="m2s-join-starrocks-"
    ) as td:
        path=Path(td)/"state.sqlite3"
        cfg=config(path,load_mode)
        create_target(cfg)

        con=open_state(path)
        register_sources(con)
        task=register_descriptor(con,ir)
        loaded=join_task_runner.load_task(
            con,TASK_ID,cfg)
        if loaded["task"]["descriptor_hash"]!=task["descriptor_hash"]:
            raise AssertionError(
                "JOIN task descriptor changed during target bind")
        mapping=loaded["mapping"]

        status=run_until_not_bootstrap(
            con,mapping,cfg,limit=2)
        if status["generation"]["fixed_w"]!=0:
            raise AssertionError(
                "JOIN initial fixed-W is not zero")
        if status["generation"]["status"]!="history_staged":
            raise AssertionError(
                "JOIN generation did not enter history_staged")

        bootstrap_deliveries=drain_target(
            con,mapping,cfg)
        if bootstrap_deliveries<1:
            raise AssertionError(
                "JOIN bootstrap produced no real delivery")
        status=join_task_runner.step(
            con,TASK_ID,cfg,mapping=mapping)
        if (
            status["generation"]["status"]!="ready"
            or status["task"]["status"]!="active"
            or status["visible_frontier"]!=0
        ):
            raise AssertionError(
                "JOIN bootstrap was not published after VISIBLE")

        first_rows=target_rows(cfg)
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

        # One source commit updates a right row and must fan out to both left
        # matches. The runtime stages durable jobs before the synthetic restart.
        if add_commit(
            con,None,[
                dict(
                    id=10,name="same",
                    _sync_op=1),
                dict(
                    id=10,name="renamed",
                    _sync_op=0),
            ],100)!=1:
            raise AssertionError(
                "unexpected JOIN update source sequence")
        status=join_task_runner.step(
            con,TASK_ID,cfg,mapping=mapping)
        if (
            status["consumer"]["watermark"]!=1
            or status["visible_frontier"]>=1
        ):
            raise AssertionError(
                "JOIN update visibility fence did not hold")
        staged_jobs=con.execute("""
            SELECT COUNT(*) FROM join_job_links
            WHERE consumer_id=? AND source_seq=1
        """,(CONSUMER_ID,)).fetchone()[0]
        if int(staged_jobs)<1:
            raise AssertionError(
                "JOIN update produced no durable jobs")

        # Restart from durable descriptor and re-read the actual StarRocks
        # target contract. No JOIN recomputation is allowed here.
        con.close()
        con=open_state(path)
        persisted=join_task_catalog.task_info(
            con,TASK_ID)
        if persisted["descriptor_hash"]!=task["descriptor_hash"]:
            raise AssertionError(
                "JOIN descriptor changed across restart")
        loaded=join_task_runner.load_task(
            con,TASK_ID,cfg)
        mapping=loaded["mapping"]
        update_deliveries=drain_target(
            con,mapping,cfg)
        if update_deliveries<1:
            raise AssertionError(
                "JOIN staged update did not survive restart")
        status=join_task_runner.step(
            con,TASK_ID,cfg,mapping=mapping)
        if (
            status["consumer"]["watermark"]!=1
            or status["visible_frontier"]!=1
        ):
            raise AssertionError(
                "JOIN update frontier did not reach seq 1")

        second_rows=target_rows(cfg)
        if projected(second_rows)!=[
            ("alice",5),
            ("renamed",7),
            ("renamed",7),
        ]:
            raise AssertionError(
                "JOIN fan-out target differs: "
                +repr(second_rows))

        # Retract only one left source row; one duplicate bag member remains.
        if add_commit(
            con,[
                dict(
                    id=1,customer_id=10,
                    amount=7,_sync_op=1),
            ],None,120)!=2:
            raise AssertionError(
                "unexpected JOIN retract source sequence")
        status=join_task_runner.step(
            con,TASK_ID,cfg,mapping=mapping)
        if status["consumer"]["watermark"]!=2:
            raise AssertionError(
                "JOIN retract compute frontier did not reach seq 2")
        delete_deliveries=drain_target(
            con,mapping,cfg)
        if delete_deliveries<1:
            raise AssertionError(
                "JOIN retract produced no real delivery")
        status=join_task_runner.step(
            con,TASK_ID,cfg,mapping=mapping)
        if status["visible_frontier"]!=2:
            raise AssertionError(
                "JOIN retract target frontier did not reach seq 2")

        third_rows=target_rows(cfg)
        if projected(third_rows)!=[
            ("alice",5),
            ("renamed",7),
        ]:
            raise AssertionError(
                "JOIN pair delete removed wrong bag member: "
                +repr(third_rows))

        # A source transaction touching neither input advances compute/outbox
        # frontier without manufacturing a StarRocks load.
        if add_commit(
            con,None,None,140)!=3:
            raise AssertionError(
                "unexpected JOIN zero-output source sequence")
        status=join_task_runner.step(
            con,TASK_ID,cfg,mapping=mapping)
        zero_deliveries=drain_target(
            con,mapping,cfg)
        if zero_deliveries!=0:
            raise AssertionError(
                "JOIN zero-output commit manufactured a delivery")
        if (
            status["consumer"]["watermark"]!=3
            or status["visible_frontier"]!=3
        ):
            raise AssertionError(
                "JOIN zero-output frontier did not reach seq 3")
        if target_rows(cfg)!=third_rows:
            raise AssertionError(
                "JOIN zero-output commit changed target rows")

        final_task=join_task_catalog.task_info(
            con,TASK_ID)
        report=dict(
            format_version=1,
            kind="join_starrocks_contract",
            starrocks_version=str(wait_ready(cfg)),
            protocol=cfg["load_mode"],
            pair_key_column=join_target_mapping.PAIR_COLUMN,
            task_status=final_task["status"],
            descriptor_hash=final_task["descriptor_hash"],
            generation_id=status["generation"]["generation_id"],
            fixed_w=status["generation"]["fixed_w"],
            source_relations=final_task["source_relations"],
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
