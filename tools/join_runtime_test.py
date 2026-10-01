#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0,str(ROOT))

import j4
import join_ir
import join_outbox
import join_runtime
import join_state
import join_target_mapping
import source_state
import task_generation


LEFT_RELATION="db.orders"
RIGHT_RELATION="db.customers"
SINK_KEY="join_sink"
PLAN_VERSION=41
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
        LEFT_RELATION,LEFT_SIG,"id",
        RIGHT_RELATION,RIGHT_SIG,"id",
        'SELECT r.name AS customer_name,l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.customer_id=r.id',
    )


def wire_mapping():
    return join_target_mapping.build(
        SINK_KEY,SINK_KEY,
        pa.schema([
            pa.field("customer_name",pa.string()),
            pa.field("amount",pa.int64()),
        ]),
        plan_version=PLAN_VERSION,
    )


def cfg(path):
    return dict(
        state=path,
        key_partitions=4,
        batch_bytes=1024*1024,
        max_row_bytes=1024*1024,
    )


def snapshot_table(schema,rows):
    return pa.Table.from_pylist(rows,schema=schema)


def change_batch(schema,rows):
    return pa.Table.from_pylist([
        dict(row,_sync_order=index)
        for index,row in enumerate(rows)
    ],schema=pa.schema(list(schema)+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def add_commit(con,left_rows,right_rows,pos):
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
        con,"source-1",
        ("binlog.000001",int(pos)),None,parts)
    source_state.apply_pending(con)
    return seq


def open_state(path):
    con=j4.init_state(path)
    join_state.install(con)
    return con


def ack_all(con):
    count=0
    while True:
        row=con.execute("""
            SELECT j.id,j.table_name,j.lane,j.plan_version
            FROM active_jobs j
            LEFT JOIN job_assignments a ON a.job_id=j.id
            WHERE a.job_id IS NULL
            ORDER BY j.id LIMIT 1
        """).fetchone()
        if row is None:
            return count
        job_id,table,lane,plan_version=row
        delivery="join-rt-%d" % int(job_id)
        with j4.state_transaction(con):
            con.execute("""
                INSERT INTO deliveries(
                    id,table_name,lane,plan_version,prepared)
                VALUES(?,?,?,?,1)
            """,(
                delivery,str(table),int(lane),
                int(plan_version)))
            con.execute("""
                INSERT INTO job_assignments(job_id,delivery_id)
                VALUES(?,?)
            """,(int(job_id),delivery))
            con.execute("""
                INSERT INTO load_parts(
                    delivery_id,part,label,payload,nrows,visible)
                VALUES(?,0,?,X'00',1,1)
            """,(delivery,"label-"+delivery))
        j4.acknowledge_delivery(con,delivery)
        count+=1


def main():
    ir=plan()
    mapping=wire_mapping()
    with tempfile.TemporaryDirectory(
        prefix="m2s-join-runtime-"
    ) as td:
        path=os.path.join(td,"state.sqlite3")
        con=open_state(path)
        source_state.register_relation(
            con,LEFT_RELATION,"source-1",
            left_schema(),["id"])
        source_state.register_relation(
            con,RIGHT_RELATION,"source-1",
            right_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,LEFT_RELATION,
            snapshot_table(left_schema(),[
                dict(id=1,customer_id=10,amount=7),
                dict(id=2,customer_id=10,amount=8),
            ]),
            cursor=(2,),is_last=True)
        source_state.stage_snapshot_batch(
            con,RIGHT_RELATION,
            snapshot_table(right_schema(),[
                dict(id=10,name="same"),
            ]),
            cursor=(10,),is_last=True)

        # Establish W=1 before the generation begins.
        assert add_commit(
            con,None,[
                dict(id=10,name="same",_sync_op=1),
                dict(id=10,name="alice",_sync_op=0),
            ],100)==1

        first=join_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,
            ir,STATE_ID,mapping,cfg(path),
            bootstrap_limit=1)
        assert first["phase"]=="bootstrap"
        fixed_w=first["generation"]["fixed_w"]
        assert fixed_w==1
        assert task_generation.source_relations(
            con,SINK_KEY,PLAN_VERSION
        )==[LEFT_RELATION,RIGHT_RELATION]
        con.close()

        # Restart while bootstrap is incomplete; source continues beyond W.
        con=open_state(path)
        assert add_commit(
            con,[
                dict(
                    id=3,customer_id=10,
                    amount=9,_sync_op=0),
            ],None,120)==2

        for _ in range(20):
            status=join_runtime.step(
                con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,
                ir,STATE_ID,mapping,cfg(path),
                bootstrap_limit=1)
            if status["phase"]!="bootstrap":
                break
        assert status["phase"]=="catchup"
        generation=status["generation"]
        assert generation["status"]=="history_staged"
        assert generation["source_pin_released"]
        assert task_generation.source_relations(
            con,SINK_KEY,PLAN_VERSION
        )==[LEFT_RELATION,RIGHT_RELATION]

        # Compute may catch up before the target; ready must remain fenced on
        # target-visible continuous outbox frontier.
        for _ in range(10):
            status=join_runtime.step(
                con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,
                ir,STATE_ID,mapping,cfg(path))
            if status["consumer"]["watermark"]==2:
                break
        assert status["consumer"]["watermark"]==2
        assert status["generation"]["status"]=="history_staged"
        assert status["visible_frontier"]<2
        stream=join_outbox.stream_info(
            con,CONSUMER_ID)
        assert stream["generation_id"]==generation["generation_id"]
        assert stream["fixed_w"]==fixed_w
        con.close()

        # Durable jobs alone are enough to finish publication after restart.
        con=open_state(path)
        assert ack_all(con)>0
        status=join_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,
            ir,STATE_ID,mapping,cfg(path))
        assert status["generation"]["status"]=="ready"
        assert status["phase"]=="ready"
        assert status["visible_frontier"]>=2
        assert status["consumer"]["watermark"]==2
        assert status["generation"]["fixed_w"]==fixed_w

        # Ready remains a live maintenance state. A source transaction touching
        # neither input still advances compute/outbox visibility contiguously.
        assert add_commit(
            con,None,None,140)==3
        status=join_runtime.step(
            con,SINK_KEY,PLAN_VERSION,CONSUMER_ID,
            ir,STATE_ID,mapping,cfg(path))
        assert status["generation"]["status"]=="ready"
        assert status["consumer"]["watermark"]==3
        assert status["visible_frontier"]==3
        assert status["generation"]["fixed_w"]==fixed_w
        con.close()

    print(
        "join_runtime_test ok bootstrap_restart multi_source "
        "catchup target_visibility ready_continuous",
        flush=True,
    )


if __name__=="__main__":
    main()
