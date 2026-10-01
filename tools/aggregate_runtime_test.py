#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import os
import tempfile

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0,str(ROOT))

import aggregate_ir
import aggregate_outbox
import aggregate_target_mapping
import aggregate_task_catalog
import aggregate_task_runner
import aggregate_state
import j4
import source_state


SCHEMA_SIG=[
    ("id","bigint","bigint",None,None,False),
    ("category","varchar","varchar(16)",None,"utf8mb4_bin",True),
    ("amount","decimal","decimal(18,2)",None,None,True),
    ("active","tinyint","tinyint",None,None,False),
]


def source_schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("category",pa.string()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("active",pa.int8()),
    ])


def source_table(rows):
    return pa.Table.from_pylist([
        dict(id=r[0],category=r[1],amount=r[2],active=r[3])
        for r in rows
    ],schema=source_schema())


def source_batch(rows):
    return pa.Table.from_pylist([
        dict(
            id=r[0],category=r[1],amount=r[2],active=r[3],
            _sync_op=r[4],_sync_order=i)
        for i,r in enumerate(rows)
    ],schema=pa.schema(list(source_schema())+[
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))


def ir():
    return aggregate_ir.compile_sql(
        "db.orders",SCHEMA_SIG,
        'SELECT "category",COUNT(*) AS "n",'
        'SUM("amount") AS "total",AVG("amount") AS "mean" '
        'FROM arrow_batch WHERE "active"=1 GROUP BY "category"',
        source_filter='"amount" IS NULL OR "amount">-1000',
    )


def target_schema():
    return [
        dict(name="category",type="VARCHAR(16)",nullable=False,key=True),
        dict(name="n",type="BIGINT",nullable=False,key=False),
        dict(name="total",type="DECIMAL(38,2)",nullable=True,key=False),
        dict(name="mean",type="DOUBLE",nullable=True,key=False),
    ]


def register_task(con,plan):
    return aggregate_task_catalog.register_task(
        con,"agg-task","agg_sink",31,plan,"agg_sink",
        "agg-state","agg-consumer",target_schema())


def mapping(task):
    return aggregate_target_mapping.mapping_from_descriptor(task)


def cfg(path):
    return dict(
        state=path,key_partitions=4,
        batch_bytes=1024*1024,max_row_bytes=1024*1024,
    )


def add_commit(con,rows,pos):
    parts=[]
    if rows is not None:
        parts.append(source_state.prepare_part(
            "db.orders",source_batch(rows)))
    seq=source_state.log_commit(
        con,"source-1",("binlog.000001",pos),None,parts)
    source_state.apply_pending(con)
    return seq


def ack_all(con):
    jobs=[
        int(row[0]) for row in con.execute("""
            SELECT j.id
            FROM active_jobs j
            LEFT JOIN job_assignments a ON a.job_id=j.id
            WHERE a.job_id IS NULL
            ORDER BY j.id
        """).fetchall()
    ]
    for job_id in jobs:
        table,lane,plan=con.execute("""
            SELECT table_name,lane,plan_version
            FROM jobs WHERE id=?
        """,(job_id,)).fetchone()
        delivery="rt-%d" % job_id
        with j4.state_transaction(con):
            con.execute("""
                INSERT INTO deliveries(
                    id,table_name,lane,plan_version,prepared)
                VALUES(?,?,?,?,1)
            """,(delivery,table,int(lane),int(plan)))
            con.execute("""
                INSERT INTO job_assignments(job_id,delivery_id)
                VALUES(?,?)
            """,(job_id,delivery))
            con.execute("""
                INSERT INTO load_parts(
                    delivery_id,part,label,payload,nrows,visible)
                VALUES(?,0,?,X'00',1,1)
            """,(delivery,"label-"+delivery))
        j4.acknowledge_delivery(con,delivery)
    return len(jobs)


def main():
    plan=ir()
    with tempfile.TemporaryDirectory(prefix="m2s-agg-runtime-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        aggregate_state.install(con)
        source_state.register_relation(
            con,"db.orders","source-1",source_schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.orders",
            source_table([
                (1,"a",Decimal("10.00"),1),
                (2,"b",Decimal("5.00"),1),
            ]),
            cursor=(2,),is_last=True)
        assert add_commit(con,[
            (2,"b",Decimal("5.00"),1,1),
            (2,"a",Decimal("20.00"),1,0),
        ],100)==1
        task=register_task(con,plan)
        wire_mapping=mapping(task)

        first=aggregate_task_runner.step(
            con,"agg-task",cfg(path),
            mapping=wire_mapping,bootstrap_limit=1)
        assert first["phase"]=="bootstrap"
        fixed_w=first["generation"]["fixed_w"]
        assert fixed_w==1
        con.close()

        # Restart during bootstrap and let source advance after the fixed cut.
        con=j4.init_state(path)
        aggregate_state.install(con)
        assert add_commit(con,[
            (3,"c",Decimal("30.00"),1,0),
        ],120)==2

        for _ in range(20):
            status=aggregate_task_runner.step(
                con,"agg-task",cfg(path),
                mapping=wire_mapping,bootstrap_limit=1)
            if status["phase"]!="bootstrap":
                break
        assert status["phase"]=="catchup"
        generation_id=status["generation"]["generation_id"]
        stream=aggregate_outbox.stream_info(
            con,"agg-consumer")
        assert stream["generation_id"]==generation_id
        assert stream["fixed_w"]==fixed_w

        # Runtime may consume source ahead of target visibility, but cannot
        # publish the generation until the durable output prefix is visible.
        for _ in range(10):
            status=aggregate_task_runner.step(
                con,"agg-task",cfg(path),mapping=wire_mapping)
            if status["consumer"]["watermark"]==2:
                break
        assert status["consumer"]["watermark"]==2
        assert status["generation"]["status"]=="history_staged"
        assert status["visible_frontier"]<2
        con.close()

        # Restart with durable jobs already staged; no recomputation/reacquire W.
        con=j4.init_state(path)
        aggregate_state.install(con)
        persisted=aggregate_task_catalog.task_info(con,"agg-task")
        assert persisted["descriptor_hash"]==task["descriptor_hash"]
        wire_mapping=mapping(persisted)
        assert ack_all(con)>0
        status=aggregate_task_runner.step(
            con,"agg-task",cfg(path),mapping=wire_mapping)
        assert status["generation"]["status"]=="ready"
        assert status["task"]["status"]=="active"
        assert status["visible_frontier"]>=2
        assert status["consumer"]["watermark"]==2

        # Ready is a lifecycle milestone, not a stop condition. New source
        # commits continue through state/outbox/jobs without changing W.
        assert add_commit(con,None,140)==3
        status=aggregate_task_runner.step(
            con,"agg-task",cfg(path),mapping=wire_mapping)
        assert status["generation"]["status"]=="ready"
        assert status["consumer"]["watermark"]==3
        # Zero-output source commit is immediately target-visible once prior
        # output is already visible.
        assert status["visible_frontier"]==3
        assert status["generation"]["fixed_w"]==fixed_w
        con.close()

    print(
        "aggregate_runtime_test ok bootstrap_restart catchup "
        "target_visibility ready_continuous",
        flush=True,
    )


if __name__=="__main__":
    main()
