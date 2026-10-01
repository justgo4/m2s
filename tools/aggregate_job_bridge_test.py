#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import os
import tempfile
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0,str(ROOT))

import aggregate_job_bridge
import aggregate_outbox
import aggregate_state
import j4


def mapping():
    return dict(
        src_table="agg_sink",
        sr_table="agg_sink",
        primary_key="category",
        _schema=[
            ("category",pa.string()),
            ("n",pa.int64()),
            ("total",pa.decimal128(38,2)),
            ("mean",pa.float64()),
        ],
    )


def cfg(path):
    return dict(
        state=path,
        key_partitions=4,
        batch_bytes=1024*1024,
        max_row_bytes=1024*1024,
    )


def create_delivery(con,job_id):
    row=con.execute("""
        SELECT table_name,lane,plan_version
        FROM jobs WHERE id=?
    """,(int(job_id),)).fetchone()
    delivery="d-%d" % int(job_id)
    with j4.state_transaction(con):
        con.execute("""
            INSERT INTO deliveries(
                id,table_name,lane,plan_version,prepared)
            VALUES(?,?,?,?,1)
        """,(delivery,row[0],int(row[1]),int(row[2])))
        con.execute("""
            INSERT INTO job_assignments(job_id,delivery_id)
            VALUES(?,?)
        """,(int(job_id),delivery))
        con.execute("""
            INSERT INTO load_parts(
                delivery_id,part,label,payload,nrows,visible)
            VALUES(?,0,?,X'00',1,1)
        """,(delivery,"label-"+delivery))
    return delivery


def main():
    with tempfile.TemporaryDirectory(prefix="m2s-agg-bridge-") as td:
        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        aggregate_state.install(con)

        spec=aggregate_state.aggregate_spec(
            ["category"],
            [
                dict(output="n",function="count",input="*"),
                dict(output="total",function="sum",input="amount"),
                dict(output="mean",function="avg",input="amount"),
            ])
        aggregate_state.create_state(
            con,"agg-state",spec,watermark=0)
        aggregate_outbox.ensure_stream(
            con,"agg-task","agg-state",7,
            "generation-7",0)
        aggregate_outbox.seed_bootstrap(
            con,"agg-task","agg-state",7,
            "generation-7",0)

        assert aggregate_state.apply_transaction(
            con,"agg-state",1,[
                dict(category="a",amount=Decimal("10.00"),_sync_op=0),
                dict(category="b",amount=Decimal("5.00"),_sync_op=0),
            ])
        aggregate_outbox.enqueue_incremental(
            con,"agg-task","agg-state",1,[
                dict(category="a",amount=Decimal("10.00"),_sync_op=0),
                dict(category="b",amount=Decimal("5.00"),_sync_op=0),
            ])
        assert aggregate_state.apply_transaction(
            con,"agg-state",2,[])
        aggregate_outbox.enqueue_incremental(
            con,"agg-task","agg-state",2,[])
        assert aggregate_state.apply_transaction(
            con,"agg-state",3,[
                dict(category="a",amount=Decimal("10.00"),_sync_op=1),
                dict(category="a",amount=Decimal("20.00"),_sync_op=0),
            ])
        aggregate_outbox.enqueue_incremental(
            con,"agg-task","agg-state",3,[
                dict(category="a",amount=Decimal("10.00"),_sync_op=1),
                dict(category="a",amount=Decimal("20.00"),_sync_op=0),
            ])

        staged=aggregate_job_bridge.stage_pending(
            con,"agg-task",mapping(),cfg(path))
        # bootstrap(0) and empty source commit(2) are immediately visible.
        assert aggregate_outbox.commit_info(
            con,"agg-task",0)["visible"]
        assert aggregate_outbox.commit_info(
            con,"agg-task",2)["visible"]
        assert aggregate_outbox.visible_frontier(
            con,"agg-task")==0

        seq1=_already=[
            row[0] for row in con.execute("""
                SELECT job_id FROM aggregate_job_links
                WHERE consumer_id='agg-task' AND source_seq=1
                ORDER BY job_id
            """).fetchall()
        ]
        seq3=[
            row[0] for row in con.execute("""
                SELECT job_id FROM aggregate_job_links
                WHERE consumer_id='agg-task' AND source_seq=3
                ORDER BY job_id
            """).fetchall()
        ]
        assert seq1 and seq3
        assert all(
            con.execute(
                "SELECT source_seq FROM jobs WHERE id=?",(job_id,)
            ).fetchone()[0] in (1,3)
            for job_id in seq1+seq3)

        # Respect the production invariant UNIQUE(table_name,lane):
        # a lane owns at most one active delivery. Ack one durable lane job at
        # a time; only after the delivery is removed may the lane be reused.
        for job_id in seq1:
            delivery=create_delivery(con,job_id)
            j4.acknowledge_delivery(con,delivery)
        assert aggregate_outbox.commit_info(
            con,"agg-task",1)["visible"]
        # seq 2 is a zero-output commit already marked visible, so the
        # continuous target prefix can now advance through it.
        assert aggregate_outbox.visible_frontier(
            con,"agg-task")==2

        for job_id in seq3:
            delivery=create_delivery(con,job_id)
            j4.acknowledge_delivery(con,delivery)
        assert aggregate_outbox.commit_info(
            con,"agg-task",3)["visible"]
        assert aggregate_outbox.visible_frontier(
            con,"agg-task")==3
        assert aggregate_outbox.pending_commits(
            con,"agg-task")==[]

        # Retired job GC may remove sidecar links without changing the already
        # durable output frontier.
        while True:
            count,_=j4.retired_job_gc_batch(con)
            if not count:
                break
        assert con.execute(
            "SELECT COUNT(*) FROM aggregate_job_links"
        ).fetchone()[0]==0
        assert aggregate_outbox.visible_frontier(
            con,"agg-task")==3
        con.close()

    print(
        "aggregate_job_bridge_test ok jobs lane_fifo "
        "continuous_visible_frontier",
        flush=True,
    )


if __name__=="__main__":
    main()
