#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0,str(ROOT))

import j4
import join_job_bridge
import join_outbox
import join_state


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


def mapping():
    return dict(
        src_table="join_sink",
        sr_table="join_sink",
        primary_key=join_job_bridge.PAIR_COLUMN,
        _schema=[
            (join_job_bridge.PAIR_COLUMN,pa.string()),
            ("customer_name",pa.string()),
            ("amount",pa.int64()),
        ],
        _output_columns=[
            join_job_bridge.PAIR_COLUMN,
            "customer_name","amount",
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
    delivery="join-d-%d" % int(job_id)
    with j4.state_transaction(con):
        con.execute("""
            INSERT INTO deliveries(
                id,table_name,lane,plan_version,prepared)
            VALUES(?,?,?,?,1)
        """,(
            delivery,row[0],int(row[1]),int(row[2])))
        con.execute("""
            INSERT INTO job_assignments(
                job_id,delivery_id)
            VALUES(?,?)
        """,(int(job_id),delivery))
        con.execute("""
            INSERT INTO load_parts(
                delivery_id,part,label,payload,nrows,visible)
            VALUES(?,0,?,X'00',1,1)
        """,(delivery,"label-"+delivery))
    return delivery


def stage_delta(con,seq,deltas):
    result=join_state.apply_transaction(
        con,"join-state",seq,[])
    assert result["applied"]
    return join_outbox.enqueue_incremental(
        con,"join-consumer","join-state",seq,deltas)


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-join-bridge-"
    ) as td:
        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        join_state.install(con)
        join_state.create_state(
            con,"join-state",spec(),watermark=0)
        join_outbox.ensure_stream(
            con,"join-consumer","join-state",11,
            "join-generation-11",0)
        join_outbox.seed_bootstrap(
            con,"join-consumer","join-state",11,
            "join-generation-11",0)

        pair_a=b"pair-a"
        pair_b=b"pair-b"
        duplicate=dict(
            customer_name="same",amount=7)
        stage_delta(con,1,[
            dict(
                pair_id=pair_a,op=0,
                row=dict(duplicate)),
            dict(
                pair_id=pair_b,op=0,
                row=dict(duplicate)),
        ])
        stage_delta(con,2,[])
        stage_delta(con,3,[
            dict(
                pair_id=pair_a,op=1,
                row=dict(duplicate)),
            dict(
                pair_id=pair_b,op=0,
                row=dict(
                    customer_name="same",
                    amount=8)),
        ])

        staged=join_job_bridge.stage_pending(
            con,"join-consumer",
            mapping(),cfg(path))
        assert staged
        assert join_outbox.commit_info(
            con,"join-consumer",0)["visible"]
        assert join_outbox.commit_info(
            con,"join-consumer",2)["visible"]
        assert join_outbox.visible_frontier(
            con,"join-consumer")==0

        seq1=[
            int(row[0]) for row in con.execute("""
                SELECT job_id FROM join_job_links
                WHERE consumer_id='join-consumer'
                  AND source_seq=1
                ORDER BY job_id
            """).fetchall()
        ]
        seq3=[
            int(row[0]) for row in con.execute("""
                SELECT job_id FROM join_job_links
                WHERE consumer_id='join-consumer'
                  AND source_seq=3
                ORDER BY job_id
            """).fetchall()
        ]
        assert seq1 and seq3

        # The two identical projected rows must retain two exact pair IDs in
        # the routed durable payload path rather than collapsing by value.
        staged_pairs=[
            item["pair_id"]
            for item in join_outbox.commit_rows(
                con,"join-consumer",1)
        ]
        assert staged_pairs==[pair_a,pair_b]
        wire_a=join_job_bridge._pair_text(pair_a)
        wire_b=join_job_bridge._pair_text(pair_b)
        assert wire_a!=wire_b

        first=create_delivery(con,seq1[0])
        j4.acknowledge_delivery(
            con,first)
        if len(seq1)>1:
            assert not join_outbox.commit_info(
                con,"join-consumer",1)["visible"]
        for job_id in seq1[1:]:
            delivery=create_delivery(
                con,job_id)
            j4.acknowledge_delivery(
                con,delivery)
        assert join_outbox.commit_info(
            con,"join-consumer",1)["visible"]
        assert join_outbox.visible_frontier(
            con,"join-consumer")==2

        for job_id in seq3:
            delivery=create_delivery(
                con,job_id)
            j4.acknowledge_delivery(
                con,delivery)
        assert join_outbox.commit_info(
            con,"join-consumer",3)["visible"]
        assert join_outbox.visible_frontier(
            con,"join-consumer")==3
        assert join_outbox.pending_commits(
            con,"join-consumer")==[]

        while True:
            count,_=j4.retired_job_gc_batch(
                con)
            if not count:
                break
        assert con.execute(
            "SELECT COUNT(*) FROM join_job_links"
        ).fetchone()[0]==0
        assert join_outbox.visible_frontier(
            con,"join-consumer")==3

        try:
            bad=mapping()
            bad["primary_key"]="customer_name"
            join_job_bridge.validate_mapping(
                bad)
            raise AssertionError(
                "JOIN bridge accepted projected-value primary key")
        except ValueError:
            pass

        con.close()

    print(
        "join_job_bridge_test ok exact_pair_identity "
        "duplicate_projection lane_fifo visible_frontier",
        flush=True,
    )


if __name__=="__main__":
    main()
