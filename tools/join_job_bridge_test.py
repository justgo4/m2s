#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
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
        stream=join_outbox.stream_info(
            con,"join-consumer")
        assert stream["identity_format"]==join_outbox.IDENTITY_FORMAT_HASH
        wire_a=join_job_bridge._pair_text(pair_a)
        wire_b=join_job_bridge._pair_text(pair_b)
        assert wire_a!=wire_b
        assert len(wire_a)==64 and len(wire_b)==64
        assert join_outbox.target_id_for_pair(
            con,"join-consumer",pair_a)==wire_a
        assert join_outbox.target_id_for_pair(
            con,"join-consumer",pair_b)==wire_b
        assert con.execute("""
            SELECT COUNT(*) FROM join_output_identities
            WHERE consumer_id='join-consumer'
        """).fetchone()[0]==2
        legacy=join_job_bridge._pair_text(
            pair_a,join_outbox.IDENTITY_FORMAT_LEGACY)
        assert legacy!=wire_a and len(legacy)<64

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

        # Collision registry makes the fixed-width key fail closed rather
        # than silently conflating two exact pair identities.
        join_state.create_state(
            con,"collision-state",spec(),watermark=0)
        join_outbox.ensure_stream(
            con,"collision-consumer","collision-state",12,
            "join-generation-12",0)
        join_outbox.seed_bootstrap(
            con,"collision-consumer","collision-state",12,
            "join-generation-12",0)
        pair_c=b"pair-c"
        collision_id=join_outbox.target_id(pair_c)
        con.execute("""
            INSERT INTO join_output_identities(
                consumer_id,target_id,pair_id,created)
            VALUES(?,?,?,0)
        """,(
            "collision-consumer",collision_id,
            b"different-exact-pair"))
        assert join_state.apply_transaction(
            con,"collision-state",1,[])["applied"]
        try:
            join_outbox.enqueue_incremental(
                con,"collision-consumer","collision-state",1,[
                    dict(
                        pair_id=pair_c,op=0,
                        row=dict(duplicate)),
                ])
            raise AssertionError(
                "JOIN target identity collision was not rejected")
        except RuntimeError as exc:
            assert "identity collision" in str(exc)
        try:
            join_outbox.commit_info(
                con,"collision-consumer",1)
            raise AssertionError(
                "colliding JOIN output commit was persisted")
        except KeyError:
            pass

        con.close()

    # Existing pre-format streams must resume with the exact legacy base64
    # identity instead of silently changing target keys after upgrade.
    old=sqlite3.connect(":memory:",isolation_level=None)
    old.execute("PRAGMA foreign_keys=ON")
    old.executescript("""
        CREATE TABLE join_output_streams(
            consumer_id TEXT PRIMARY KEY,
            state_id TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            generation_id TEXT NOT NULL,
            fixed_w INTEGER NOT NULL,
            visible_seq INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        INSERT INTO join_output_streams(
            consumer_id,state_id,plan_version,generation_id,
            fixed_w,visible_seq,created,updated)
        VALUES('old','state',1,'generation',0,-1,0,0);
    """)
    join_outbox.install(old)
    assert join_outbox.stream_info(
        old,"old")["identity_format"]==join_outbox.IDENTITY_FORMAT_LEGACY
    old.close()

    print(
        "join_job_bridge_test ok versioned_pair_identity collision_fence "
        "duplicate_projection lane_fifo visible_frontier",
        flush=True,
    )


if __name__=="__main__":
    main()
