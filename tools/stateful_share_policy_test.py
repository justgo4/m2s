#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile
from unittest.mock import patch
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_state
import j4
import join_state
import stateful_share_policy


def candidate(task_id,state_id,watermark,reuse_mode="exact",surplus=0):
    return (
        dict(task_id=task_id,state_id=state_id),
        dict(),
        dict(watermark=watermark),
        dict(health="ready"),
        dict(
            mode=reuse_mode,
            surplus_aggregates=surplus,
            surplus_projections=surplus,
        ),
    )


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-share-policy-"
    ) as td:
        con=j4.init_state(
            os.path.join(td,"state.sqlite3"))
        con.execute("""
            INSERT OR REPLACE INTO source_state_meta(key,value)
            VALUES('base_applied_seq','10')
        """)
        task=dict(task_id="follower")

        aggregate_state.create_state(
            con,"size-agg",
            aggregate_state.aggregate_spec(
                ["k"],[
                    dict(
                        output="n",
                        function="count",
                        input="*"),
                ]),
            0)
        con.execute("""
            INSERT INTO aggregate_groups(
                state_id,key_blob,key_payload,row_count,accum_payload)
            VALUES('size-agg',X'01',X'0203',1,X'040506')
        """)
        assert stateful_share_policy._state_stats(
            con,"aggregate","size-agg"
        )==dict(rows=1,payload_bytes=6)
        con.execute("""
            UPDATE aggregate_groups
            SET accum_payload=X'04050607'
            WHERE state_id='size-agg' AND key_blob=X'01'
        """)
        assert stateful_share_policy._state_stats(
            con,"aggregate","size-agg"
        )==dict(rows=1,payload_bytes=7)
        con.execute("""
            DELETE FROM aggregate_groups
            WHERE state_id='size-agg' AND key_blob=X'01'
        """)
        assert stateful_share_policy._state_stats(
            con,"aggregate","size-agg"
        )==dict(rows=0,payload_bytes=0)

        join_spec=dict(
            format_version=1,
            kind="inner_join_state",
            sources=dict(
                left=dict(
                    relation="db.left",
                    primary_key=["id"],
                    join_key=["k"]),
                right=dict(
                    relation="db.right",
                    primary_key=["id"],
                    join_key=["k"]),
            ),
            projections=[
                dict(
                    output="v",
                    source="left",
                    column="v"),
            ],
            semantics=dict(
                bag=True,nulls="sql",
                retract="source_pk_pair_identity"),
        )
        join_state.create_state(
            con,"size-join",join_spec,0)
        con.execute("""
            INSERT INTO join_rows(
                state_id,side,pk_blob,join_blob,row_payload)
            VALUES('size-join','left',X'01',X'0203',X'040506')
        """)
        assert stateful_share_policy._state_stats(
            con,"inner_join","size-join"
        )==dict(rows=1,payload_bytes=6)
        con.execute("""
            UPDATE join_rows
            SET join_blob=NULL,row_payload=X'04050607'
            WHERE state_id='size-join' AND side='left' AND pk_blob=X'01'
        """)
        assert stateful_share_policy._state_stats(
            con,"inner_join","size-join"
        )==dict(rows=1,payload_bytes=5)
        con.execute("""
            DELETE FROM join_rows
            WHERE state_id='size-join' AND side='left' AND pk_blob=X'01'
        """)
        assert stateful_share_policy._state_stats(
            con,"inner_join","size-join"
        )==dict(rows=0,payload_bytes=0)

        exact=candidate(
            "leader-exact","state-exact",9,"exact",0)
        subview=candidate(
            "leader-subview","state-subview",10,"subview",2)

        assert stateful_share_policy.choose(
            con,"aggregate",task,[exact,subview],
            cfg=dict(stateful_share_mode="off")
        ) is None
        off=stateful_share_policy.decision_info(
            con,"follower")
        assert off["reason"]=="sharing_disabled"
        assert off["selected_leader_task_id"] is None

        with patch.object(
            stateful_share_policy,"_state_stats",
            side_effect=AssertionError(
                "compatible sharing scanned full state")
        ):
            chosen=stateful_share_policy.choose(
                con,"aggregate",task,[exact,subview],
                cfg=dict(stateful_share_mode="compatible"))
        assert chosen is exact
        compatible=stateful_share_policy.decision_info(
            con,"follower")
        assert compatible["selected_leader_task_id"]=="leader-exact"
        assert compatible["reuse_mode"]=="exact"

        # Adaptive mode rejects the exact leader when lag exceeds the bound,
        # then admits the current subview within the configured surplus bound.
        with patch.object(
            stateful_share_policy,"_state_stats",
            side_effect=AssertionError(
                "adaptive sharing scanned full state without size budget")
        ):
            chosen=stateful_share_policy.choose(
                con,"aggregate",task,[exact,subview],
                cfg=dict(
                    stateful_share_mode="adaptive",
                    stateful_share_max_lag=0,
                    stateful_share_max_followers=10,
                    stateful_share_max_surplus=2,
                ))
        assert chosen is subview
        adaptive=stateful_share_policy.decision_info(
            con,"follower")
        assert adaptive["reason"]=="adaptive_selected"
        assert adaptive["selected_leader_task_id"]=="leader-subview"
        assert adaptive["metrics"]["lag"]==0
        assert adaptive["metrics"]["surplus"]==2

        # Existing fanout is an admission constraint rather than an implicit
        # unbounded promise. One current follower reaches the configured cap.
        con.execute("""
            INSERT INTO aggregate_shared_followers(
                follower_task_id,leader_task_id,shared_state_id,
                leader_consumer_id,fixed_w,created,updated)
            VALUES('existing','leader-subview','state-subview',
                   'leader-consumer',10,0,0)
        """)
        rejected=stateful_share_policy.choose(
            con,"aggregate",task,[subview],
            cfg=dict(
                stateful_share_mode="adaptive",
                stateful_share_max_lag=0,
                stateful_share_max_followers=1,
                stateful_share_max_surplus=2,
            ))
        assert rejected is None
        decision=stateful_share_policy.decision_info(
            con,"follower")
        assert decision["reason"]=="adaptive_rejected_all"
        reasons=decision["metrics"]["rejected"][0]["reasons"]
        assert "followers" in reasons

        # Surplus can be independently fenced for projection/subview sharing.
        rejected=stateful_share_policy.choose(
            con,"aggregate",task,[subview],
            cfg=dict(
                stateful_share_mode="adaptive",
                stateful_share_max_lag=10,
                stateful_share_max_followers=10,
                stateful_share_max_surplus=1,
            ))
        assert rejected is None
        decision=stateful_share_policy.decision_info(
            con,"follower")
        assert "surplus" in (
            decision["metrics"]["rejected"][0]["reasons"])

        with patch.object(
            stateful_share_policy,"_state_stats",
            return_value=dict(
                rows=5000,payload_bytes=8*1024*1024)
        ):
            rejected=stateful_share_policy.choose(
                con,"aggregate",task,[subview],
                cfg=dict(
                    stateful_share_mode="adaptive",
                    stateful_share_max_lag=10,
                    stateful_share_max_followers=10,
                    stateful_share_max_surplus=2,
                    stateful_share_max_state_rows=1000,
                    stateful_share_max_state_bytes=1024*1024,
                ))
        assert rejected is None
        decision=stateful_share_policy.decision_info(
            con,"follower")
        reasons=decision["metrics"]["rejected"][0]["reasons"]
        assert "state_rows" in reasons
        assert "state_bytes" in reasons

        stateful_share_policy.observe(
            con,"existing",10,10,10,7,
            copied_sequences=0)
        rejected=stateful_share_policy.choose(
            con,"aggregate",task,[subview],
            cfg=dict(
                stateful_share_mode="adaptive",
                stateful_share_max_lag=10,
                stateful_share_max_followers=10,
                stateful_share_max_surplus=2,
                stateful_share_max_observed_visible_lag=2,
            ))
        assert rejected is None
        decision=stateful_share_policy.decision_info(
            con,"follower")
        assert "observed_visible_lag" in (
            decision["metrics"]["rejected"][0]["reasons"])

        first=stateful_share_policy.observe(
            con,"follower",10,9,10,8,
            copied_sequences=1)
        assert first["samples"]==1
        assert first["max_leader_lag"]==1
        assert first["max_source_lag"]==1
        assert first["max_visible_lag"]==1
        assert first["copied_sequences"]==1
        second=stateful_share_policy.observe(
            con,"follower",12,10,13,7,
            copied_sequences=2)
        assert second["samples"]==2
        assert second["max_leader_lag"]==2
        assert second["max_source_lag"]==3
        assert second["max_visible_lag"]==3
        assert second["copied_sequences"]==3
        status=stateful_share_policy.status(con)
        assert status["decisions"]==1
        # status aggregates observations across all followers. The earlier
        # "existing" follower contributes one sample in addition to the two
        # explicit "follower" samples below.
        assert status["samples"]==3
        assert status["copied_sequences"]==3
        assert status["max_visible_lag"]==3

        con.close()

    print(
        "stateful_share_policy_test ok off compatible adaptive "
        "lag fanout surplus incremental_state_size lazy_state_size "
        "state_size observed_visible_lag durable_decision runtime_feedback",
        flush=True,
    )


if __name__=="__main__":
    main()
