#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_state


def spec():
    return dict(
        format_version=1,
        kind="inner_join_state",
        sources=dict(
            left=dict(
                relation="db.left_rows",
                primary_key=["id"],
                join_key=["bucket"],
            ),
            right=dict(
                relation="db.right_rows",
                primary_key=["id"],
                join_key=["bucket"],
            ),
        ),
        projections=[
            dict(
                output="left_value",
                source="left",
                column="value",
            ),
            dict(
                output="right_value",
                source="right",
                column="value",
            ),
        ],
        semantics=dict(
            bag=True,
            nulls="sql",
            retract="source_pk_pair_identity",
        ),
    )


def main():
    con=sqlite3.connect(":memory:",isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    join_state.install(con)

    state_spec=spec()
    join_state.begin_bootstrap(
        con,"hot-key",state_spec,0)
    left=[
        dict(id=index,bucket=1,value=index)
        for index in range(100)
    ]
    right=[
        dict(id=1000+index,bucket=1,value=index)
        for index in range(100)
    ]
    join_state.apply_bootstrap_chunk(
        con,"hot-key",0,"left",
        left,b"left",True)
    join_state.apply_bootstrap_chunk(
        con,"hot-key",0,"right",
        right,b"right",True)

    assert len(join_state.read_pairs(
        con,"hot-key"))==10000

    original=join_state._project
    calls=0

    def counted(state_spec,left_row,right_row):
        nonlocal calls
        calls+=1
        return original(
            state_spec,left_row,right_row)

    join_state._project=counted
    try:
        result=join_state.apply_transaction(
            con,"hot-key",1,[
                (
                    "left",
                    dict(
                        id=0,bucket=1,value=0,
                        _sync_op=1,
                    ),
                ),
                (
                    "left",
                    dict(
                        id=0,bucket=1,value=999,
                        _sync_op=0,
                    ),
                ),
            ])
    finally:
        join_state._project=original

    assert result["applied"]
    assert len(result["deltas"])==100
    assert all(
        item["op"]==0
        for item in result["deltas"]
    )
    assert all(
        item["row"]["left_value"]==999
        for item in result["deltas"]
    )
    # Previous correctness-first path projected the entire 100x100 key twice:
    # 20,000 projections for this one-row update. Only pairs containing the
    # changed left PK can differ, so the incremental path needs 100 before
    # plus 100 after projections.
    assert calls==200,calls

    pairs=join_state.read_pairs(
        con,"hot-key")
    assert len(pairs)==10000
    assert sum(
        item["row"]["left_value"]==999
        for item in pairs
    )==100
    assert sum(
        item["row"]["left_value"]==0
        for item in pairs
    )==0

    con.close()
    print(
        "join_incremental_delta_test ok "
        "100x100 one_left_update deltas=100 projections=200 "
        "full_state_exact",
        flush=True,
    )


if __name__=="__main__":
    main()
