#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import stateful_rebuild


def main():
    con=sqlite3.connect(":memory:",isolation_level=None)
    stateful_rebuild.install(con)

    first=stateful_rebuild.begin(
        con,"aggregate","starrocks.orders_agg",
        "catalog:starrocks.orders_agg:plan:3",
        "catalog:starrocks.orders_agg:plan:9",
        "orders_agg")
    assert first["phase"]=="building_shadow"
    assert first["frontier"] is None
    assert first["shadow_target"].startswith(
        "__j4_rebuild_orders_agg_")
    assert first==stateful_rebuild.begin(
        con,"aggregate","starrocks.orders_agg",
        first["old_task_id"],first["new_task_id"],
        "orders_agg",shadow=first["shadow_target"])

    fenced=stateful_rebuild.freeze_frontier(
        con,"starrocks.orders_agg",41)
    assert fenced["phase"]=="fencing"
    assert fenced["frontier"]==41
    assert stateful_rebuild.freeze_frontier(
        con,"starrocks.orders_agg",41)==fenced
    try:
        stateful_rebuild.freeze_frontier(
            con,"starrocks.orders_agg",42)
        raise AssertionError(
            "rebuild frontier changed across retry")
    except RuntimeError as exc:
        assert "frontier changed" in str(exc)

    assert not stateful_rebuild.frontier_reached(
        fenced,41,40,41,41)
    assert not stateful_rebuild.frontier_reached(
        fenced,41,41,40,41)
    assert stateful_rebuild.frontier_reached(
        fenced,41,41,41,99)

    ready=stateful_rebuild.mark_ready_to_swap(
        con,"starrocks.orders_agg")
    assert ready["phase"]=="ready_to_swap"
    swapped=stateful_rebuild.mark_swapped(
        con,"starrocks.orders_agg")
    assert swapped["phase"]=="swapped"
    cleanup=stateful_rebuild.mark_cleanup(
        con,"starrocks.orders_agg")
    assert cleanup["phase"]=="cleanup"
    complete=stateful_rebuild.mark_complete(
        con,"starrocks.orders_agg")
    assert complete["phase"]=="complete"
    assert stateful_rebuild.active(con)==[]
    assert stateful_rebuild.clear_terminal(
        con,"starrocks.orders_agg")
    assert stateful_rebuild.maybe_info(
        con,"starrocks.orders_agg") is None

    join=stateful_rebuild.begin(
        con,"inner_join","starrocks.joined",
        "old-join","new-join","joined")
    failed=stateful_rebuild.fail(
        con,"starrocks.joined","synthetic swap fault")
    assert failed["phase"]=="failed"
    assert failed["error"]=="synthetic swap fault"
    try:
        stateful_rebuild.begin(
            con,"inner_join","starrocks.joined",
            "old-join","new-join","joined",
            shadow=join["shadow_target"])
        raise AssertionError(
            "failed rebuild was silently revived")
    except RuntimeError as exc:
        assert "explicitly cleared" in str(exc)
    stateful_rebuild.clear_terminal(
        con,"starrocks.joined")

    con.close()
    print(
        "stateful_rebuild_test ok deterministic_shadow "
        "durable_frontier phase_machine readiness terminal_clear",
        flush=True)


if __name__=="__main__":
    main()
