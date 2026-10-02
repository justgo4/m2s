#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import stateful_rebuild
import stateful_rebuild_cohort


def rebuilds(con,prefix,count):
    result=[]
    for index in range(count):
        sink="starrocks.%s_%02d" % (
            prefix,index)
        result.append(stateful_rebuild.begin(
            con,
            "aggregate" if index%2==0 else "inner_join",
            sink,
            "old-%s-%02d" % (prefix,index),
            "new-%s-%02d" % (prefix,index),
            "%s_%02d" % (prefix,index)))
    return result


def main():
    con=sqlite3.connect(
        ":memory:",isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    stateful_rebuild.install(con)
    stateful_rebuild_cohort.install(con)

    items=rebuilds(
        con,"cohort",3)
    sinks=[
        item["sink_key"]
        for item in items
    ]
    cohort=stateful_rebuild_cohort.begin(
        con,9,reversed(sinks))
    assert cohort["phase"]=="building"
    assert [
        item["sink_key"]
        for item in cohort["members"]
    ]==sorted(sinks)
    assert cohort==stateful_rebuild_cohort.begin(
        con,9,sinks)
    assert stateful_rebuild_cohort.for_sink(
        con,sinks[0])["cohort_id"]==cohort["cohort_id"]

    try:
        stateful_rebuild_cohort.freeze(
            con,cohort["cohort_id"],55)
        raise AssertionError(
            "cohort froze before any member was ready")
    except RuntimeError as exc:
        assert "every member is ready" in str(exc)
    assert all(
        stateful_rebuild.info(
            con,sink)["frontier"] is None
        for sink in sinks
    )

    for sink in sinks[:2]:
        stateful_rebuild_cohort.mark_member_ready(
            con,cohort["cohort_id"],sink)
    assert not stateful_rebuild_cohort.ready(
        con,cohort["cohort_id"])
    try:
        stateful_rebuild_cohort.freeze(
            con,cohort["cohort_id"],55)
        raise AssertionError(
            "cohort froze with one member missing")
    except RuntimeError as exc:
        assert "every member is ready" in str(exc)
    assert all(
        stateful_rebuild.info(
            con,sink)["frontier"] is None
        for sink in sinks
    )

    stateful_rebuild_cohort.mark_member_ready(
        con,cohort["cohort_id"],sinks[2])
    assert stateful_rebuild_cohort.ready(
        con,cohort["cohort_id"])
    frozen=stateful_rebuild_cohort.freeze(
        con,cohort["cohort_id"],55)
    assert frozen["phase"]=="fencing"
    assert frozen["frontier"]==55
    assert {
        (
            stateful_rebuild.info(con,sink)["phase"],
            stateful_rebuild.info(con,sink)["frontier"],
        )
        for sink in sinks
    }=={("fencing",55)}
    try:
        stateful_rebuild_cohort.freeze(
            con,cohort["cohort_id"],56)
        raise AssertionError(
            "cohort frontier changed across retry")
    except RuntimeError as exc:
        assert "frontier changed" in str(exc)

    ready=stateful_rebuild_cohort.mark_ready_to_swap(
        con,cohort["cohort_id"])
    assert ready["phase"]=="ready_to_swap"
    assert {
        stateful_rebuild.info(
            con,sink)["phase"]
        for sink in sinks
    }=={"ready_to_swap"}

    swapping=stateful_rebuild_cohort.begin_swap(
        con,cohort["cohort_id"])
    assert swapping["phase"]=="swapping"
    try:
        stateful_rebuild_cohort.mark_cleanup(
            con,cohort["cohort_id"])
        raise AssertionError(
            "cohort cleaned before all remote swaps")
    except RuntimeError as exc:
        assert "before all swaps" in str(exc)

    for sink in sinks:
        stateful_rebuild_cohort.mark_member_swapped(
            con,cohort["cohort_id"],sink)
    assert stateful_rebuild_cohort.all_swapped(
        con,cohort["cohort_id"])
    cleanup=stateful_rebuild_cohort.mark_cleanup(
        con,cohort["cohort_id"])
    assert cleanup["phase"]=="cleanup"
    assert {
        stateful_rebuild.info(
            con,sink)["phase"]
        for sink in sinks
    }=={"cleanup"}
    complete=stateful_rebuild_cohort.mark_complete(
        con,cohort["cohort_id"])
    assert complete["phase"]=="complete"
    assert stateful_rebuild_cohort.active(con)==[]
    assert {
        stateful_rebuild.info(
            con,sink)["phase"]
        for sink in sinks
    }=={"complete"}

    # Cohort sink ownership is active-only. A later plan may reuse the same
    # logical sinks after the old rebuild records are explicitly retired.
    for sink in sinks:
        stateful_rebuild.clear_terminal(
            con,sink)
    next_items=[]
    for index,sink in enumerate(sinks):
        next_items.append(stateful_rebuild.begin(
            con,
            "aggregate" if index%2==0 else "inner_join",
            sink,
            "new-cohort-%02d" % index,
            "newer-cohort-%02d" % index,
            sink.split(".",1)[1]))
    next_cohort=stateful_rebuild_cohort.begin(
        con,10,sinks)
    assert next_cohort["phase"]=="building"

    overlap=[
        sinks[0],
        "starrocks.other",
    ]
    overlap_id=stateful_rebuild_cohort.identity(
        11,overlap)
    try:
        stateful_rebuild_cohort.begin(
            con,11,overlap)
        raise AssertionError(
            "active sink joined two rebuild cohorts")
    except RuntimeError as exc:
        assert "another active cohort" in str(exc)
    assert stateful_rebuild_cohort.maybe_info(
        con,overlap_id) is None

    failed=stateful_rebuild_cohort.fail(
        con,next_cohort["cohort_id"],
        "synthetic cohort failure")
    assert failed["phase"]=="failed"
    assert failed["error"]=="synthetic cohort failure"
    assert stateful_rebuild_cohort.for_sink(
        con,sinks[0]) is None

    con.close()
    print(
        "stateful_rebuild_cohort_test ok "
        "deterministic_members common_frontier atomic_freeze "
        "all_ready_before_swap partial_swap_visible "
        "active_only_sink_ownership transactional_begin",
        flush=True,
    )


if __name__=="__main__":
    main()
