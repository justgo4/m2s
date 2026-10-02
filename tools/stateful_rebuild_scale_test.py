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

    count=32
    intents=[]
    for index in range(count):
        sink="starrocks.stateful_%02d" % index
        kind="aggregate" if index%2==0 else "inner_join"
        intent=stateful_rebuild.begin(
            con,kind,sink,
            "old-%02d" % index,
            "new-%02d" % index,
            "stateful_%02d" % index)
        intents.append(intent)

    assert len(stateful_rebuild.active(con))==count
    shadows=[item["shadow_target"] for item in intents]
    markers=[
        stateful_rebuild.remote_marker(item["new_task_id"])
        for item in intents
    ]
    assert len(set(shadows))==count
    assert len(set(markers))==count

    # Freeze every generation at a different frontier. One intent fails,
    # several progress to different phases, and the rest remain building.
    for index,intent in enumerate(intents):
        if index%5==0:
            stateful_rebuild.freeze_frontier(
                con,intent["sink_key"],1000+index)
    failed=stateful_rebuild.fail(
        con,intents[0]["sink_key"],
        "synthetic multi-generation fault")
    assert failed["phase"]=="failed"

    ready=stateful_rebuild.mark_ready_to_swap(
        con,intents[5]["sink_key"])
    assert ready["frontier"]==1005
    assert ready["phase"]=="ready_to_swap"
    swapped=stateful_rebuild.mark_swapped(
        con,intents[5]["sink_key"])
    assert swapped["phase"]=="swapped"

    fencing=stateful_rebuild.info(
        con,intents[10]["sink_key"])
    assert fencing["phase"]=="fencing"
    assert fencing["frontier"]==1010

    untouched=stateful_rebuild.info(
        con,intents[1]["sink_key"])
    assert untouched["phase"]=="building_shadow"
    assert untouched["frontier"] is None

    # Task lookup must resolve exactly one active rebuild and failed/terminal
    # generations must not masquerade as active ownership.
    assert stateful_rebuild.for_task(
        con,intents[10]["old_task_id"]
    )["sink_key"]==intents[10]["sink_key"]
    assert stateful_rebuild.for_task(
        con,intents[10]["new_task_id"]
    )["sink_key"]==intents[10]["sink_key"]
    assert stateful_rebuild.for_task(
        con,intents[0]["new_task_id"]) is None

    # Complete one generation and verify all unrelated rebuild identities are
    # unchanged. This catches accidental global phase/frontier updates.
    stateful_rebuild.mark_cleanup(
        con,intents[5]["sink_key"])
    complete=stateful_rebuild.mark_complete(
        con,intents[5]["sink_key"])
    assert complete["phase"]=="complete"
    assert stateful_rebuild.for_task(
        con,intents[5]["new_task_id"]) is None
    assert stateful_rebuild.info(
        con,intents[10]["sink_key"]
    )["phase"]=="fencing"
    assert stateful_rebuild.info(
        con,intents[1]["sink_key"]
    )["phase"]=="building_shadow"

    # Terminal cleanup is per-sink and permits a later generation to reuse the
    # logical sink only after explicit removal of the terminal record.
    stateful_rebuild.clear_terminal(
        con,intents[5]["sink_key"])
    next_intent=stateful_rebuild.begin(
        con,intents[5]["kind"],intents[5]["sink_key"],
        intents[5]["new_task_id"],"newer-05",
        intents[5]["logical_target"])
    assert next_intent["phase"]=="building_shadow"
    assert next_intent["shadow_target"]!=intents[5]["shadow_target"]

    active=stateful_rebuild.active(con)
    assert len(active)==count-1
    assert len({
        item["sink_key"] for item in active
    })==count-1

    con.close()
    print(
        "stateful_rebuild_scale_test ok 32_independent_generations "
        "unique_shadow_marker isolated_phase_fault terminal_reuse",
        flush=True,
    )


if __name__=="__main__":
    main()
