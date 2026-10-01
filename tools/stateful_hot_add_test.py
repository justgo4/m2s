#!/usr/bin/env python3
from pathlib import Path
import threading
from unittest.mock import patch
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import stateful_catalog_runtime
import stateful_task_plan


def item(task_id,sink,version,hash_value):
    return dict(
        kind="aggregate",
        task=dict(
            task_id=task_id,
            sink_key=sink,
            plan_version=version,
            descriptor_hash=hash_value,
        ),
        mapping=dict(
            src_table=sink,
            sr_table=sink.split(".",1)[-1],
            _output_columns=["k","n"],
            primary_key="k",
        ),
    )


def main():
    current=[
        item("task-a","starrocks.a",5,"hash-a"),
        item("task-b","starrocks.b",6,"hash-b"),
    ]
    candidate=[
        item("task-a","starrocks.a",5,"hash-a"),
        item("task-b2","starrocks.b",7,"hash-b2"),
        item("task-c","starrocks.c",8,"hash-c"),
    ]
    change=stateful_catalog_runtime.transition(
        current,candidate)
    assert change["added"]==["starrocks.c"]
    assert change["dropped"]==[]
    assert change["changed"]==["starrocks.b"]
    assert change["retained"]==["starrocks.a"]

    runtime=dict(
        plan_lock=threading.RLock(),
        thread_lock=threading.Lock(),
        stateful_mappings={},
        stateful_active_task_ids=set(),
        stateful_tasks=[],
        stateful_worker_threads={},
    )
    added=item(
        "task-hot","starrocks.hot",9,"hash-hot")
    candidate_plan=dict(
        version=12,
        stateful_additions=[added],
    )
    calls=[]
    with patch.object(
        j4,"runtime_add_sink",
        side_effect=lambda mapping,cfg,runtime,**kwargs:
            calls.append(("sink",mapping["sr_table"],kwargs))
    ), patch.object(
        j4,"runtime_thread_register",
        side_effect=lambda runtime,thread:
            calls.append(("thread",thread.name)) or thread
    ):
        activated=j4.activate_stateful_additions(
            {},runtime,candidate_plan)
    assert activated==["task-hot"]
    writer_version=stateful_task_plan.writer_plan_version(9)
    assert (
        writer_version,"starrocks.hot"
    ) in runtime["stateful_mappings"]
    assert runtime["stateful_active_task_ids"]=={"task-hot"}
    assert [entry["task"]["task_id"]
            for entry in runtime["stateful_tasks"]]==["task-hot"]
    assert calls[0]==(
        "sink","hot",{"historical_snapshot":False})
    assert calls[1][0]=="thread"

    # Replaying the same cutover must not duplicate durable task membership.
    # The real runtime does not call this twice, but idempotent in-memory
    # membership keeps crash-recovery logic simple.
    calls.clear()
    with patch.object(
        j4,"runtime_add_sink",
        return_value=False
    ), patch.object(
        j4,"runtime_thread_register",
        side_effect=lambda runtime,thread: thread
    ):
        j4.activate_stateful_additions(
            {},runtime,candidate_plan)
    assert [entry["task"]["task_id"]
            for entry in runtime["stateful_tasks"]]==["task-hot"]

    keep=item(
        "task-keep","starrocks.keep",10,"hash-keep")
    drop=item(
        "task-drop","starrocks.drop",11,"hash-drop")
    drop_runtime=dict(
        plan_lock=threading.RLock(),
        thread_lock=threading.Lock(),
        stateful_mappings={},
        stateful_active_task_ids={"task-keep","task-drop"},
        stateful_tasks=[keep,drop],
        stateful_worker_threads={},
    )
    drop_candidate=dict(
        version=13,
        stateful_candidate_tasks=[keep],
        stateful_dropped_sinks=["starrocks.drop"],
        stateful_additions=[],
    )
    retired_calls=[]
    with patch.object(
        stateful_catalog_runtime,"retire_absent",
        return_value=[drop]
    ) as retire, patch.object(
        j4,"runtime_mark_sink_retiring",
        side_effect=lambda runtime,key,cfg:
            retired_calls.append(key) or True
    ), patch.object(
        j4,"activate_stateful_additions",
        return_value=[]
    ):
        cutover=j4.activate_stateful_transition(
            None,{},drop_runtime,drop_candidate)
    assert cutover==dict(
        added=[],retired=["task-drop"])
    assert drop_runtime["stateful_active_task_ids"]=={"task-keep"}
    assert [entry["task"]["task_id"]
            for entry in drop_runtime["stateful_tasks"]]==["task-keep"]
    assert retired_calls==["starrocks.drop"]
    retire.assert_called_once_with(
        None,{},[keep])

    generous=dict(
        load_mode="transaction",
        writer_max=4,
        snapshot_workers=2,
        resource=dict(memory_mb=4096,cpu_target=8),
        duckdb_memory="64MB",
    )
    runtime_budget=dict(worker_keys={"a","b"})
    j4.hot_add_worker_resource_check(
        generous,runtime_budget,["c"])

    constrained=dict(
        load_mode="transaction",
        writer_max=4,
        snapshot_workers=4,
        resource=dict(memory_mb=128,cpu_target=2),
        duckdb_memory="64MB",
    )
    try:
        j4.hot_add_worker_resource_check(
            constrained,runtime_budget,["c","d"])
        raise AssertionError(
            "stateful hot add exceeded memory budget without restart fence")
    except RuntimeError as exc:
        assert "memory budget" in str(exc)

    print(
        "stateful_hot_add_test ok transition writer_only_activation "
        "online_drop_retirement namespaced_mapping resource_fence",
        flush=True,
    )


if __name__=="__main__":
    main()
