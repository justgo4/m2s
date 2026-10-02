#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import stateful_rebuild


def item(kind,sink,task_id,plan_version,target,shadow=False):
    mapping=dict(
        src_table=sink.replace("starrocks.",""),
        sr_table=target,
        _catalog_sink=sink,
        _output_columns=["id"],
        _target_sequence=False,
    )
    value=dict(
        kind=kind,
        task=dict(
            task_id=task_id,
            sink_key=sink,
            target_table=target,
            plan_version=plan_version,
            descriptor_hash="hash-"+task_id,
        ),
        mapping=mapping,
    )
    if shadow:
        value["logical_mapping"]=dict(
            mapping,sr_table=target)
    return value


def old_item(kind,sink,task_id,plan_version,target):
    return item(
        kind,sink,task_id,plan_version,target)


def durable(task_id,sink,plan_version):
    return dict(
        task_id=task_id,
        sink_key=sink,
        plan_version=plan_version,
        status="active",
        consumer_id="consumer-"+task_id,
        target_table=sink.split(".",1)[1],
    )


def begin(con,kind,sink,old_id,new_id,target,phase):
    value=stateful_rebuild.begin(
        con,kind,sink,old_id,new_id,target,
        original_comment="original-"+sink)
    if phase=="building_shadow":
        return value
    value=stateful_rebuild.freeze_frontier(
        con,sink,100)
    value=stateful_rebuild.mark_ready_to_swap(
        con,sink)
    value=stateful_rebuild.mark_swapped(
        con,sink)
    if phase=="swapped":
        return value
    return stateful_rebuild.mark_cleanup(
        con,sink)


def setup_db(path):
    con=sqlite3.connect(
        path,isolation_level=None)
    con.execute(
        "CREATE TABLE meta("
        "key TEXT PRIMARY KEY,value BLOB NOT NULL)")
    stateful_rebuild.install(con)
    return con


def patches(old_by_id):
    def durable_task(con,kind,task_id):
        return dict(old_by_id[task_id])

    def from_durable(kind,task):
        return old_item(
            kind,task["sink_key"],
            task["task_id"],
            task["plan_version"],
            task["target_table"])

    def remote_marker(cfg,table):
        for rebuild in stateful_rebuild.active(cfg["con"]):
            if table==rebuild["shadow_target"]:
                return stateful_rebuild.remote_marker(
                    rebuild["new_task_id"])
            if table==rebuild["logical_target"]:
                return rebuild["original_comment"]
        return None

    return (
        patch.object(
            j4,"stateful_durable_task",
            side_effect=durable_task),
        patch.object(
            j4.stateful_catalog_runtime,
            "item_from_durable",
            side_effect=from_durable),
        patch.object(
            j4.stateful_catalog_runtime,
            "register_compiled",
            side_effect=lambda con,items:list(items)),
        patch.object(
            j4.stateful_catalog_runtime,
            "retire_task",
            side_effect=lambda con,cfg,kind,task:{
                "task":dict(task,status="retired")
            }),
        patch.object(
            j4,"stateful_rebuild_cleanup_remote",
            return_value=None),
        patch.object(
            j4,"ensure_stateful_rebuild_shadow",
            return_value=None),
        patch.object(
            j4,"stateful_rebuild_remote_marker",
            side_effect=remote_marker),
    )


def mixed_restart_contract():
    with tempfile.TemporaryDirectory(
        prefix="m2s-rebuild-multi-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=setup_db(path)
        a=begin(
            con,"aggregate","starrocks.a",
            "old-a","new-a","a","cleanup")
        b=begin(
            con,"inner_join","starrocks.b",
            "old-b","new-b","b","building_shadow")
        old_by_id={
            "old-a":durable(
                "old-a","starrocks.a",3),
            "old-b":durable(
                "old-b","starrocks.b",3),
        }
        compiled=[
            item(
                "aggregate","starrocks.a",
                "new-a",9,"a"),
            item(
                "inner_join","starrocks.b",
                "new-b",9,"b"),
        ]
        cfg=dict(
            catalog_version=9,
            con=con,
        )
        stack=patches(old_by_id)
        with stack[0],stack[1],stack[2],stack[3],\
             stack[4],stack[5],stack[6]:
            result=j4.recover_stateful_rebuilds_startup(
                con,cfg,compiled,"fingerprint-9")
        assert result["recovered_cutover"]
        assert len(result["rebuild_specs"])==1
        assert result["rebuild_specs"][0][
            "sink"]=="starrocks.b"
        assert stateful_rebuild.info(
            con,"starrocks.a")["phase"]=="complete"
        assert stateful_rebuild.info(
            con,"starrocks.b")["phase"]=="building_shadow"
        assert j4.meta_get(
            con,"active_plan_version") is None
        worker_ids=[
            value["task"]["task_id"]
            for value in result["worker_items"]
        ]
        assert worker_ids.count("old-b")==1
        assert "old-a" not in worker_ids
        assert "new-a" in worker_ids
        assert "new-b" in worker_ids
        con.close()


def final_restart_promotes_plan():
    with tempfile.TemporaryDirectory(
        prefix="m2s-rebuild-final-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=setup_db(path)
        begin(
            con,"aggregate","starrocks.a",
            "old-a","new-a","a","cleanup")
        begin(
            con,"inner_join","starrocks.b",
            "old-b","new-b","b","cleanup")
        old_by_id={
            "old-a":durable(
                "old-a","starrocks.a",3),
            "old-b":durable(
                "old-b","starrocks.b",3),
        }
        compiled=[
            item(
                "aggregate","starrocks.a",
                "new-a",9,"a"),
            item(
                "inner_join","starrocks.b",
                "new-b",9,"b"),
        ]
        cfg=dict(
            catalog_version=9,
            con=con,
        )
        stack=patches(old_by_id)
        with stack[0],stack[1],stack[2],stack[3],\
             stack[4],stack[5],stack[6]:
            result=j4.recover_stateful_rebuilds_startup(
                con,cfg,compiled,"fingerprint-9")
        assert result["recovered_cutover"]
        assert result["rebuild_specs"]==[]
        assert j4.meta_get(
            con,"active_plan_version")==9
        assert j4.meta_get(
            con,"fingerprint")=="fingerprint-9"
        assert all(
            stateful_rebuild.info(
                con,sink)["phase"]=="complete"
            for sink in (
                "starrocks.a","starrocks.b"
            )
        )
        assert {
            value["task"]["task_id"]
            for value in result["worker_items"]
        }=={"new-a","new-b"}
        con.close()


def runtime_partial_switch_contract():
    old_a=old_item(
        "aggregate","starrocks.a",
        "old-a",3,"a")
    old_b=old_item(
        "inner_join","starrocks.b",
        "old-b",3,"b")
    new_a=item(
        "aggregate","starrocks.a",
        "new-a",9,"__shadow_a",shadow=True)
    new_a["logical_mapping"]["sr_table"]="a"
    new_b=item(
        "inner_join","starrocks.b",
        "new-b",9,"__shadow_b",shadow=True)
    new_b["logical_mapping"]["sr_table"]="b"
    candidate=dict(
        version=9,
        stateful_candidate_tasks=[
            new_a,new_b],
        stateful_rebuilds=[
            dict(sink="starrocks.a"),
            dict(sink="starrocks.b"),
        ],
    )
    runtime=dict(
        plan_lock=threading.RLock(),
        active_plan_version=3,
        pending_plan="pending",
        deferred_plan="deferred",
        stateful_tasks=[
            old_a,old_b,new_a,new_b],
        stateful_active_task_ids={
            "old-a","old-b","new-a","new-b"},
        stateful_rebuild_plans={
            "starrocks.a":candidate,
            "starrocks.b":candidate,
        },
        stateful_mappings={
            (3,"starrocks.a"):old_a["mapping"],
            (3,"starrocks.b"):old_b["mapping"],
            (9,"starrocks.a"):new_a["mapping"],
            (9,"starrocks.b"):new_b["mapping"],
        },
    )
    rebuild_a=dict(
        sink_key="starrocks.a",
        old_task_id="old-a",
        new_task_id="new-a",
        logical_target="a",
    )
    j4.stateful_rebuild_switch_runtime(
        runtime,rebuild_a,candidate,False)
    assert runtime["active_plan_version"]==3
    assert runtime["pending_plan"]=="pending"
    ids=[
        value["task"]["task_id"]
        for value in runtime["stateful_tasks"]
    ]
    assert "old-a" not in ids
    assert "new-a" in ids
    assert "old-b" in ids
    assert "new-b" in ids
    assert "starrocks.a" not in runtime[
        "stateful_rebuild_plans"]
    assert "starrocks.b" in runtime[
        "stateful_rebuild_plans"]

    rebuild_b=dict(
        sink_key="starrocks.b",
        old_task_id="old-b",
        new_task_id="new-b",
        logical_target="b",
    )
    j4.stateful_rebuild_switch_runtime(
        runtime,rebuild_b,candidate,True)
    assert runtime["active_plan_version"]==9
    assert runtime["pending_plan"] is None
    assert runtime["deferred_plan"] is None
    assert [
        value["task"]["task_id"]
        for value in runtime["stateful_tasks"]
    ]==["new-a","new-b"]


def activation_contract():
    with tempfile.TemporaryDirectory(
        prefix="m2s-rebuild-activate-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=setup_db(path)
        con.close()

        old_a=old_item(
            "aggregate","starrocks.a",
            "old-a",3,"a")
        old_b=old_item(
            "inner_join","starrocks.b",
            "old-b",3,"b")
        new_a=item(
            "aggregate","starrocks.a",
            "new-a",9,"__shadow_a")
        new_a["task"]["target_table"]="a"
        new_b=item(
            "inner_join","starrocks.b",
            "new-b",9,"__shadow_b")
        new_b["task"]["target_table"]="b"
        candidate=dict(
            version=9,
            stateful_candidate_tasks=[
                new_a,new_b],
            stateful_rebuilds=[
                dict(
                    sink="starrocks.a",
                    old=old_a,new=new_a,
                    shadow_target="__shadow_a",
                    logical_target="a"),
                dict(
                    sink="starrocks.b",
                    old=old_b,new=new_b,
                    shadow_target="__shadow_b",
                    logical_target="b"),
            ],
        )
        runtime=dict(
            plan_lock=threading.RLock(),
            stateful_mappings={},
            stateful_active_task_ids=set(
                ["old-a","old-b"]),
            stateful_tasks=[old_a,old_b],
            stateful_rebuild_plans={},
            stateful_rebuild_locks={},
            stateful_worker_threads={},
            worker_keys=set(),
        )
        cfg=dict(
            state=path,
            stateful_admission_retry_seconds=5,
        )

        def open_state(_):
            local=sqlite3.connect(
                path,isolation_level=None)
            local.execute(
                "PRAGMA busy_timeout=10000")
            return local

        def remote_marker(cfg,table):
            return "original-"+table

        def register(con,items):
            return list(items)

        def add_sink(mapping,cfg,runtime,**kwargs):
            runtime["worker_keys"].add(
                j4.mapping_key(mapping))
            return True

        with patch.object(
            j4,"open_state",
            side_effect=open_state
        ), patch.object(
            j4.stateful_admission,
            "admit_or_defer",
            return_value=dict(status="admitted")
        ), patch.object(
            j4.stateful_admission,
            "clear_wait",
            return_value=0
        ), patch.object(
            j4,"stateful_rebuild_remote_marker",
            side_effect=remote_marker
        ), patch.object(
            j4,"ensure_stateful_rebuild_shadow",
            return_value=None
        ), patch.object(
            j4.stateful_catalog_runtime,
            "register_compiled",
            side_effect=register
        ), patch.object(
            j4,"runtime_add_sink",
            side_effect=add_sink
        ), patch.object(
            j4,"runtime_thread_register",
            side_effect=lambda runtime,thread:thread
        ), patch.object(
            j4,"rollback_stateful_admission",
            return_value=None
        ):
            activated=j4.activate_stateful_rebuild_candidate(
                cfg,runtime,candidate)

        assert activated==["new-a","new-b"]
        assert runtime["worker_keys"]=={
            "starrocks.a","starrocks.b"}
        assert set(runtime[
            "stateful_rebuild_plans"])=={
                "starrocks.a","starrocks.b"}
        assert set(runtime[
            "stateful_rebuild_locks"])=={
                "starrocks.a","starrocks.b"}
        assert set(runtime[
            "stateful_worker_threads"])=={
                "new-a","new-b"}
        assert {
            value["task"]["task_id"]
            for value in runtime["stateful_tasks"]
        }=={"old-a","old-b","new-a","new-b"}

        con=sqlite3.connect(
            path,isolation_level=None)
        try:
            records=stateful_rebuild.active(con)
            assert len(records)==2
            assert {
                value["sink_key"]
                for value in records
            }=={"starrocks.a","starrocks.b"}
            assert all(
                value["phase"]=="building_shadow"
                for value in records
            )
        finally:
            con.close()


def main():
    mixed_restart_contract()
    final_restart_promotes_plan()
    runtime_partial_switch_contract()
    activation_contract()
    print(
        "stateful_rebuild_multi_test ok "
        "mixed_restart final_plan_promotion partial_runtime_switch "
        "multi_activation",
        flush=True,
    )


if __name__=="__main__":
    main()
