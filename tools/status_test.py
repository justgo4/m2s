#!/usr/bin/env python3
from pathlib import Path
import os
import tempfile
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import stateful_admission
import stateful_catalog_runtime
import stateful_rebuild


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-status-"
    ) as td:
        missing=os.path.join(td,"missing.sqlite3")
        empty=j4.durable_status_snapshot(missing)
        assert empty["format_version"]==1
        assert not empty["state_exists"]

        path=os.path.join(td,"state.sqlite3")
        con=j4.init_state(path)
        with j4.state_transaction(con):
            j4.meta_set(
                con,"active_plan_version",7)
        stateful_rebuild.begin(
            con,"aggregate","starrocks.agg",
            "old-task","new-task","agg")
        stateful_catalog_runtime.stage_retirement(
            con,"aggregate",
            dict(
                task_id="retiring-task",
                sink_key="starrocks.old"),
            11)
        stateful_admission.queue_wait(
            con,[dict(task=dict(
                task_id="waiting-task",
                sink_key="starrocks.waiting"))],
            plan_version=8,
            reason="max_state_bytes",
            retry_seconds=30)
        con.close()

        status=j4.durable_status_snapshot(path)
        assert status["state_exists"]
        assert status["active_plan_version"]==7
        assert status["state_format"]==j4.STATE_FORMAT
        assert status["source"]["base_applied_seq"]==0
        assert status["jobs"]==dict(
            active=0,deliveries=0,invisible_parts=0)
        assert status["physical"]==dict(
            states=0,refs=0,pins=0,
            health={},sizes={})
        assert len(status["rebuilds"])==1
        assert status["rebuilds"][0]["sink_key"]=="starrocks.agg"
        assert status["rebuilds"][0]["phase"]=="building_shadow"
        assert status["retirements"]==[
            dict(
                task_id="retiring-task",
                kind="aggregate",
                sink_key="starrocks.old",
                frontier=11,
                phase="draining",
            )
        ]
        assert status["sharing"]["decisions"]==0
        assert status["admission"]["waiting_tasks"]==1
        assert status["admission"]["waiting_plans"]==1
        assert status["admission"]["waiting"][0]["plan_version"]==8
        assert status["admission"]["waiting"][0]["retry_count"]==0

    print(
        "status_test ok missing_state read_only_snapshot "
        "rebuild retirement source jobs sharing admission physical",
        flush=True,
    )


if __name__=="__main__":
    main()
