#!/usr/bin/env python3
"""Durable stateful admission retry/requeue contract."""
from pathlib import Path
import tempfile
import threading
from unittest.mock import patch
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import stateful_admission


def task(task_id,sink):
    return dict(task=dict(
        task_id=task_id,
        sink_key=sink,
    ))


def main():
    # Rejection must become a durable retryable condition without touching
    # remote target state. The exact published plan version is retained.
    with tempfile.TemporaryDirectory(
        prefix="m2s-admission-retry-"
    ) as td:
        state=str(Path(td)/"state.sqlite3")
        con=j4.init_state(state)
        additions=[task("wait-a","starrocks.wait_a")]
        try:
            stateful_admission.admit_or_defer(
                con,additions,7,
                cfg=dict(
                    stateful_admission_max_tasks=0,
                    stateful_admission_max_state_bytes=1,
                    stateful_admission_reserve_state_bytes=2,
                ),
                retry_seconds=1)
            raise AssertionError(
                "over-budget admission did not defer")
        except stateful_admission.AdmissionDeferred as exc:
            assert exc.plan_version==7
            assert "max_state_bytes" in exc.result["reasons"]
        waits=stateful_admission.waiting_tasks(
            con,plan_version=7)
        assert len(waits)==1
        assert waits[0]["task_id"]=="wait-a"
        assert waits[0]["retry_count"]==0
        first_retry=waits[0]["next_retry"]

        # Repeated pressure must back off instead of hot-looping expensive
        # catalog validation/target checks. A replacement plan resets the
        # retry generation because its resource shape may differ.
        stateful_admission.queue_wait(
            con,additions,7,
            reason="max_state_bytes",
            retry_seconds=1,max_retry_seconds=10)
        waits=stateful_admission.waiting_tasks(
            con,plan_version=7)
        assert waits[0]["retry_count"]==1
        assert waits[0]["next_retry"]>first_retry
        second_retry=waits[0]["next_retry"]
        stateful_admission.queue_wait(
            con,additions,7,
            reason="max_state_bytes",
            retry_seconds=1,max_retry_seconds=10)
        waits=stateful_admission.waiting_tasks(
            con,plan_version=7)
        assert waits[0]["retry_count"]==2
        assert waits[0]["next_retry"]>second_retry
        stateful_admission.queue_wait(
            con,additions,8,
            reason="replacement_plan",
            retry_seconds=1,max_retry_seconds=10)
        waits=stateful_admission.waiting_tasks(
            con,plan_version=8)
        assert waits[0]["retry_count"]==0
        con.close()

        runtime=dict(
            plan_lock=threading.RLock(),
            catalog_install_lock=threading.Lock(),
            active_plan_version=1,
            catalog_activation={},
        )
        cfg=dict(
            state=state,
            catalog=str(Path(td)/"catalog.sqlite3"),
            catalog_seed=None,
        )

        # A newer published catalog supersedes an older waiting version.
        with patch.object(
            j4.cdc_catalog,"load_plan",
            return_value=dict(
                version=8,plan_hash="latest")
        ), patch.object(
            j4,"queue_hot_catalog_plan",
            side_effect=AssertionError(
                "superseded plan must not be installed")
        ):
            result=j4.retry_waiting_stateful_admission(
                cfg,runtime,now=10**12)
        assert result==[dict(
            status="superseded",
            version=7,
            latest_version=8,
            cleared_tasks=1,
        )],result
        con=j4.open_state(state)
        try:
            assert not stateful_admission.waiting_tasks(con)
        finally:
            con.close()

        # The current published version is retried. Once activation reports
        # active, any residual wait record is removed idempotently.
        con=j4.open_state(state)
        try:
            stateful_admission.queue_wait(
                con,[task("wait-b","starrocks.wait_b")],
                plan_version=8,reason="max_tasks",
                retry_seconds=0)
        finally:
            con.close()
        calls=[]
        def install(_cfg,_runtime,publish):
            calls.append(dict(publish))
            return dict(status="active",version=8)
        with patch.object(
            j4.cdc_catalog,"load_plan",
            return_value=dict(
                version=8,plan_hash="latest")
        ), patch.object(
            j4,"queue_hot_catalog_plan",
            side_effect=install
        ):
            result=j4.retry_waiting_stateful_admission(
                cfg,runtime,now=10**12)
        assert result==[dict(status="active",version=8)]
        assert calls==[dict(
            version=8,plan_hash="latest")]
        con=j4.open_state(state)
        try:
            assert not stateful_admission.waiting_tasks(con)
        finally:
            con.close()

        # Catalog install converts the typed deferred exception into a normal
        # control-plane status; it must not stop the running data plane.
        deferred=stateful_admission.AdmissionDeferred(
            dict(
                ok=False,
                reason="max_pending_bytes",
                reasons=["max_pending_bytes"],
                metrics={},
                limits={},
            ),9)
        publish=dict(
            version=9,
            validation=dict(
                status="hot_add",
                version=9,
            ))
        with patch.object(
            j4,"install_hot_catalog_plan",
            side_effect=deferred
        ):
            result=j4.catalog_publish_callback(
                cfg,runtime,publish,"install")
        assert result["status"]=="resource_waiting"
        assert result["retryable"]
        assert result["version"]==9
        assert result["stateful_admission"][
            "reason"]=="max_pending_bytes"

    print(
        "stateful_admission_retry_test ok durable_defer "
        "superseded_plan_fence current_plan_retry "
        "control_plane_nonfatal_wait bounded_retry_backoff plan_retry_reset",
        flush=True,
    )


if __name__=="__main__":
    main()
