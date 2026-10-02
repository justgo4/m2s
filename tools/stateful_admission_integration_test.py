#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import stateful_admission


def task(task_id,sink):
    return dict(
        task=dict(
            task_id=task_id,
            sink_key=sink,
        )
    )


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-admission-integration-"
    ) as td:
        state=str(Path(td)/"state.sqlite3")
        con=j4.init_state(state)
        con.close()

        candidate=dict(
            version=2,
            stateful_additions=[
                task("new-a","starrocks.new_a"),
                task("new-b","starrocks.new_b"),
            ],
            stateful_added_manifests=[],
            stateful_source_metadata={},
        )
        cfg=dict(
            state=state,
            stateful_admission_max_tasks=1,
        )
        with patch.object(
            j4,"ensure_stateful_hot_add_targets_empty",
            side_effect=AssertionError(
                "remote target touched before admission")
        ), patch.object(
            j4.stateful_catalog_runtime,
            "compile_catalog_tasks",
            side_effect=AssertionError(
                "compile/create path reached before admission")
        ):
            try:
                j4.prepare_hot_stateful_additions(
                    cfg,{},candidate)
                raise AssertionError(
                    "over-budget hot add was admitted")
            except RuntimeError as exc:
                assert "max_tasks" in str(exc)
        con=j4.open_state(state)
        try:
            assert not stateful_admission.decision_info(
                con,"new-a")["admitted"]
            assert not stateful_admission.decision_info(
                con,"new-b")["admitted"]
        finally:
            con.close()

        rebuild=dict(
            version=3,
            stateful_rebuilds=[dict(
                sink="starrocks.rebuild",
                old=task("old","starrocks.rebuild"),
                new=task("new-rebuild","starrocks.rebuild"),
            )],
        )
        rebuild_cfg=dict(
            state=state,
            stateful_admission_max_state_bytes=1,
            stateful_admission_reserve_state_bytes=2,
        )
        with patch.object(
            j4,"stateful_rebuild_remote_marker",
            side_effect=AssertionError(
                "remote rebuild marker read before admission")
        ), patch.object(
            j4.stateful_rebuild,"maybe_info",
            side_effect=AssertionError(
                "rebuild intent touched before admission")
        ):
            try:
                j4.activate_stateful_rebuild_candidate(
                    rebuild_cfg,{},rebuild)
                raise AssertionError(
                    "over-budget rebuild was admitted")
            except RuntimeError as exc:
                assert "max_state_bytes" in str(exc)

        con=j4.open_state(state)
        try:
            info=stateful_admission.decision_info(
                con,"new-rebuild")
            assert not info["admitted"]
            assert info["metrics"][
                "projected_state_bytes"]>=2
        finally:
            con.close()

        # A successful reservation must be released if hot-add preparation
        # fails before a durable task descriptor is registered.
        release_candidate=dict(
            version=4,
            stateful_additions=[
                task("release-hot","starrocks.release_hot"),
            ],
            stateful_added_manifests=[],
            stateful_source_metadata={},
        )
        release_cfg=dict(
            state=state,
            stateful_admission_max_state_bytes=100,
            stateful_admission_reserve_state_bytes=60,
        )
        with patch.object(
            j4,"ensure_stateful_hot_add_targets_empty",
            side_effect=RuntimeError(
                "synthetic hot-add failure")
        ):
            try:
                j4.prepare_hot_stateful_additions(
                    release_cfg,{},release_candidate)
                raise AssertionError(
                    "synthetic hot-add failure was ignored")
            except RuntimeError as exc:
                assert "synthetic hot-add failure" in str(exc)
        con=j4.open_state(state)
        try:
            released=stateful_admission.decision_info(
                con,"release-hot")
            assert released["admitted"]
            assert released["reserved_state_bytes"]==0
            assert released["reason"].startswith("released:")
        finally:
            con.close()

        # Admission wait consumption is ordered after durable descriptor
        # registration. A future refactor must not move clear_wait ahead of
        # register_compiled, which would reopen the crash orphan window.
        boundary_item=dict(
            kind="aggregate",
            task=dict(
                task_id="boundary-hot",
                sink_key="starrocks.boundary_hot",
                descriptor_hash="boundary-hash",
            ),
        )
        boundary_candidate=dict(
            version=6,
            stateful_additions=[boundary_item],
            stateful_added_manifests=[],
            stateful_source_metadata={},
        )
        order=[]
        def registered(_con,items):
            order.append("register")
            return list(items)
        def cleared(*_args,**_kwargs):
            order.append("clear")
            return 1
        with patch.object(
            j4.stateful_admission,
            "admit_or_defer",
            return_value=dict(ok=True)
        ), patch.object(
            j4,"ensure_stateful_hot_add_targets_empty"
        ), patch.object(
            j4.stateful_catalog_runtime,
            "compile_catalog_tasks",
            return_value=[boundary_item]
        ), patch.object(
            j4.stateful_catalog_runtime,
            "ensure_registration_safe"
        ), patch.object(
            j4.stateful_catalog_runtime,
            "register_compiled",
            side_effect=registered
        ), patch.object(
            j4.stateful_share_policy,
            "plan_graph",
            return_value=[]
        ), patch.object(
            j4.stateful_admission,
            "clear_wait",
            side_effect=cleared
        ):
            result=j4.prepare_hot_stateful_additions(
                dict(state=state),
                dict(
                    plan_lock=__import__("threading").RLock(),
                    stateful_tasks=[],
                ),
                boundary_candidate)
        assert result==[boundary_item]
        assert order==["register","clear"],order

        # Rebuild admission is released if activation fails before a durable
        # stateful_rebuild intent owns the new generation.
        release_rebuild=dict(
            version=5,
            stateful_rebuilds=[dict(
                sink="starrocks.release_rebuild",
                old=task(
                    "release-old",
                    "starrocks.release_rebuild"),
                new=task(
                    "release-new",
                    "starrocks.release_rebuild"),
            )],
        )
        try:
            j4.activate_stateful_rebuild_candidate(
                release_cfg,{},release_rebuild)
            raise AssertionError(
                "invalid rebuild was unexpectedly activated")
        except KeyError:
            pass
        con=j4.open_state(state)
        try:
            released=stateful_admission.decision_info(
                con,"release-new")
            assert released["admitted"]
            assert released["reserved_state_bytes"]==0
            assert released["reason"].startswith("released:")
        finally:
            con.close()

    print(
        "stateful_admission_integration_test ok "
        "hot_add_pre_side_effect rebuild_pre_side_effect "
        "failed_reservation_release durable_registration_before_wait_clear",
        flush=True,
    )


if __name__=="__main__":
    main()
