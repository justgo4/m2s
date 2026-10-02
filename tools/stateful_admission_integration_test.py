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

    print(
        "stateful_admission_integration_test ok "
        "hot_add_pre_side_effect rebuild_pre_side_effect",
        flush=True,
    )


if __name__=="__main__":
    main()
