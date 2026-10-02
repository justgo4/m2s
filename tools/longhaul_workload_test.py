#!/usr/bin/env python3
from pathlib import Path
import json
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))

import longhaul_workload


def main():
    assert longhaul_workload.percentile([],0.95) is None
    assert longhaul_workload.percentile(
        [5,1,3,2,4],0.50)==3
    assert longhaul_workload.percentile(
        [5,1,3,2,4],0.99)==4

    with patch.object(
        longhaul_workload.subprocess,
        "run",
        return_value=SimpleNamespace(
            returncode=0,stdout="",stderr="")
    ):
        assert longhaul_workload.code_worktree_clean() is True
    with patch.object(
        longhaul_workload.subprocess,
        "run",
        return_value=SimpleNamespace(
            returncode=0,stdout=" M j4.py\n",stderr="")
    ):
        assert longhaul_workload.code_worktree_clean() is False
    with patch.object(
        longhaul_workload.subprocess,
        "run",
        return_value=SimpleNamespace(
            returncode=128,stdout="",stderr="failed")
    ):
        assert longhaul_workload.code_worktree_clean() is None
    class FakeCursor:
        def __init__(self,source):
            self.source=source
        def __enter__(self):
            return self
        def __exit__(self,*_):
            return False
        def execute(self,sql):
            if sql.startswith(
                "SET SESSION sql_log_bin="
            ):
                self.source.binlog=int(
                    sql.rsplit("=",1)[1])
        def fetchone(self):
            return (self.source.binlog,)

    class FakeSource:
        def __init__(self):
            self.binlog=1
            self.commits=0
        def commit(self):
            self.commits+=1
        def cursor(self):
            return FakeCursor(self)

    fake_source=FakeSource()
    longhaul_workload.set_session_binlog(
        fake_source,False)
    assert fake_source.binlog==0
    longhaul_workload.set_session_binlog(
        fake_source,True)
    assert fake_source.binlog==1
    assert fake_source.commits==2

    assert not longhaul_workload.source_ready(None)
    assert not longhaul_workload.source_ready(dict(source=[]))
    assert not longhaul_workload.source_ready(
        dict(source=[("db.events",None)]))
    assert longhaul_workload.source_ready(
        dict(source=[("db.events",7)]))
    baseline=dict(
        source=[("db.events",7)],
        generations=[
            ("starrocks.events","ready"),
            ("starrocks.agg_000","ready"),
        ],
        aggregate_tasks=[
            ("starrocks.agg_000","active"),
        ],
    )
    assert longhaul_workload.baseline_ready(
        baseline)
    not_ready=dict(baseline)
    not_ready["generations"]=[
        ("starrocks.events","history_staged"),
        ("starrocks.agg_000","ready"),
    ]
    assert not longhaul_workload.baseline_ready(
        not_ready)

    calls=[]
    def fake_execute(_cfg,sql):
        calls.append(sql)
        values=[
            int(value)
            for value in (
                sql.split("IN (",1)[1]
                .split(")",1)[0]
                .split(",")
            )
        ]
        return [
            (value,)
            for value in values
            if value%2==0
        ],["id"]

    with patch.object(
        longhaul_workload,
        "execute",side_effect=fake_execute
    ):
        visible=longhaul_workload.visible_markers(
            {},range(1000,2025))
    assert visible=={
        value
        for value in range(1000,2025)
        if value%2==0
    }
    assert len(calls)==3
    assert all("MAX(" not in sql for sql in calls)

    # Recovery must keep accepting source progress and include markers
    # created after the fault started in the catch-up boundary.
    commit_times={
        100:10.0,
    }
    healthy_markers={100}
    progress_calls=[]
    def progress():
        progress_calls.append(1)
        if len(progress_calls)==1:
            commit_times[101]=11.0
    clock=iter([
        12.0,12.1,12.2,12.3,
        12.4,12.5,12.6,12.7,
    ])
    with patch.object(
        longhaul_workload,"assert_live"
    ), patch.object(
        longhaul_workload,"visible_markers",
        side_effect=[{100},{101}]
    ), patch.object(
        longhaul_workload,"read_state",
        return_value=dict(
            log_durable_seq=7,
            base_applied_seq=7)
    ), patch.object(
        longhaul_workload.time,"monotonic",
        side_effect=lambda: next(clock)
    ), patch.object(
        longhaul_workload.time,"sleep"
    ):
        recovered=longhaul_workload.recover_after_fault(
            object(),Path("daemon.log"),
            Path("state.sqlite3"),{},
            commit_times,10,
            progress=progress,
            healthy_markers=healthy_markers)
    assert len(progress_calls)==2
    assert commit_times=={}
    assert len(recovered["latencies"])==2
    assert len(
        recovered["healthy_latencies"])==1
    assert not healthy_markers
    assert recovered["state"]["base_applied_seq"]==7

    startup_progress=[]
    startup_clock=iter([20.0,20.1,20.2])
    with patch.object(
        longhaul_workload,"assert_live"
    ), patch.object(
        longhaul_workload,"read_state",
        side_effect=[None,dict(ready=True)]
    ), patch.object(
        longhaul_workload.time,"monotonic",
        side_effect=lambda: next(startup_clock)
    ), patch.object(
        longhaul_workload.time,"sleep"
    ):
        started=longhaul_workload.wait_started(
            object(),Path("daemon.log"),
            Path("state.sqlite3"),
            progress=lambda: startup_progress.append(1))
    assert started==dict(ready=True)
    assert len(startup_progress)==2

    expected=[
        (0,2,10),
        (1,3,20),
    ]
    with patch.object(
        longhaul_workload,
        "_aggregate_rows_source",
        return_value=expected
    ), patch.object(
        longhaul_workload,
        "_aggregate_rows_target",
        side_effect=[
            list(expected),
            [(0,2,10),(1,3,21)],
        ]
    ):
        checks=longhaul_workload.aggregate_exactness(
            object(),{},["agg_000","agg_001"])
    assert checks["expected_rows"]==2
    assert checks["tables"]["agg_000"]["match"]
    assert not checks["tables"]["agg_001"]["match"]
    assert not checks["all_match"]
    assert checks["expected_digest"]==longhaul_workload._rows_digest(
        expected)


    with tempfile.TemporaryDirectory(
        prefix="m2s-longhaul-workdir-test-"
    ) as td:
        root=Path(td)
        assert longhaul_workload.path_within(
            root/"child",root)
        assert not longhaul_workload.path_within(
            root.parent/"sibling",root)
        explicit=root/"persistent"
        with longhaul_workload.work_directory(
            explicit
        ) as directory:
            assert directory==explicit.resolve()
            assert (
                directory/".m2s-longhaul-workdir"
            ).read_text(
                encoding="utf-8"
            )=="format_version=1\n"
            (directory/"daemon.log").write_text(
                "retained\n",encoding="utf-8")
        assert (
            explicit/"daemon.log"
        ).read_text(
            encoding="utf-8"
        )=="retained\n"

        occupied=root/"occupied"
        occupied.mkdir()
        (occupied/"stale").write_text(
            "x",encoding="utf-8")
        try:
            with longhaul_workload.work_directory(
                occupied
            ):
                raise AssertionError(
                    "non-empty work directory was accepted")
        except RuntimeError as exc:
            assert "must be empty" in str(exc)

        disposable=None
        with longhaul_workload.work_directory() as directory:
            disposable=directory
            assert directory.exists()
        assert disposable is not None
        assert not disposable.exists()

    with tempfile.TemporaryDirectory(
        prefix="m2s-longhaul-checkpoint-test-"
    ) as td:
        directory=Path(td)
        output=directory/"results"/"workload.json"
        first=dict(
            format_version=1,
            kind="m2s_longhaul_checkpoint",
            live_rows=50)
        path=longhaul_workload.write_checkpoint(
            output,first)
        assert path==(
            output.parent/
            "workload-checkpoint.json")
        assert json.loads(
            path.read_text(
                encoding="utf-8")
        )==first
        assert not Path(
            str(path)+".tmp"
        ).exists()
        second=dict(
            format_version=1,
            kind="m2s_longhaul_checkpoint",
            live_rows=100)
        replaced=longhaul_workload.write_checkpoint(
            output,second)
        assert replaced==path
        assert json.loads(
            path.read_text(
                encoding="utf-8")
        )["live_rows"]==100

    with tempfile.TemporaryDirectory(
        prefix="m2s-longhaul-helper-"
    ) as td:
        directory=Path(td)
        state=directory/"state.sqlite3"
        summary=Path(
            str(state)+".summary.json")
        metrics=Path(
            str(state)+".metrics.jsonl")
        state.write_bytes(b"x"*128)
        summary.write_text(
            json.dumps(dict(
                event="run_summary",
                tables={
                    "starrocks.events":dict(
                        max_rowset=17),
                    "starrocks.agg_000":dict(
                        max_rowset=9),
                },
                state=dict(
                    pending_bytes=0,
                    prepared_budget_used=0,
                    field_overflow_rows=0),
                stateful=dict(
                    physical=dict(
                        sizes={
                            "aggregate":dict(
                                rows=1024,
                                payload_bytes=4096),
                        })),
            )),
            encoding="utf-8")
        metrics.write_text(
            '{"event":"metrics"}\n',
            encoding="utf-8")
        output=directory/"artifacts"/"workload.json"
        debt=longhaul_workload.collect_final_debt(
            state)
        assert debt["state_storage_bytes"]>=128
        assert debt["stateful_rows"]==1024
        assert debt["stateful_payload_bytes"]==4096
        assert debt["max_rowset"]==17
        copied=longhaul_workload.copy_evidence(
            directory,output)
        assert set(copied)=={
            "daemon_summary","daemon_metrics"}
        assert (
            output.parent/"longhaul-daemon-summary.json"
        ).exists()
        assert (
            output.parent/"longhaul-daemon-metrics.jsonl"
        ).exists()

    print(
        "longhaul_workload_test ok percentile clean_worktree_probe seed_binlog_toggle "
        "source_ready baseline_ready "
        "per_transaction_sentinel continuous_source_during_fault crash_catchup aggregate_exactness "
        "work_directory_privacy_boundary work_directory_retention "
        "checkpoint_atomic_replace evidence_copy",
        flush=True,
    )


if __name__=="__main__":
    main()
