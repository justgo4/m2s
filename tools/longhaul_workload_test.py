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
    # A supervisor SIGTERM must unwind run() and restore the previous handler;
    # otherwise the daemon's independent process group can be orphaned.
    with patch.object(longhaul_workload.sys, "argv", ["longhaul_workload.py", "--isolated"]), \
         patch.object(longhaul_workload, "run", side_effect=KeyboardInterrupt("cancelled")), \
         patch.object(longhaul_workload.signal, "signal", return_value="previous") as signals:
        try:
            longhaul_workload.main()
            raise AssertionError("cancellation was swallowed")
        except KeyboardInterrupt:
            pass
        assert signals.call_count == 2
        assert signals.call_args_list[0].args == (
            longhaul_workload.signal.SIGTERM, longhaul_workload.cancellation_signal)
        assert signals.call_args_list[1].args == (
            longhaul_workload.signal.SIGTERM, "previous")
    assert longhaul_workload.percentile([],0.95) is None
    assert longhaul_workload.percentile(
        [5,1,3,2,4],0.50)==3
    assert longhaul_workload.percentile(
        [5,1,3,2,4],0.99)==4
    assert longhaul_workload.interval_overlap_seconds(
        20,68,0,60)==40
    assert longhaul_workload.interval_overlap_seconds(
        70,80,0,60)==0
    assert longhaul_workload.interval_overlap_seconds(
        -10,10,0,60)==10
    try:
        longhaul_workload.interval_overlap_seconds(
            2,1,0,60)
        raise AssertionError(
            "reversed interval was accepted")
    except ValueError:
        pass

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
    assert not longhaul_workload.source_ready(None)
    assert not longhaul_workload.source_ready(dict(source=[]))
    assert not longhaul_workload.source_ready(
        dict(source=[("db.events",None)]))
    assert longhaul_workload.source_ready(
        dict(source=[("db.events",7)]))
    assert longhaul_workload.dynamic_task_kind(
        "aggregate",2)=="aggregate"
    assert longhaul_workload.dynamic_task_kind(
        "join",1)=="join"
    assert longhaul_workload.dynamic_task_kind(
        "mixed",1)=="aggregate"
    assert longhaul_workload.dynamic_task_kind(
        "mixed",2)=="join"

    class FakeCursor:
        def __init__(self,rowcount=1):
            self.rowcount=int(rowcount)
            self.calls=[]
        def __enter__(self):
            return self
        def __exit__(self,*_):
            return False
        def execute(self,sql,args):
            self.calls.append((sql,args))

    class FakeSource:
        def __init__(self,rowcount=1):
            self.cur=FakeCursor(rowcount)
            self.commits=0
        def cursor(self):
            return self.cur
        def commit(self):
            self.commits+=1

    fake_source=FakeSource()
    mutation=longhaul_workload.mutate_join_right(
        fake_source,2,5000)
    assert mutation==dict(
        bucket=1,revision=2,
        label="fault-000002-bucket-0001")
    assert fake_source.commits==1
    assert len(fake_source.cur.calls)==1
    assert fake_source.cur.calls[0][1]==(
        "fault-000002-bucket-0001",1)

    failed_source=FakeSource(rowcount=0)
    try:
        longhaul_workload.mutate_join_right(
            failed_source,1,1)
        raise AssertionError(
            "missing JOIN dimension row was accepted")
    except RuntimeError:
        pass
    assert failed_source.commits==0

    formal_budget=longhaul_workload.topology_resource_budget(
        memory_mb=8192,
        cpu_cap=8,
        cpu_target=8,
        dynamic_tasks=10,
        load_mode="merge_async")
    assert formal_budget["ok"],formal_budget
    assert formal_budget["initial_physical_sinks"]==3
    assert formal_budget["final_physical_sinks"]==13
    assert formal_budget["minimum_per_engine_cap_mb"]>=128
    assert formal_budget["failing_physical_sinks"]==[]

    tight_budget=longhaul_workload.topology_resource_budget(
        memory_mb=4096,
        cpu_cap=8,
        cpu_target=8,
        dynamic_tasks=2,
        load_mode="merge_async")
    assert not tight_budget["ok"],tight_budget
    assert 4 in tight_budget["failing_physical_sinks"]

    lower_cpu_budget=longhaul_workload.topology_resource_budget(
        memory_mb=4096,
        cpu_cap=4,
        cpu_target=4,
        dynamic_tasks=2,
        load_mode="merge_async")
    assert lower_cpu_budget["ok"],lower_cpu_budget

    certification=SimpleNamespace(
        **longhaul_workload.p11_profile.PARAMETERS,
        work_directory=None,
    )
    mismatch=longhaul_workload.certification_mismatches(
        certification)
    assert set(mismatch)=={"work_directory"}
    certification.work_directory=Path("p11-evidence")
    assert longhaul_workload.certification_mismatches(
        certification)=={}

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
            progress=progress)
    assert len(progress_calls)==2
    assert commit_times=={}
    assert len(recovered["latencies"])==2
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

    event_expected=[
        (1,0,10,"a"),
        (2,0,20,"b"),
    ]
    with patch.object(
        longhaul_workload,
        "_event_rows_source_stream",
        return_value=iter(event_expected)
    ), patch.object(
        longhaul_workload,
        "_event_rows_target_stream",
        side_effect=[
            iter(event_expected),
            iter([
                (1,0,20,"a"),
                (2,0,10,"b"),
            ]),
        ]
    ):
        event_checks=longhaul_workload.event_exactness(
            object(),{},["events","events_bad"])
    assert (
        event_checks["comparison"]
        =="streamed_full_rows_v3"
    )
    assert (
        event_checks["scan_mode"]
        =="full_table_unbuffered"
    )
    assert event_checks["scan_passes"]==3
    assert event_checks["expected_rows"]==2
    assert event_checks["source_total_rows"]==2
    assert event_checks["source_uncovered_rows"]==0
    assert event_checks["tables"]["events"]["match"]
    assert not event_checks["tables"]["events_bad"]["match"]
    assert not event_checks["all_match"]

    # Preserve complete JOIN row identity. Swapping v keeps old COUNT/SUM
    # summaries unchanged but must fail the full-row multiset fingerprint.
    join_expected=[
        (1,0,"dim-0000",10),
        (2,0,"dim-0000",20),
    ]
    with patch.object(
        longhaul_workload,
        "_join_rows_source_stream",
        return_value=iter(join_expected)
    ), patch.object(
        longhaul_workload,
        "_join_rows_target_stream",
        side_effect=[
            iter(join_expected),
            iter([
                (1,0,"dim-0000",20),
                (2,0,"dim-0000",10),
            ]),
        ]
    ):
        join_checks=longhaul_workload.join_exactness(
            object(),{},["join_000","join_002"])
    assert join_checks["expected_rows"]==2
    assert join_checks["tables"]["join_000"]["match"]
    assert not join_checks["tables"]["join_002"]["match"]
    assert (
        join_checks["tables"]["join_002"][
            "mismatches"][0]["expected_digest"]
        !=join_checks["tables"]["join_002"][
            "mismatches"][0]["actual_digest"]
    )
    assert not join_checks["all_match"]

    # Full-table streaming has no bucket filter, so rows at 1024, -1 and NULL
    # are hashed and cannot hide outside the oracle domain.
    invalid_rows=(
        (3,1024,"unexpected",99),
        (3,-1,"unexpected",99),
        (3,None,"unexpected",99),
    )
    for invalid in invalid_rows:
        with patch.object(
            longhaul_workload,
            "_join_rows_source_stream",
            return_value=iter(join_expected)
        ), patch.object(
            longhaul_workload,
            "_join_rows_target_stream",
            return_value=iter(
                join_expected+[invalid])
        ):
            extra=longhaul_workload.join_exactness(
                object(),{},["join_extra"])
        assert not extra["all_match"],invalid
        assert extra["coverage_complete"]
        assert extra["tables"]["join_extra"]["rows"]==3
        assert extra["tables"]["join_extra"]["total_rows"]==3
        assert extra["tables"]["join_extra"]["uncovered_rows"]==0
        assert not extra["tables"]["join_extra"]["match"]

    with patch.object(
        longhaul_workload,
        "_event_rows_source_stream",
        return_value=iter(event_expected)
    ), patch.object(
        longhaul_workload,
        "_event_rows_target_stream",
        return_value=iter(
            event_expected+[(3,1024,99,"unexpected")])
    ):
        event_extra=longhaul_workload.event_exactness(
            object(),{},["events"])
    assert not event_extra["all_match"]
    assert event_extra["tables"]["events"]["rows"]==3

    with tempfile.TemporaryDirectory(
        prefix="m2s-longhaul-workdir-test-"
    ) as td:
        root=Path(td)
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
        "longhaul_workload_test ok percentile interval_overlap clean_worktree_probe source_ready "
        "per_transaction_sentinel continuous_source_during_fault crash_catchup "
        "mixed_task_selection join_right_fault_mutation topology_resource_budget "
        "aggregate_exactness join_exactness "
        "work_directory_retention certification_requires_persistent_workdir "
        "checkpoint_atomic_replace evidence_copy",
        flush=True,
    )


if __name__=="__main__":
    main()
