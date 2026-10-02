#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import stateful_rebuild


def connection(path):
    con=sqlite3.connect(
        path,timeout=10,isolation_level=None)
    con.execute("PRAGMA busy_timeout=10000")
    con.execute("PRAGMA journal_mode=WAL")
    return con


def run_parallel(count,fn):
    barrier=threading.Barrier(count)
    results=[]
    errors=[]
    lock=threading.Lock()

    def worker(index):
        try:
            barrier.wait()
            value=fn(index)
            with lock:
                results.append(value)
        except BaseException as exc:
            with lock:
                errors.append(exc)

    threads=[
        threading.Thread(target=worker,args=(index,))
        for index in range(count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(15)
        assert not thread.is_alive()
    return results,errors


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-rebuild-concurrency-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=connection(path)
        stateful_rebuild.install(con)
        con.close()

        def identical(_):
            local=connection(path)
            try:
                return stateful_rebuild.begin(
                    local,"aggregate","starrocks.same",
                    "old-same","new-same","same")
            finally:
                local.close()

        results,errors=run_parallel(8,identical)
        assert not errors,[
            (type(exc).__name__,str(exc))
            for exc in errors
        ]
        assert len(results)==8
        assert len({
            item["shadow_target"]
            for item in results
        })==1

        con=connection(path)
        assert stateful_rebuild.info(
            con,"starrocks.same"
        )["new_task_id"]=="new-same"
        con.close()

        def conflicting(index):
            local=connection(path)
            try:
                return stateful_rebuild.begin(
                    local,"aggregate","starrocks.conflict",
                    "old-conflict","new-conflict-%d" % index,
                    "conflict")
            finally:
                local.close()

        results,errors=run_parallel(8,conflicting)
        assert len(results)==1
        assert len(errors)==7
        assert all(
            isinstance(exc,RuntimeError)
            and "identity changed" in str(exc)
            for exc in errors
        ),[
            (type(exc).__name__,str(exc))
            for exc in errors
        ]

        def cross_sink(index):
            local=connection(path)
            try:
                return stateful_rebuild.begin(
                    local,"inner_join",
                    "starrocks.shared_%d" % index,
                    "old-shared-%d" % index,
                    "new-shared",
                    "shared_%d" % index)
            finally:
                local.close()

        results,errors=run_parallel(2,cross_sink)
        assert len(results)==1
        assert len(errors)==1
        assert isinstance(errors[0],RuntimeError)
        assert "identity collision" in str(errors[0])
        assert not isinstance(errors[0],sqlite3.IntegrityError)

        def shared_old(index):
            local=connection(path)
            try:
                return stateful_rebuild.begin(
                    local,"aggregate",
                    "starrocks.old_owner_%d" % index,
                    "one-old-owner",
                    "new-old-owner-%d" % index,
                    "old_owner_%d" % index)
            finally:
                local.close()

        results,errors=run_parallel(2,shared_old)
        assert len(results)==1
        assert len(errors)==1
        assert isinstance(errors[0],RuntimeError)
        assert (
            "active owner" in str(errors[0])
            or "identity collision" in str(errors[0])
        )
        assert not isinstance(errors[0],sqlite3.IntegrityError)

        con=connection(path)
        phase=stateful_rebuild.begin(
            con,"aggregate","starrocks.phase",
            "old-phase","new-phase","phase")
        stateful_rebuild.freeze_frontier(
            con,phase["sink_key"],77)
        con.close()

        def ready_retry(_):
            local=connection(path)
            try:
                return stateful_rebuild.mark_ready_to_swap(
                    local,"starrocks.phase")
            finally:
                local.close()

        results,errors=run_parallel(8,ready_retry)
        assert not errors,[
            (type(exc).__name__,str(exc))
            for exc in errors
        ]
        assert len(results)==8
        assert {
            item["phase"] for item in results
        }=={"ready_to_swap"}

        con=connection(path)
        failed=stateful_rebuild.begin(
            con,"inner_join","starrocks.fail",
            "old-fail","new-fail","fail")
        con.close()

        def fail_retry(_):
            local=connection(path)
            try:
                return stateful_rebuild.fail(
                    local,failed["sink_key"],
                    "synthetic concurrent failure")
            finally:
                local.close()

        results,errors=run_parallel(8,fail_retry)
        assert not errors,[
            (type(exc).__name__,str(exc))
            for exc in errors
        ]
        assert len(results)==8
        assert {
            (item["phase"],item["error"])
            for item in results
        }=={(
            "failed",
            "synthetic concurrent failure",
        )}

        con=connection(path)
        try:
            try:
                stateful_rebuild.fail(
                    con,failed["sink_key"],
                    "different retry reason")
                raise AssertionError(
                    "failed rebuild accepted changed retry error")
            except RuntimeError as exc:
                assert "error changed" in str(exc)
        finally:
            con.close()

        legacy=str(Path(directory)/"legacy.sqlite3")
        con=connection(legacy)
        stateful_rebuild.install(con)
        con.execute(
            "DROP TRIGGER stateful_rebuild_task_owner_insert")
        now=1.0
        con.execute("""
            INSERT INTO stateful_rebuilds(
                sink_key,kind,old_task_id,new_task_id,
                logical_target,shadow_target,original_comment,
                frontier,phase,error,created,updated)
            VALUES(?,?,?,?,?,?,?,NULL,'building_shadow','',?,?)
        """,(
            "starrocks.legacy_a","aggregate",
            "legacy-shared-old","legacy-new-a",
            "legacy_a","legacy_shadow_a","",now,now))
        con.execute("""
            INSERT INTO stateful_rebuilds(
                sink_key,kind,old_task_id,new_task_id,
                logical_target,shadow_target,original_comment,
                frontier,phase,error,created,updated)
            VALUES(?,?,?,?,?,?,?,NULL,'building_shadow','',?,?)
        """,(
            "starrocks.legacy_b","aggregate",
            "legacy-shared-old","legacy-new-b",
            "legacy_b","legacy_shadow_b","",now+1,now+1))
        try:
            stateful_rebuild.install(con)
            raise AssertionError(
                "ambiguous legacy task ownership was accepted")
        except RuntimeError as exc:
            assert "ownership is ambiguous" in str(exc)
        finally:
            con.close()

    print(
        "stateful_rebuild_concurrency_test ok "
        "identical_begin conflicting_begin unique_collision "
        "old_task_atomic_owner phase_retry fail_retry "
        "legacy_ambiguity_fail_closed no_sqlite_integrity_leak",
        flush=True,
    )


if __name__=="__main__":
    main()
