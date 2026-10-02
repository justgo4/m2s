#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
from unittest.mock import patch

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import source_state


def open_db(path):
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    return con


def assert_gc_index_contract(directory):
    path=os.path.join(
        directory,"gc-index.sqlite3")
    con=open_db(path)
    indexes={
        str(row[1]) for row in con.execute(
            "PRAGMA index_list(source_versions)")
    }
    assert "source_versions_gc" in indexes
    assert "source_versions_visible" not in indexes

    # Simulate an on-disk database created by the earlier wide-index build.
    # Reinstall must remove it so upgrades get the same write-amplification
    # profile as fresh databases.
    con.execute("""
        CREATE INDEX source_versions_visible
        ON source_versions(
            table_name,valid_from,valid_to,deleted,pk)
    """)
    con.close()
    con=open_db(path)
    indexes={
        str(row[1]) for row in con.execute(
            "PRAGMA index_list(source_versions)")
    }
    assert "source_versions_gc" in indexes
    assert "source_versions_visible" not in indexes

    sql=con.execute("""
        SELECT sql FROM sqlite_master
        WHERE type='index' AND name='source_versions_gc'
    """).fetchone()
    assert sql is not None
    normalized=" ".join(str(sql[0]).split()).upper()
    assert "ON SOURCE_VERSIONS(VALID_TO)" in normalized
    assert "WHERE VALID_TO IS NOT NULL" in normalized
    plan=" ".join(
        str(row[3])
        for row in con.execute("""
            EXPLAIN QUERY PLAN
            DELETE FROM source_versions
            WHERE valid_to IS NOT NULL
              AND valid_to<=?
        """,(100,))
    )
    assert "source_versions_gc" in plan,plan
    con.close()




def assert_scratch_schema_migration(directory):
    path=os.path.join(
        directory,"scratch-schema-migration.sqlite3")
    con=sqlite3.connect(
        path,timeout=30,isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    # Model the durable scratch schema left by builds before TEMP staging.
    # Rows here are deliberately junk: neither table is authoritative.
    con.executescript("""
        CREATE TABLE source_apply_actions(
            marker INTEGER NOT NULL);
        INSERT INTO source_apply_actions VALUES(1);
        CREATE TABLE source_snapshot_rows(
            marker INTEGER NOT NULL);
        INSERT INTO source_snapshot_rows VALUES(1);
    """)
    source_state.install(con)
    for name in (
        "source_apply_actions",
        "source_snapshot_rows",
    ):
        assert con.execute("""
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name=?
        """,(name,)).fetchone() is None

    # Runtime staging is connection-local and status must report that schema,
    # not stale/main objects from an older database format.
    source_state._ensure_apply_staging(con)
    source_state._ensure_snapshot_staging(con)
    assert con.execute("""
        SELECT 1 FROM sqlite_temp_master
        WHERE type='table' AND name='source_apply_actions'
    """).fetchone() is not None
    assert con.execute("""
        SELECT 1 FROM sqlite_temp_master
        WHERE type='table' AND name='source_snapshot_rows'
    """).fetchone() is not None
    pipeline=source_state.status(con)["pipeline_stats"]
    assert pipeline["apply_staging_rows"]==0
    assert pipeline["snapshot_staging_rows"]==0
    con.close()


def assert_temp_store_contract(directory):
    path=os.path.join(
        directory,"temp-store-contract.sqlite3")
    con=open_db(path)
    before=source_state.temp_store_info(con)
    source_state._ensure_apply_staging(con)
    after=source_state.temp_store_info(con)
    assert after["mode"]==1,(before,after)
    assert after["name"]=="file",after
    source_state._ensure_snapshot_staging(con)
    assert source_state.temp_store_info(con)==after
    assert con.execute("""
        SELECT COUNT(*) FROM sqlite_temp_master
        WHERE type='table'
          AND name IN ('source_apply_actions','source_snapshot_rows')
    """).fetchone()[0]==2
    con.close()


def assert_bounded_gc_contract(directory):
    path=os.path.join(
        directory,"bounded-gc.sqlite3")
    con=open_db(path)
    source_state.register_relation(
        con,"db.orders","source-bounded",
        source_schema(),["id"])
    source_state.stage_snapshot_batch(
        con,"db.orders",batch([(0,1,"v0")]),
        cursor=(1,),is_last=True)

    for seq in range(1,7):
        part=source_state.prepare_part(
            "db.orders",batch([(0,1,"v%d" % seq)]))
        assert source_state.log_commit(
            con,"source-bounded",
            ("binlog.000010",100+seq),None,[part])==seq
        assert source_state.apply_pending(con)==1

    first=source_state.gc(
        con,version_limit=2,commit_limit=2)
    assert first["floor"]==6
    assert first["min_readable_seq"]==6
    assert first["versions"]==2
    assert first["commits"]==2
    assert first["versions_pending"]
    assert first["commits_pending"]
    assert not first["complete"]

    deleted_versions=first["versions"]
    deleted_commits=first["commits"]
    for _ in range(10):
        step=source_state.gc(
            con,version_limit=2,commit_limit=2)
        deleted_versions+=step["versions"]
        deleted_commits+=step["commits"]
        if step["complete"]:
            break
    else:
        raise AssertionError("bounded source GC did not converge")

    assert deleted_versions==6
    assert deleted_commits==5
    assert con.execute("""
        SELECT COUNT(*) FROM source_versions
        WHERE valid_to IS NOT NULL AND valid_to<=6
    """).fetchone()[0]==0
    assert [
        item["seq"] for item in source_state.read_commits(con,0,allow_truncated=True)
    ]==[6]

    pin=source_state.acquire_pin(
        con,"bounded-current",["db.orders"])
    assert pin["watermark"]==6
    snapshot_values(con,pin,{1:"v6"})
    source_state.release_pin(con,pin["pin_id"])

    for key,value in (
        ("version_limit",0),
        ("commit_limit",0),
    ):
        kwargs={key:value}
        try:
            source_state.gc(con,**kwargs)
            raise AssertionError(
                "%s=0 was accepted" % key)
        except ValueError:
            pass
    con.close()

def source_schema():
    return pa.schema([
        pa.field("id", pa.int64()),
        pa.field("value", pa.large_string()),
    ])


def batch(rows):
    table = pa.Table.from_pylist(
        [dict(id=row[1], value=row[2]) for row in rows],
        schema=source_schema(),
    )
    return table.append_column(
        "_sync_op", pa.array([row[0] for row in rows], type=pa.int8())
    ).append_column(
        "_sync_order", pa.array(range(len(rows)), type=pa.int64())
    )


def snapshot_values(con, pin, expected):
    cursor = None
    values = {}
    while True:
        table, cursor = source_state.read_snapshot_batch(
            con, pin["pin_id"], "db.orders", cursor, limit=1
        )
        for row in table.select(["id", "value"]).to_pylist():
            values[int(row["id"])] = row["value"]
        if cursor is None or table.num_rows == 0:
            break
    assert values == expected, (values, expected)


def assert_log_stats_migration(directory):
    path=os.path.join(
        directory,"log-stats-migration.sqlite3")
    con=open_db(path)
    source_state.register_relation(
        con,"db.orders","source-migration",
        source_schema(),["id"])
    p1=source_state.prepare_part(
        "db.orders",batch([
            (0,1,"a"),(0,2,"b"),
        ]))
    p2=source_state.prepare_part(
        "db.orders",batch([
            (1,1,"a"),(0,1,"a2"),
            (0,3,"c"),
        ]))
    assert source_state.log_commit(
        con,"source-migration",
        ("binlog.000009",100),None,[p1])==1
    assert source_state.log_commit(
        con,"source-migration",
        ("binlog.000009",120),None,[p2])==2
    before=source_state.status(con)
    stats=before["log_stats"]["db.orders"]
    assert stats["commits"]==2
    assert stats["event_rows"]==5
    assert stats["payload_bytes"]>0
    assert before["log_stats_started_seq"]==1
    assert before["log_stats_started_at"] is not None

    # Simulate an interrupted/old counter migration. Reinstall must derive
    # exact retained evidence under one transaction before marking v1 done.
    con.execute("""
        UPDATE source_log_stats
        SET commits=999,event_rows=999,payload_bytes=999
        WHERE table_name='db.orders'
    """)
    con.execute("""
        DELETE FROM source_state_meta
        WHERE key IN (
            'log_stats_v1',
            'log_stats_started_seq',
            'log_stats_started_at'
        )
    """)
    source_state.install(con)
    rebuilt=source_state.status(con)
    assert rebuilt["log_stats"]["db.orders"]==stats
    assert rebuilt["log_stats_started_seq"]==1
    assert rebuilt["log_stats_started_at"] is not None
    assert source_state._meta_int(
        con,"log_stats_v1",0)==1

    # Empty source transactions advance the authoritative sequence but do not
    # invent row-rate evidence because no table part exists.
    assert source_state.log_commit(
        con,"source-migration",
        ("binlog.000009",140),None,[])==3
    after=source_state.status(con)
    assert after["log_stats"]["db.orders"]==stats
    con.close()


def assert_apply_staging_atomicity(directory):
    path=os.path.join(
        directory,"apply-staging.sqlite3")
    con=open_db(path)
    source_state.register_relation(
        con,"db.orders","source-stage",
        source_schema(),["id"])
    source_state.stage_snapshot_batch(
        con,"db.orders",batch([]),
        cursor=None,is_last=True)

    parts=[
        source_state.prepare_part(
            "db.orders",batch([
                (0,1,"a"),
                (0,2,"b"),
                (0,1,"a2"),
            ])),
        source_state.prepare_part(
            "db.orders",batch([
                (1,2,"b"),
                (0,3,"c"),
                (0,1,"a3"),
            ])),
    ]
    seq=source_state.log_commit(
        con,"source-stage",
        ("binlog.000008",100),None,
        parts)
    assert seq==1

    original=source_state.decode_batch
    calls=[0]
    def fail_second(payload):
        calls[0]+=1
        if calls[0]==2:
            raise RuntimeError(
                "synthetic second-part decode failure")
        return original(payload)

    with patch.object(
        source_state,"decode_batch",
        side_effect=fail_second
    ):
        try:
            source_state.apply_pending(con)
            raise AssertionError(
                "partial staged apply unexpectedly committed")
        except RuntimeError as exc:
            assert "second-part" in str(exc)

    assert source_state.base_applied_seq(con)==0
    assert con.execute(
        "SELECT COUNT(*) "
        "FROM source_apply_actions"
    ).fetchone()[0]==0
    assert con.execute(
        "SELECT COUNT(*) "
        "FROM source_versions "
        "WHERE valid_from>0"
    ).fetchone()[0]==0
    assert con.execute(
        "SELECT base_applied "
        "FROM source_commits WHERE seq=1"
    ).fetchone()[0]==0

    assert source_state.apply_pending(con)==1
    assert source_state.base_applied_seq(con)==1
    assert con.execute(
        "SELECT COUNT(*) "
        "FROM source_apply_actions"
    ).fetchone()[0]==0
    pin=source_state.acquire_pin(
        con,"stage-result",["db.orders"])
    snapshot_values(
        con,pin,{1:"a3",3:"c"})
    source_state.release_pin(
        con,pin["pin_id"])
    status=source_state.status(con)
    assert status["pipeline_stats"][
        "apply_input_rows"]==6
    assert status["pipeline_stats"][
        "apply_actions"]==3
    assert status["pipeline_stats"][
        "apply_staging_rows"]==0
    con.close()


def child_crash_after_log(path):
    con = open_db(path)
    source_state.register_relation(
        con, "db.orders", "source-1", source_schema(), ["id"]
    )
    part = source_state.prepare_part(
        "db.orders", batch([(0, 4, "d")])
    )
    source_state.log_commit(
        con, "source-1", ("binlog.000001", 140), None, [part]
    )
    con.close()
    os._exit(23)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--crash-after-log":
        child_crash_after_log(sys.argv[2])

    with tempfile.TemporaryDirectory(prefix="m2s-source-state-") as td:
        assert_log_stats_migration(td)
        assert_apply_staging_atomicity(td)
        assert_scratch_schema_migration(td)
        assert_temp_store_contract(td)
        assert_gc_index_contract(td)
        assert_bounded_gc_contract(td)
        path = os.path.join(td, "state.sqlite3")
        con = open_db(path)
        source_state.register_relation(
            con, "db.orders", "source-1", source_schema(), ["id"]
        )

        source_state.stage_snapshot_batch(
            con, "db.orders", batch([(0, 1, "a"), (0, 2, "b")]),
            cursor=(2,), is_last=True,
        )
        assert source_state.snapshot_safe_watermark(
            con, ["db.orders"]
        ) == 0

        p1 = source_state.prepare_part(
            "db.orders",
            batch([(1, 1, "a"), (0, 1, "a2")]),
        )
        seq1 = source_state.log_commit(
            con, "source-1", ("binlog.000001", 100), None, [p1]
        )
        assert seq1 == 1
        state = source_state.status(con)
        assert state["log_durable_seq"] == 1
        assert state["base_applied_seq"] == 0
        # A durable log alone is not snapshot-safe.
        assert source_state.snapshot_safe_watermark(
            con, ["db.orders"]
        ) == 0

        assert source_state.apply_pending(con) == 1
        pin1 = source_state.acquire_pin(con, "build-q1", ["db.orders"])
        assert pin1["watermark"] == 1
        snapshot_values(con, pin1, {1: "a2", 2: "b"})

        p2 = source_state.prepare_part(
            "db.orders",
            batch([(1, 1, "a2"), (0, 3, "c")]),
        )
        assert source_state.log_commit(
            con, "source-1", ("binlog.000001", 120), None, [p2]
        ) == 2
        assert source_state.apply_pending(con) == 1

        # The fixed W remains readable while the live base advances.
        snapshot_values(con, pin1, {1: "a2", 2: "b"})
        pin2 = source_state.acquire_pin(con, "build-q2", ["db.orders"])
        assert pin2["watermark"] == 2
        snapshot_values(con, pin2, {2: "b", 3: "c"})

        assert source_state.min_readable_seq(con) == 0
        before = source_state.gc(con)
        assert before["floor"] == 1
        assert before["min_readable_seq"] == 1
        snapshot_values(con, pin1, {1: "a2", 2: "b"})
        assert [c["seq"] for c in source_state.read_commits(con, 0, allow_truncated=True)] == [1, 2]

        source_state.release_pin(con, pin1["pin_id"])
        after = source_state.gc(con)
        assert after["floor"] == 2
        assert after["min_readable_seq"] == 2
        try:
            source_state.read_commits(con, 0)
            raise AssertionError(
                "source changelog gap was silently truncated")
        except RuntimeError as exc:
            assert "changelog gap" in str(exc)
        assert [c["seq"] for c in source_state.read_commits(
            con, 0, allow_truncated=True
        )] == [2]
        snapshot_values(con, pin2, {2: "b", 3: "c"})

        source_state.release_pin(con, pin2["pin_id"])
        con.close()

        child = subprocess.run(
            [sys.executable, __file__, "--crash-after-log", path],
            check=False,
        )
        assert child.returncode == 23
        con = open_db(path)
        state = source_state.status(con)
        assert state["log_durable_seq"] == 3
        assert state["base_applied_seq"] == 2
        assert state["snapshot_safe_seq"] == 2

        # Restart replays the authoritative log into base.
        assert source_state.apply_pending(con) == 1
        state = source_state.status(con)
        assert state["log_durable_seq"] == 3
        assert state["base_applied_seq"] == 3
        pin3 = source_state.acquire_pin(con, "after-restart", ["db.orders"])
        snapshot_values(con, pin3, {2: "b", 3: "c", 4: "d"})
        source_state.release_pin(con, pin3["pin_id"])

        # A source transaction with no row event must still advance the
        # authoritative sequence so downstream progress/GC never waits for an
        # output that cannot exist.
        empty_seq = source_state.log_commit(
            con, "source-1", ("binlog.000001", 160), None, [])
        assert empty_seq == 4
        assert source_state.log_durable_seq(con) == 4
        assert source_state.base_applied_seq(con) == 3
        assert source_state.apply_pending(con) == 1
        assert source_state.base_applied_seq(con) == 4
        pin4 = source_state.acquire_pin(con, "empty-commit", ["db.orders"])
        assert pin4["watermark"] == 4
        snapshot_values(con, pin4, {2: "b", 3: "c", 4: "d"})
        source_state.release_pin(con, pin4["pin_id"])

        # Durable consumers, unlike build pins, represent changelog readers.
        # They advance even across zero-output commits and bound GC after restart.
        consumer = source_state.register_consumer(
            con, "task-q1", 2, owner="task:q1",
            metadata={"plan_version": 7})
        assert consumer["watermark"] == 2
        assert source_state.retention_floor(con) == 2
        # A retention decision alone does not change physical readability.
        assert source_state.min_readable_seq(con) == 2
        assert source_state.gc(con)["floor"] == 2
        assert [c["seq"] for c in source_state.read_commits(con, 0, allow_truncated=True)] == [2, 3, 4]

        consumer = source_state.advance_consumer(con, "task-q1", 3)
        assert consumer["watermark"] == 3
        assert source_state.gc(con)["floor"] == 3
        assert [c["seq"] for c in source_state.read_commits(con, 0, allow_truncated=True)] == [3, 4]

        # Commit 4 has no row event, but computation can still advance through it.
        consumer = source_state.advance_consumer(con, "task-q1", 4)
        assert consumer["watermark"] == 4
        try:
            source_state.advance_consumer(con, "task-q1", 3)
            raise AssertionError("consumer watermark regression was accepted")
        except ValueError:
            pass
        try:
            source_state.advance_consumer(con, "task-q1", 5)
            raise AssertionError("consumer advanced beyond applied base")
        except ValueError:
            pass
        con.close()

        con = open_db(path)
        persisted = source_state.consumer_info(con, "task-q1")
        assert persisted["watermark"] == 4
        assert persisted["metadata"] == {"plan_version": 7}
        assert source_state.status(con)["consumers"] == [
            dict(consumer_id="task-q1", watermark=4, owner="task:q1")
        ]
        source_state.remove_consumer(con, "task-q1")
        assert source_state.status(con)["consumers"] == []
        con.close()

    print(
        "source_state_protocol_test ok fixed_w gc gc_index bounded_gc crash_replay "
        "consumer_frontier source_rate_migration",
        flush=True,
    )


if __name__ == "__main__":
    main()
