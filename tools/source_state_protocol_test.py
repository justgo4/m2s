#!/usr/bin/env python3
from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile

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

        before = source_state.gc(con)
        assert before["floor"] == 1
        snapshot_values(con, pin1, {1: "a2", 2: "b"})
        assert [c["seq"] for c in source_state.read_commits(con, 0)] == [1, 2]

        source_state.release_pin(con, pin1["pin_id"])
        after = source_state.gc(con)
        assert after["floor"] == 2
        assert [c["seq"] for c in source_state.read_commits(con, 0)] == [2]
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
        con.close()

    print("source_state_protocol_test ok", flush=True)


if __name__ == "__main__":
    main()
