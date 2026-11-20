#!/usr/bin/env python3
"""SIGKILL the aggregate consumer inside its open transaction.

The child uses the existing fault_after_state hook, which runs after
aggregate_state.apply_transaction and before the outbox write and COMMIT.
The parent kills that process only after the hook marks the cut, and only
accepts an exit status of SIGKILL. Reopening the same SQLite file must show
the state, watermark, and outbox still at the previous commit. A later child
then publishes that batch once; another child must not publish it again.

This is the consumer and the synthetic source log. It is not a daemon
restart and not a MySQL binlog or StarRocks VISIBLE run.
"""
from decimal import Decimal
from pathlib import Path
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import aggregate_ir
import aggregate_log_consumer
import aggregate_outbox
import aggregate_shadow
import aggregate_state
import source_state


SCHEMA = [
    ("id", "bigint", "bigint", None, None, False),
    ("category", "varchar", "varchar(16)", None, "utf8mb4_bin", True),
    ("amount", "decimal", "decimal(18,2)", None, None, True),
]


def source_schema():
    return pa.schema([
        pa.field("id", pa.int64()),
        pa.field("category", pa.string()),
        pa.field("amount", pa.decimal128(18, 2)),
    ])


def batch(rows):
    return pa.Table.from_pylist([
        dict(
            id=row[0], category=row[1], amount=row[2],
            _sync_op=row[3], _sync_order=index)
        for index, row in enumerate(rows)
    ], schema=pa.schema(list(source_schema()) + [
        pa.field("_sync_op", pa.int8()),
        pa.field("_sync_order", pa.int64()),
    ]))


def open_db(path):
    con = sqlite3.connect(path, timeout=30, isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    aggregate_state.install(con)
    return con


def plan():
    return aggregate_ir.compile_sql(
        "db.orders", SCHEMA,
        'SELECT "category", COUNT(*) AS "n", SUM("amount") AS "total" '
        'FROM arrow_batch GROUP BY "category"')


def prepare(path):
    ir = plan()
    con = open_db(path)
    source_state.register_relation(
        con, "db.orders", "source-1", source_schema(), ["id"])
    source_state.stage_snapshot_batch(
        con, "db.orders",
        pa.Table.from_pylist([], schema=source_schema()),
        cursor=None, is_last=True)
    aggregate_log_consumer.ensure_consumer(
        con, "agg-task", "db.orders", 3, ir, "agg-state", 0)
    parts = [source_state.prepare_part("db.orders", batch([
        (1, "a", Decimal("10.00"), 0),
        (2, "a", Decimal("5.00"), 0),
    ]))]
    seq = source_state.log_commit(
        con, "source-1", ("binlog.000001", 100), None, parts)
    source_state.apply_pending(con)
    if seq != 1:
        raise AssertionError(seq)
    con.close()


def child_kill(path, marker):
    ir = plan()
    con = open_db(path)

    def hold(seq):
        handle = open(marker, "w")
        handle.write(str(seq))
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        signal.pause()

    aggregate_log_consumer.process_next(
        con, "agg-task", ir, fault_after_state=hold, shadow=True)
    con.close()


def child_publish(path):
    ir = plan()
    con = open_db(path)
    result = aggregate_log_consumer.process_next(
        con, "agg-task", ir, shadow=True)
    text = "none" if result is None else str(result["source_seq"])
    print(text, flush=True)
    con.close()


def snapshot(con):
    state = aggregate_state.state_info(con, "agg-state")
    consumer = source_state.consumer_info(con, "agg-task")
    try:
        published = aggregate_outbox.commit_info(con, "agg-task", 1)
    except KeyError:
        published = None
    rows = {
        row["category"]: (row["n"], format(row["total"], "f"))
        for row in aggregate_state.read_rows(con, "agg-state")
    }
    return (
        int(state["watermark"]),
        int(consumer["watermark"]),
        None if published is None else int(published["nrows"]),
        rows,
    )


def run_child(mode, path, marker=None):
    command = [sys.executable, str(Path(__file__).resolve()), mode, path]
    if marker is not None:
        command.append(marker)
    return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "child-kill":
        child_kill(sys.argv[2], sys.argv[3])
        return
    if len(sys.argv) > 1 and sys.argv[1] == "child-publish":
        child_publish(sys.argv[2])
        return
    with tempfile.TemporaryDirectory(prefix="m2s-agg-kill-") as td:
        path = os.path.join(td, "state.sqlite3")
        marker = os.path.join(td, "cut")
        prepare(path)
        killer = run_child("child-kill", path, marker)
        deadline = time.time() + 20
        while not os.path.exists(marker):
            if killer.poll() is not None:
                err = killer.stderr.read().decode("utf-8", "replace")
                raise AssertionError(
                    "child left before the cut exit=%s %s"
                    % (killer.returncode, err[-400:]))
            if time.time() > deadline:
                os.kill(killer.pid, signal.SIGKILL)
                killer.wait(5)
                raise AssertionError("cut marker was not written")
            time.sleep(0.05)
        os.kill(killer.pid, signal.SIGKILL)
        code = killer.wait(5)
        if code != -signal.SIGKILL:
            raise AssertionError("exit was not SIGKILL: %s" % code)
        if open(marker).read().strip() != "1":
            raise AssertionError("cut was not source seq 1")
        con = open_db(path)
        killed = snapshot(con)
        con.close()
        if killed != (0, 0, None, {}):
            raise AssertionError(killed)
        published = run_child("child-publish", path)
        out, err = published.communicate(timeout=30)
        if published.returncode != 0:
            raise AssertionError(err.decode("utf-8", "replace")[-400:])
        if out.decode().strip() != "1":
            raise AssertionError(out)
        con = open_db(path)
        aggregate_shadow.verify(
            con, plan(), "agg-state", 1, "db.orders",
            aggregate_log_consumer.transform_commit)
        after = snapshot(con)
        con.close()
        if after != (1, 1, 1, {"a": (2, "15.00")}):
            raise AssertionError(after)
        again = run_child("child-publish", path)
        out, err = again.communicate(timeout=30)
        if again.returncode != 0:
            raise AssertionError(err.decode("utf-8", "replace")[-400:])
        if out.decode().strip() != "none":
            raise AssertionError(out)
        con = open_db(path)
        final = snapshot(con)
        con.close()
        if final != after:
            raise AssertionError(final)
    print(
        "aggregate_shadow_kill exit=%d cut_seq=1 rolled_back_watermark=0 "
        "outbox_seq1=absent replay_seq=1 replay_rows=1 second_consume=none "
        "shadow_ok=1 starrocks=not_run daemon=not_run"
        % (-signal.SIGKILL),
        flush=True)
    print("aggregate_shadow_kill_test ok", flush=True)


if __name__ == "__main__":
    main()
