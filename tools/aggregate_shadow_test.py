#!/usr/bin/env python3
"""COUNT/SUM shadow on the aggregate log consumer.

The publish path is still aggregate_state plus the outbox. Shadow is off
unless the caller asks for it. These checks use a synthetic source log,
not a live MySQL or StarRocks service.
"""
from decimal import Decimal
from pathlib import Path
import os
import sqlite3
import sys
import tempfile

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


def add_commit(con, rows, pos):
    parts = [source_state.prepare_part("db.orders", batch(rows))]
    seq = source_state.log_commit(
        con, "source-1", ("binlog.000001", pos), None, parts)
    source_state.apply_pending(con)
    return seq


def groups(con):
    found = {}
    for row in aggregate_log_consumer.state_rows(con, "agg-task"):
        found[row["category"]] = (
            row["n"], None if row["total"] is None else format(row["total"], "f"))
    return found


def consume(con, ir, shadow=True):
    return aggregate_log_consumer.process_next(
        con, "agg-task", ir, shadow=shadow)


def main():
    ir = plan()
    with tempfile.TemporaryDirectory(prefix="m2s-agg-shadow-") as td:
        path = os.path.join(td, "state.sqlite3")
        con = open_db(path)
        source_state.register_relation(
            con, "db.orders", "source-1", source_schema(), ["id"])
        source_state.stage_snapshot_batch(
            con, "db.orders",
            pa.Table.from_pylist([], schema=source_schema()),
            cursor=None, is_last=True)
        aggregate_log_consumer.ensure_consumer(
            con, "agg-task", "db.orders", 3, ir, "agg-state", 0)

        add_commit(con, [
            (1, "a", Decimal("10.00"), 0),
            (2, "a", Decimal("5.00"), 0),
            (3, "b", Decimal("7.00"), 0),
        ], 100)
        first = consume(con, ir)
        if first["source_seq"] != 1:
            raise AssertionError(first)
        if groups(con) != {"a": (2, "15.00"), "b": (1, "7.00")}:
            raise AssertionError(groups(con))
        if aggregate_outbox.commit_info(con, "agg-task", 1)["nrows"] != 2:
            raise AssertionError("outbox missing the compared commit")
        state = aggregate_state.state_info(con, "agg-state")
        consumer = source_state.consumer_info(con, "agg-task")
        if int(state["watermark"]) != 1 or int(consumer["watermark"]) != 1:
            raise AssertionError((state["watermark"], consumer["watermark"]))
        again = source_state.read_commits(con, 0, through_seq=1, limit=1)[0]
        same = aggregate_log_consumer.transform_commit(
            again, "db.orders", ir)
        replay = aggregate_state.apply_transaction(
            con, "agg-state", 1, same)
        if replay:
            raise AssertionError("same watermark was applied again")

        add_commit(con, [
            (2, "a", Decimal("5.00"), 1),
            (2, "a", Decimal("8.00"), 0),
        ], 120)
        consume(con, ir)
        if groups(con)["a"] != (2, "18.00"):
            raise AssertionError(groups(con))

        add_commit(con, [
            (2, "a", Decimal("8.00"), 1),
            (2, "b", Decimal("8.00"), 0),
        ], 140)
        consume(con, ir)
        if groups(con) != {"a": (1, "10.00"), "b": (2, "15.00")}:
            raise AssertionError(groups(con))

        add_commit(con, [(1, "a", Decimal("10.00"), 1)], 160)
        consume(con, ir)
        if "a" in groups(con) or groups(con)["b"] != (2, "15.00"):
            raise AssertionError(groups(con))

        add_commit(con, [
            (2, "b", Decimal("8.00"), 1),
            (3, "b", Decimal("7.00"), 1),
        ], 180)
        consume(con, ir)
        if groups(con):
            raise AssertionError(groups(con))
        if int(aggregate_state.state_info(con, "agg-state")["watermark"]) != 5:
            raise AssertionError("watermark drifted")

        add_commit(con, [(4, "c", Decimal("1.00"), 0)], 200)

        def mismatch(con, consumer_id, ir, seq):
            raise aggregate_shadow.ShadowMismatch("injected")

        try:
            consume(con, ir, shadow=mismatch)
            raise AssertionError("mismatch published")
        except aggregate_shadow.ShadowMismatch as exc:
            if "injected" not in str(exc):
                raise
        if int(aggregate_state.state_info(con, "agg-state")["watermark"]) != 5:
            raise AssertionError("mismatch kept the state write")
        if int(source_state.consumer_info(con, "agg-task")["watermark"]) != 5:
            raise AssertionError("mismatch kept the consumer watermark")
        try:
            aggregate_outbox.commit_info(con, "agg-task", 6)
            raise AssertionError("mismatch wrote an outbox commit")
        except KeyError:
            pass
        consume(con, ir, shadow=False)
        if groups(con) != {"c": (1, "1.00")}:
            raise AssertionError(groups(con))
        con.close()

        con = open_db(path)
        if int(aggregate_state.state_info(con, "agg-state")["watermark"]) != 6:
            raise AssertionError("restart lost the watermark")
        aggregate_shadow.verify(
            con, ir, "agg-state", 6, "db.orders",
            aggregate_log_consumer.transform_commit)
        add_commit(con, [(4, "c", Decimal("1.00"), 1)], 220)
        consume(con, ir, shadow=True)
        if groups(con):
            raise AssertionError(groups(con))
        con.close()
    print(
        "aggregate_shadow rows=7 batches=7 mismatch_rolled_back=1 "
        "restart_verified=1 starrocks=not_run",
        flush=True)
    print("aggregate_shadow_test ok", flush=True)


if __name__ == "__main__":
    main()
