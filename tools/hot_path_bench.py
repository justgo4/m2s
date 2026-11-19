#!/usr/bin/env python3
"""Compare the CDC engines with RocksDB state and Rust consolidate.

The measured CDC path is DuckDB SQL plus the SQLite aggregate engine.
RocksDB and the Rust helper are side paths. This bench does not switch
them on. A faster raw put is not the aggregate contract.
"""
from pathlib import Path
import importlib.util
import shutil
import sqlite3
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb
import pyarrow as pa

import aggregate_state
import state_backend
import stream_hot


ROWS = 50000
KEYS = 5000


def _events():
    keys = [index % KEYS for index in range(ROWS)]
    values = [1 for _ in range(ROWS)]
    return keys, values


def _duckdb(keys, values):
    frame = pa.table({"k": keys, "v": values})
    con = duckdb.connect()
    started = time.perf_counter()
    con.register("t", frame)
    rows = con.execute(
        "SELECT k, sum(v), count(*) FROM t GROUP BY k ORDER BY k").fetchall()
    elapsed = time.perf_counter() - started
    con.close()
    return elapsed, rows


def _sqlite_sql(keys, values):
    con = sqlite3.connect(":memory:")
    started = time.perf_counter()
    con.execute("CREATE TABLE t(k INTEGER, v INTEGER)")
    con.executemany("INSERT INTO t VALUES (?, ?)", list(zip(keys, values)))
    rows = con.execute(
        "SELECT k, sum(v), count(*) FROM t GROUP BY k ORDER BY k").fetchall()
    elapsed = time.perf_counter() - started
    con.close()
    return elapsed, rows


def _aggregate(keys, values):
    con = sqlite3.connect(":memory:")
    aggregate_state.install(con)
    spec = aggregate_state.aggregate_spec(
        ["k"],
        [
            dict(function="sum", output="s", input="v"),
            dict(function="count", output="c", input="*"),
        ])
    aggregate_state.create_state(con, "bench", spec, 0)
    changes = [
        dict(k=key, v=value, _sync_op=0)
        for key, value in zip(keys, values)
    ]
    started = time.perf_counter()
    seq = 0
    step = 2000
    for offset in range(0, len(changes), step):
        seq += 1
        aggregate_state.apply_transaction(
            con, "bench", seq, changes[offset:offset + step])
    elapsed = time.perf_counter() - started
    stored = aggregate_state.read_rows(con, "bench")
    con.close()
    rows = sorted(
        (int(row["k"]), int(row["s"]), int(row["c"]))
        for row in stored)
    return elapsed, rows


def _rocks(keys, values):
    root = Path(tempfile.mkdtemp(prefix="m2s-rocks-"))
    backend = state_backend.open_state_backend("rocksdb", str(root / "db"))
    totals = {}
    started = time.perf_counter()
    try:
        for key, value in zip(keys, values):
            current = totals.get(key)
            if current is None:
                current = [0, 0]
                totals[key] = current
            current[0] += value
            current[1] += 1
            backend.put(
                str(key).encode("utf-8"),
                ("%d,%d" % (current[0], current[1])).encode("utf-8"))
    finally:
        backend.close()
    elapsed = time.perf_counter() - started
    rows = sorted(
        (key, totals[key][0], totals[key][1]) for key in totals)
    shutil.rmtree(root, ignore_errors=True)
    return elapsed, rows


def _load_rust():
    built = ROOT / "native" / "stream_hot" / "target" / "release" / "libstream_hot_rs.so"
    if not built.is_file():
        return None
    spec = importlib.util.spec_from_file_location("stream_hot_rs", built)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.consolidate


def _consolidate(keys, rust):
    pairs = [(str(key), 1) for key in keys]
    started = time.perf_counter()
    python_rows = stream_hot.consolidate(pairs)
    python_elapsed = time.perf_counter() - started
    if rust is None:
        return python_elapsed, None, python_rows, None
    started = time.perf_counter()
    rust_rows = list(rust(pairs))
    rust_elapsed = time.perf_counter() - started
    return python_elapsed, rust_elapsed, python_rows, rust_rows


def main():
    keys, values = _events()
    duck_seconds, duck_rows = _duckdb(keys, values)
    sqlite_seconds, sqlite_rows = _sqlite_sql(keys, values)
    aggregate_seconds, aggregate_rows = _aggregate(keys, values)
    rocks_seconds, rocks_rows = _rocks(keys, values)
    if duck_rows != sqlite_rows or duck_rows != aggregate_rows or duck_rows != rocks_rows:
        raise AssertionError("grouped sums diverged")
    rust = _load_rust()
    python_seconds, rust_seconds, python_rows, rust_rows = _consolidate(keys, rust)
    if rust_rows is not None and python_rows != rust_rows:
        raise AssertionError("consolidate diverged")
    total = sum(row[1] for row in duck_rows)
    if total != ROWS:
        raise AssertionError(total)
    rust_text = "na" if rust_seconds is None else "%.6f" % rust_seconds
    print(
        "hot_path rows=%d keys=%d groups=%d "
        "duckdb_sql=%.6f sqlite_sql=%.6f sqlite_aggregate=%.6f "
        "rocks_incremental=%.6f python_consolidate=%.6f rust_consolidate=%s "
        "switch=0 cdc_hot_path_unchanged=1"
        % (
            ROWS, KEYS, len(duck_rows),
            duck_seconds, sqlite_seconds, aggregate_seconds,
            rocks_seconds, python_seconds, rust_text,
        ),
        flush=True)
    print("hot_path_bench ok", flush=True)


if __name__ == "__main__":
    main()
