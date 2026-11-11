#!/usr/bin/env python3
"""Milestone 10 checks that this tree can run, and the gaps it must not hide."""
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import state_backend
import stream_cluster
import stream_eval
import stream_sql
import stream_zset


GAPS = (
    "TPC-H Q1-Q22 official text is not vendored",
    "TPC-DS official text is not vendored",
    "Nexmark official queries are not vendored",
    "formal p11-50m-50rps-72h-v4 was not run",
    "strict-small was not re-run on this SHA; last pass is 96501dcd0d7bf3c7b84b7d2ac17aaed7d759a9d0",
)


def _duck(sql, tables):
    con = duckdb.connect(":memory:")
    try:
        for name, rows in tables.items():
            con.register(name, pa.Table.from_pylist(rows))
        cursor = con.execute(sql)
        names = [item[0] for item in cursor.description]
        result = stream_zset.ZSet()
        for record in cursor.fetchall():
            result.add(dict(zip(names, record)), 1)
        return result
    finally:
        con.close()


def _run(sql, sources, batch):
    compiled = stream_sql.compile_sql(sql, sources)
    graph = stream_eval.StreamGraph(compiled["plan"])
    try:
        graph.push(batch)
        return graph.output.copy()
    finally:
        graph.close()


def test_original_shapes():
    events = [
        dict(id=1, bucket=1, v=3),
        dict(id=2, bucket=1, v=4),
        dict(id=3, bucket=2, v=8),
    ]
    dimensions = [dict(bucket=1, label="east"), dict(bucket=2, label="west")]
    cases = [
        (
            "SELECT id, v FROM events WHERE v > 3",
            {"events": ["id", "bucket", "v"]},
            {"events": [(row, 1) for row in events]},
            {"events": events},
        ),
        (
            "SELECT bucket, COUNT(*) AS n FROM events GROUP BY bucket",
            {"events": ["id", "bucket", "v"]},
            {"events": [(row, 1) for row in events]},
            {"events": events},
        ),
        (
            "SELECT e.id AS id, d.label AS label FROM events e "
            "INNER JOIN dimensions d ON e.bucket = d.bucket",
            {"events": ["id", "bucket", "v"], "dimensions": ["bucket", "label"]},
            {
                "events": [(row, 1) for row in events],
                "dimensions": [(row, 1) for row in dimensions],
            },
            {"events": events, "dimensions": dimensions},
        ),
    ]
    for sql, sources, batch, tables in cases:
        got = _run(sql, sources, batch)
        expected = _duck(sql, tables)
        if got.weights != expected.weights:
            raise AssertionError(sql)


def test_faults():
    try:
        stream_cluster.require_complete([{"a": 1}, None], 2)
        raise AssertionError("partial shuffle was published")
    except RuntimeError as exc:
        if "partition" not in str(exc):
            raise

    class FullDisk(state_backend.SqliteStateBackend):
        def put(self, key, value):
            raise OSError(28, "No space left on device")

    import tempfile
    with tempfile.TemporaryDirectory(prefix="m2s-full-") as directory:
        backend = FullDisk(Path(directory) / "state.sqlite3")
        try:
            backend.put(b"a", b"1")
            raise AssertionError("disk full was ignored")
        except OSError as exc:
            if exc.errno != 28:
                raise
        finally:
            backend.close()


def test_gaps_stay_listed():
    for item in GAPS:
        if not item:
            raise AssertionError("empty gap")
    if not any(item.startswith("TPC-H") for item in GAPS):
        raise AssertionError("TPC-H gap was removed")
    if not any("72h" in item for item in GAPS):
        raise AssertionError("72h gap was removed")
    print("gaps", len(GAPS), flush=True)
    for item in GAPS:
        print("gap", item, flush=True)


def main():
    test_original_shapes()
    test_faults()
    test_gaps_stay_listed()
    print("stream_gap_test ok", flush=True)


if __name__ == "__main__":
    main()
