#!/usr/bin/env python3
"""SQLite and RocksDB state backends agree. Rust consolidate matches Python."""
from pathlib import Path
import random
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import adaptive_batch
import state_backend
import stream_hot


def apply_script(backend, script):
    for op, key, value in script:
        if op == "put":
            backend.put(key, value)
        elif op == "delete":
            backend.delete(key)
        else:
            raise AssertionError(op)


def test_backends_match():
    rng = random.Random(20261111)
    script = []
    for index in range(200):
        key = ("k%03d" % rng.randrange(40)).encode()
        if rng.random() < 0.2 and script:
            script.append(("delete", key, b""))
        else:
            script.append(("put", key, ("v%d" % index).encode()))
    with tempfile.TemporaryDirectory(prefix="m2s-state-") as directory:
        root = Path(directory)
        sqlite = state_backend.open_state_backend("sqlite", root / "state.sqlite3")
        rocks = state_backend.open_state_backend("rocksdb", root / "rocks")
        try:
            apply_script(sqlite, script)
            apply_script(rocks, script)
            if sqlite.items() != rocks.items():
                raise AssertionError("sqlite and rocksdb diverged")
            sample = sqlite.items()[0][0]
            if sqlite.get(sample) != rocks.get(sample):
                raise AssertionError("get mismatch")
            if sqlite.get(b"missing") is not None or rocks.get(b"missing") is not None:
                raise AssertionError("missing key returned a value")
            stream_hot.put_ordered(sqlite, "b", "2")
            stream_hot.put_ordered(rocks, "b", "2")
            stream_hot.join_add(sqlite, "k", "r1")
            stream_hot.join_add(rocks, "k", "r1")
            stream_hot.join_add(sqlite, "k", "r2")
            stream_hot.join_add(rocks, "k", "r2")
            if stream_hot.range_ordered(sqlite, "b", "c") != stream_hot.range_ordered(rocks, "b", "c"):
                raise AssertionError("ordered range diverged")
            if stream_hot.join_get(sqlite, "k") != stream_hot.join_get(rocks, "k"):
                raise AssertionError("join index diverged")
            blob = stream_hot.dump_row({"id": 1, "name": "a"}, 3)
            if stream_hot.load_row(blob) != ({"id": 1, "name": "a"}, 3):
                raise AssertionError("row codec mismatch")
        finally:
            sqlite.close()
            rocks.close()


def test_consolidate_and_batch():
    pairs = [("b", 1), ("a", 2), ("a", -2), ("b", 3), ("c", 0)]
    python = stream_hot.consolidate(pairs)
    rust_or_python, which = stream_hot.consolidate_weights(pairs)
    if python != [("b", 4)]:
        raise AssertionError(python)
    if rust_or_python != python:
        raise AssertionError((which, rust_or_python, python))
    if adaptive_batch.choose_batch_rows(1) != 1:
        raise AssertionError("low rate did not keep a small batch")
    if adaptive_batch.choose_batch_rows(5000) != 256:
        raise AssertionError("high rate did not coalesce")
    mid = adaptive_batch.choose_batch_rows(505)
    if not 1 < mid < 256:
        raise AssertionError(mid)
    if not adaptive_batch.cdc_hot_path_unchanged():
        raise AssertionError("CDC hot path was replaced without a small-profile comparison")
    print("consolidate_impl", which, flush=True)


def main():
    test_backends_match()
    test_consolidate_and_batch()
    print("state_backend_test ok", flush=True)


if __name__ == "__main__":
    main()
