#!/usr/bin/env python3
"""Local multi-process shard, Flight exchange, reshard, and failover."""
from pathlib import Path
import sys

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_cluster


def test_shard_and_processes():
    rows = []
    for key in range(20):
        rows.append(("k%02d" % key, 1))
        rows.append(("k%02d" % key, 2))
    folded = stream_cluster.preaggregate(rows)
    if len(folded) != 20:
        raise AssertionError("pre-aggregation did not fold duplicate keys")
    if sum(weight for _key, weight in folded) != 60:
        raise AssertionError("pre-aggregation changed the total")
    direct, _blocked = stream_cluster.run_sharded(rows, 2, 64)
    folded_result, _blocked = stream_cluster.run_sharded(folded, 2, 64)
    if direct != folded_result:
        raise AssertionError("sharded totals diverged")
    if sum(direct.values()) != 60:
        raise AssertionError(direct)
    states = [{}, {}]
    for key, weight in folded:
        states[stream_cluster.shard_of(key, 2)][key] = weight
    moved = stream_cluster.reshard(states, 4)
    flat = {}
    for state in moved:
        flat.update(state)
    if flat != dict(folded):
        raise AssertionError("reshard dropped or duplicated a key")
    failed = stream_cluster.failover(states, 0)
    survived = {}
    for state in failed:
        for key, value in state.items():
            if key in survived:
                raise AssertionError("failover duplicated a key")
            survived[key] = value
    if survived != dict(folded):
        raise AssertionError("failover lost a key")


def test_flight_backpressure():
    table = pa.table({"k": ["a", "b"], "n": [1, 2]})
    restored, rejected = stream_cluster.exchange_table(table, limit_bytes=10_000_000)
    if restored.to_pydict() != table.to_pydict() or rejected != 0:
        raise AssertionError("flight exchange changed the batch")
    try:
        stream_cluster.exchange_table(table, limit_bytes=1)
        raise AssertionError("oversized batch was accepted")
    except Exception as exc:
        if "backpressure" not in str(exc):
            raise


def main():
    test_shard_and_processes()
    test_flight_backpressure()
    print("stream_cluster_test ok", flush=True)


if __name__ == "__main__":
    main()
