#!/usr/bin/env python3
import os
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import j4


def reference_input(routed):
    row_index = "_sync_partition_order"
    return routed.append_column(
        row_index,pa.array(range(routed.num_rows),type=pa.int64()))


def main():
    os.environ["CDC_NATIVE_PARTITION"] = "required"
    rows = int(os.environ.get("J4_NATIVE_PARTITION_ROWS", "5000000"))
    partitions = int(os.environ.get("J4_NATIVE_PARTITIONS", "16"))
    lanes = pa.array(
        [((i * 1103515245 + 12345) >> 8) % partitions for i in range(rows)],
        type=pa.uint16(),
    )
    routed = pa.table({
        "_sync_lane": lanes,
        "value": pa.array(range(rows), type=pa.int64()),
    })

    indexed = reference_input(routed)

    started = time.perf_counter()
    ref = pc.sort_indices(
        indexed,
        sort_keys=[("_sync_lane","ascending"),("_sync_partition_order","ascending")])
    arrow_order_seconds = time.perf_counter() - started

    started = time.perf_counter()
    native = j4.native_stable_partition_order(routed, partitions)
    native_order_seconds = time.perf_counter() - started
    if native is None:
        raise AssertionError("native partition library was not loaded")
    order, counts = native

    if not pc.all(pc.equal(ref, order)).as_py():
        raise AssertionError(
            "native stable partition order differs from Arrow reference")
    if sum(count for _, count in counts) != rows:
        raise AssertionError("native partition counts do not cover all rows")

    started = time.perf_counter()
    arrow_grouped = indexed.take(ref).drop(["_sync_partition_order"])
    arrow_full_seconds = time.perf_counter() - started + arrow_order_seconds

    started = time.perf_counter()
    native_grouped = routed.take(order)
    native_take_seconds = time.perf_counter() - started
    native_full_seconds = native_order_seconds + native_take_seconds

    if not arrow_grouped.equals(native_grouped):
        raise AssertionError(
            "native full partition differs from Arrow sort+take reference")

    print(
        "NATIVE_PARTITION_BENCH "
        f"rows={rows} partitions={partitions} "
        f"arrow_order_seconds={arrow_order_seconds:.6f} "
        f"native_order_seconds={native_order_seconds:.6f} "
        f"order_speedup={arrow_order_seconds/native_order_seconds:.2f}x "
        f"arrow_full_seconds={arrow_full_seconds:.6f} "
        f"native_full_seconds={native_full_seconds:.6f} "
        f"full_speedup={arrow_full_seconds/native_full_seconds:.2f}x "
        f"native_take_seconds={native_take_seconds:.6f} "
        f"loader={j4._NATIVE_PARTITION_SOURCE}",
        flush=True,
    )


if __name__ == "__main__":
    main()
