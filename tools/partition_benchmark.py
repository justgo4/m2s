#!/usr/bin/env python3
"""Synthetic algorithm A/B; this is not an end-to-end CDC benchmark."""
import argparse
import gc
import json
import os
from pathlib import Path
import platform
import random
import resource
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pyarrow as pa
import pyarrow.compute as pc
import j4


def measure(routed, indexed, partitions, method):
    gc.collect()
    wall = time.perf_counter()
    cpu = time.process_time()
    if method == "arrow_sort":
        order = pc.sort_indices(indexed, sort_keys=[
            ("_sync_lane", "ascending"), ("_row_index", "ascending")])
        order_seconds = time.perf_counter() - wall
        output = indexed.take(order).drop(["_row_index"])
    else:
        order, counts = j4.native_stable_partition_order(routed, partitions)
        order_seconds = time.perf_counter() - wall
        output = routed.take(order)
        if sum(count for _, count in counts) != len(routed):
            raise AssertionError("partition counts lost rows")
    sample = dict(
        order_seconds=order_seconds,
        full_seconds=time.perf_counter() - wall,
        cpu_seconds=time.process_time() - cpu)
    return sample, output


def summary(samples, rows):
    result = {}
    for key in ("order_seconds", "full_seconds", "cpu_seconds"):
        values = [item[key] for item in samples]
        result[key] = dict(median=statistics.median(values), min=min(values), max=max(values))
    result["rows_per_second"] = rows / result["full_seconds"]["median"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--commit")
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/partition.json"))
    args = parser.parse_args()
    if not 1 <= args.rows <= 10_000_000 or not 1 <= args.partitions <= 65536 or not 3 <= args.repeats <= 100:
        parser.error("rows=1..10000000, partitions=1..65536, repeats=3..100 required")
    os.environ["CDC_NATIVE_PARTITION"] = "required"
    if j4.native_partition_library() is None:
        raise RuntimeError("native partition unavailable")
    routed = pa.table({
        "_sync_lane": pa.array([
            ((i * 1103515245 + args.seed) >> 8) % args.partitions
            for i in range(args.rows)], type=pa.uint16()),
        "value": pa.array(range(args.rows), type=pa.int64()),
    })
    indexed = routed.append_column("_row_index", pa.array(range(args.rows), type=pa.int64()))
    samples = {"arrow_sort": [], "native_count_scatter": []}
    reference = None
    for method in samples:
        _, result = measure(routed, indexed, args.partitions, method)
        if reference is None:
            reference = result
        elif not result.equals(reference):
            raise AssertionError("warmup result mismatch")
    rng = random.Random(args.seed)
    for repeat in range(args.repeats):
        methods = list(samples)
        rng.shuffle(methods)
        for method in methods:
            sample, result = measure(routed, indexed, args.partitions, method)
            if not result.equals(reference):
                raise AssertionError("measured result mismatch")
            samples[method].append(sample)
    stats = {method: summary(items, args.rows) for method, items in samples.items()}
    report = dict(
        format_version=1, kind="partition_algorithm_microbenchmark",
        rows=args.rows, partitions=args.partitions, repeats=args.repeats, seed=args.seed,
        input_bytes=routed.nbytes, correctness="equal_all_runs", commit=args.commit,
        environment=dict(python=platform.python_version(), arrow=pa.__version__,
                         duckdb=j4.duckdb.__version__, architecture=platform.machine(),
                         cpu_count=os.cpu_count()),
        peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        peak_rss_scope="whole_process_including_input_and_reference",
        summary=stats, samples=samples,
        full_speedup=stats["arrow_sort"]["full_seconds"]["median"] /
                     stats["native_count_scatter"]["full_seconds"]["median"],
        excluded=["input_creation", "warmup", "correctness_comparison"],
        included=["partition_order", "arrow_take", "reference_row_index_drop"],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PARTITION BENCH PASS rows={args.rows} repeats={args.repeats} "
          f"full_speedup={report['full_speedup']:.3f} output={args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
