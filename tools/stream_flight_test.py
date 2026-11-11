#!/usr/bin/env python3
"""Measure Flight scale-out and worker takeover on this machine."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_cluster
import stream_flight


def main():
    rows = [("k%04d" % index, (index % 5) + 1) for index in range(200)]
    expected = dict(stream_cluster.preaggregate(rows))
    cluster = stream_flight.FlightCluster(2)
    try:
        cluster.ingest(rows)
        before = cluster.totals()
        if before != expected:
            raise AssertionError("two workers diverged")
        scale_seconds = cluster.scale(4)
        if len(cluster.procs) != 4:
            raise AssertionError("scale did not reach 4 workers")
        if cluster.totals() != expected:
            raise AssertionError("scale changed the totals")
        failover_seconds = cluster.failover(1)
        if not cluster.procs[1].is_alive():
            raise AssertionError("replacement worker is not alive")
        if cluster.totals() != expected:
            raise AssertionError("failover changed the totals")
        print(
            "flight_rows=%d workers_before=2 workers_after=4 bytes=%d "
            "scale_seconds=%.6f failover_seconds=%.6f sum=%d"
            % (
                len(rows), cluster.nbytes, scale_seconds, failover_seconds,
                sum(expected.values()),
            ),
            flush=True)
    finally:
        cluster.stop()
    print("stream_flight_test ok", flush=True)


if __name__ == "__main__":
    main()
