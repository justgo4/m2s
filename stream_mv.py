#!/usr/bin/env python3
"""Chain one stream plan into another.

The downstream watermark cannot move ahead of the upstream watermark, and it
cannot go backwards. That is the fixed-frontier rule for a view built on a
view. Durable SQLite generations still own the v1 tasks; this object is the
reference scheduler for the stream DAG.
"""
import stream_eval


class MaterializedView:
    def __init__(self, upstream, downstream, input_name):
        if not isinstance(upstream, stream_eval.StreamGraph):
            raise TypeError("upstream must be a stream graph")
        if not isinstance(downstream, stream_eval.StreamGraph):
            raise TypeError("downstream must be a stream graph")
        name = str(input_name or "").strip()
        if name not in downstream.plan["inputs"]:
            raise ValueError("downstream plan has no input " + name)
        self.upstream = upstream
        self.downstream = downstream
        self.input_name = name
        self.watermark = 0

    def push(self, deltas, watermark):
        if isinstance(watermark, bool):
            raise ValueError("materialized view watermark must be >= 0")
        watermark = int(watermark)
        if watermark < 0:
            raise ValueError("materialized view watermark must be >= 0")
        if watermark != self.watermark + 1:
            raise RuntimeError(
                "materialized view watermark must advance by one; "
                "downstream cannot pass the upstream frontier")
        produced = self.upstream.push(deltas)
        self.downstream.push({self.input_name: produced})
        self.watermark = watermark
        return self.downstream.output

    def close(self):
        self.upstream.close()
        self.downstream.close()
