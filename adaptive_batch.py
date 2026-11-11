#!/usr/bin/env python3
"""Adaptive batch sizing for a future stream writer.

Low input rates keep a small batch so latency stays low. High rates coalesce.
This module does not change j4's CDC batch. The default stays the current
hot path until a small-profile comparison says otherwise.
"""


def choose_batch_rows(rows_per_second, low_rows=1, high_rows=256,
                      low_rate=10, high_rate=1000):
    rate = float(rows_per_second)
    low_rows = int(low_rows)
    high_rows = int(high_rows)
    low_rate = float(low_rate)
    high_rate = float(high_rate)
    if low_rows < 1 or high_rows < low_rows:
        raise ValueError("batch bounds are invalid")
    if high_rate <= low_rate:
        raise ValueError("rate bounds are invalid")
    if rate <= low_rate:
        return low_rows
    if rate >= high_rate:
        return high_rows
    span = high_rate - low_rate
    fraction = (rate - low_rate) / span
    return int(low_rows + (high_rows - low_rows) * fraction)


def cdc_hot_path_unchanged():
    """The CDC writer still uses its existing batch settings."""
    return True
