#!/usr/bin/env python3
"""Exact CDC SLO threshold accounting contract."""
from pathlib import Path
import sys
import threading

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import j4


def runtime():
    return dict(
        metrics=dict(
            lock=threading.Lock(),
            tables=dict(
                target=dict(
                    total=j4.metric_bucket(),
                    interval=j4.metric_bucket(),
                )
            ),
        )
    )


def main():
    state=runtime()
    for age in (4.0,6.0,11.0):
        j4.metric_add_visible(
            state,"target","cdc",
            input_rows=1,byte_count=10,loads=1,merge_txns=1,
            seconds=0.1,age=age,
        )

    # Snapshot/mixed output must not pollute the CDC latency denominator.
    j4.metric_add_visible(
        state,"target","snapshot",
        input_rows=100,byte_count=1000,loads=1,merge_txns=1,
        seconds=0.2,age=99.0,
    )
    j4.metric_add_visible(
        state,"target","cdc,snapshot",
        input_rows=2,byte_count=20,loads=1,merge_txns=1,
        seconds=0.2,age=99.0,
    )

    total=j4.metric_bucket_summary(
        state["metrics"]["tables"]["target"]["total"])
    interval=j4.metric_bucket_summary(
        state["metrics"]["tables"]["target"]["interval"])

    for summary in (total,interval):
        assert summary["cdc_deliveries"]==3,summary
        assert summary["lag_over_5"]==2,summary
        assert summary["lag_over_10"]==1,summary
        exact=summary["exact_cdc_age_seconds"]
        assert exact["n"]==3,exact
        assert abs(exact["avg"]-7.0)<1e-9,exact
        assert exact["max"]==11.0,exact

    print(
        "metric_slo_test ok exact cdc >5s/>10s violation counters "
        "exclude snapshot and mixed deliveries",
        flush=True,
    )


if __name__=="__main__":
    main()
