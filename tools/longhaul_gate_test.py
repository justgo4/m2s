#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

from tools.longhaul_gate import evaluate


def summary():
    return dict(
        event="run_summary",
        elapsed_seconds=72*3600+1,
        tables=dict(
            sink=dict(
                total=dict(
                    snapshot_read_rows=50_000_000,
                    snapshot_rows=50_000_000,
                    cdc_age_seconds=dict(
                        n=1000,total_n=1000,
                        p95=4.9,p99=9.9),
                    lag_over_10=3,
                )
            )
        ),
        state=dict(
            health="normal",
            errors=[],
            quarantined_tables={},
            pending_jobs=0,
            prepared_bytes=0,
            prepare_reserved_bytes=0,
            inflight=0,
            merge_uncertain_rows=0,
        ),
        stateful=dict(
            aggregate_shared_followers=9,
            join_shared_followers=4,
            sharing=dict(
                max_visible_lag=2),
            physical=dict(
                total=3,
                health=dict(ready=3),
            ),
            rebuilds=dict(
                active=0,phases={}),
        ),
    )


def main():
    good=evaluate(summary())
    assert good["ok"],good
    assert good["evidence"]["max_snapshot_rows"]==50_000_000
    assert good["evidence"]["cdc_samples"]==1000

    bad=summary()
    bad["tables"]["sink"]["total"]["cdc_age_seconds"]["p99"]=10.01
    result=evaluate(bad)
    assert not result["ok"]
    assert "sink:p99" in result["failures"]

    bad=summary()
    bad["elapsed_seconds"]=3600
    bad["state"]["pending_jobs"]=1
    bad["stateful"]["rebuilds"]["active"]=1
    result=evaluate(bad)
    assert not result["ok"]
    assert "elapsed_seconds" in result["failures"]
    assert "pending_jobs" in result["failures"]
    assert "rebuilds_active" in result["failures"]

    bad=summary()
    del bad["stateful"]
    result=evaluate(bad)
    assert not result["ok"]
    assert "stateful_summary_missing" in result["failures"]

    smoke=evaluate(
        summary(),
        min_elapsed_seconds=1,
        min_snapshot_rows=1,
        min_cdc_samples=1,
    )
    assert smoke["ok"]

    print(
        "longhaul_gate_test ok 50m_72h p95_p99 "
        "drain stateful_health fail_closed",
        flush=True,
    )


if __name__=="__main__":
    main()
