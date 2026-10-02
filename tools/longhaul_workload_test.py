#!/usr/bin/env python3
from pathlib import Path
import json
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))

import longhaul_workload


def main():
    assert longhaul_workload.percentile([],0.95) is None
    assert longhaul_workload.percentile(
        [5,1,3,2,4],0.50)==3
    assert longhaul_workload.percentile(
        [5,1,3,2,4],0.99)==4
    assert not longhaul_workload.source_ready(None)
    assert not longhaul_workload.source_ready(dict(source=[]))
    assert not longhaul_workload.source_ready(
        dict(source=[("db.events",None)]))
    assert longhaul_workload.source_ready(
        dict(source=[("db.events",7)]))

    calls=[]
    def fake_execute(_cfg,sql):
        calls.append(sql)
        values=[
            int(value)
            for value in (
                sql.split("IN (",1)[1]
                .split(")",1)[0]
                .split(",")
            )
        ]
        return [
            (value,)
            for value in values
            if value%2==0
        ],["id"]

    with patch.object(
        longhaul_workload,
        "execute",side_effect=fake_execute
    ):
        visible=longhaul_workload.visible_markers(
            {},range(1000,2025))
    assert visible=={
        value
        for value in range(1000,2025)
        if value%2==0
    }
    assert len(calls)==3
    assert all("MAX(" not in sql for sql in calls)

    commit_times={
        100:10.0,
        101:11.0,
    }
    clock=iter([
        12.0,12.1,12.2,12.3,
        12.4,12.5,12.6,12.7,
    ])
    with patch.object(
        longhaul_workload,"assert_live"
    ), patch.object(
        longhaul_workload,"visible_markers",
        side_effect=[{100},{101}]
    ), patch.object(
        longhaul_workload,"read_state",
        return_value=dict(
            log_durable_seq=7,
            base_applied_seq=7)
    ), patch.object(
        longhaul_workload.time,"monotonic",
        side_effect=lambda: next(clock)
    ), patch.object(
        longhaul_workload.time,"sleep"
    ):
        recovered=longhaul_workload.recover_after_fault(
            object(),Path("daemon.log"),
            Path("state.sqlite3"),{},
            commit_times,10)
    assert commit_times=={}
    assert len(recovered["latencies"])==2
    assert recovered["state"]["base_applied_seq"]==7

    with tempfile.TemporaryDirectory(
        prefix="m2s-longhaul-helper-"
    ) as td:
        directory=Path(td)
        state=directory/"state.sqlite3"
        summary=Path(
            str(state)+".summary.json")
        metrics=Path(
            str(state)+".metrics.jsonl")
        summary.write_text(
            json.dumps(dict(event="run_summary")),
            encoding="utf-8")
        metrics.write_text(
            '{"event":"metrics"}\n',
            encoding="utf-8")
        output=directory/"artifacts"/"workload.json"
        copied=longhaul_workload.copy_evidence(
            directory,output)
        assert set(copied)=={
            "daemon_summary","daemon_metrics"}
        assert (
            output.parent/"longhaul-daemon-summary.json"
        ).exists()
        assert (
            output.parent/"longhaul-daemon-metrics.jsonl"
        ).exists()

    print(
        "longhaul_workload_test ok percentile source_ready "
        "per_transaction_sentinel crash_catchup evidence_copy",
        flush=True,
    )


if __name__=="__main__":
    main()
