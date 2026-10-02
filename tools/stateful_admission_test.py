#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import stateful_admission


def item(task_id,sink):
    return dict(task=dict(
        task_id=task_id,
        sink_key=sink,
    ))


def main():
    con=sqlite3.connect(":memory:")
    con.executescript("""
        CREATE TABLE aggregate_task_descriptors(
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL);
        CREATE TABLE join_task_descriptors(
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL);
        CREATE TABLE stateful_state_sizes(
            kind TEXT NOT NULL,
            state_id TEXT NOT NULL,
            rows INTEGER NOT NULL,
            payload_bytes INTEGER NOT NULL,
            PRIMARY KEY(kind,state_id));
        CREATE TABLE jobs(
            id INTEGER PRIMARY KEY,
            logical_bytes INTEGER NOT NULL);
        CREATE TABLE retired_jobs(
            job_id INTEGER PRIMARY KEY);
        CREATE VIEW active_jobs AS
            SELECT j.* FROM jobs j
            LEFT JOIN retired_jobs r ON r.job_id=j.id
            WHERE r.job_id IS NULL;
        CREATE TABLE source_state_meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL);
    """)
    stateful_admission.install(con)

    con.execute(
        "INSERT INTO aggregate_task_descriptors VALUES('active-a','active')")
    con.execute(
        "INSERT INTO join_task_descriptors VALUES('building-b','candidate')")
    con.execute(
        "INSERT INTO stateful_state_sizes VALUES('aggregate','s1',5,500)")
    con.executemany(
        "INSERT INTO jobs VALUES(?,?)",
        [(1,300),(2,400),(3,900)])
    con.execute(
        "INSERT INTO retired_jobs VALUES(3)")
    con.executemany(
        "INSERT INTO source_state_meta VALUES(?,?)",
        [
            ("log_durable_seq","30"),
            ("base_applied_seq","20"),
        ])

    additions=[
        item("new-c","starrocks.c"),
        item("new-d","starrocks.d"),
    ]
    snap=stateful_admission.snapshot(
        con,additions)
    assert snap["current_tasks"]==2
    assert snap["current_building"]==1
    assert snap["requested_tasks"]==2
    assert snap["state_bytes"]==500
    assert snap["pending_bytes"]==700
    assert snap["source_lag"]==10

    rejected=stateful_admission.admit(
        con,additions,dict(
            stateful_admission_max_tasks=3,
            stateful_admission_max_building=2,
            stateful_admission_max_state_bytes=650,
            stateful_admission_reserve_state_bytes=100,
            stateful_admission_max_pending_bytes=600,
            stateful_admission_max_source_lag=5,
        ))
    assert not rejected["ok"]
    assert set(rejected["reasons"])=={
        "max_tasks",
        "max_building",
        "max_state_bytes",
        "max_pending_bytes",
        "max_source_lag",
    }
    assert rejected["metrics"]["projected_state_bytes"]==700
    first=stateful_admission.decision_info(
        con,"new-c")
    assert not first["admitted"]
    assert "max_tasks" in first["reason"]

    admitted=stateful_admission.admit(
        con,additions,dict(
            stateful_admission_max_tasks=10,
            stateful_admission_max_building=10,
            stateful_admission_max_state_bytes=1000,
            stateful_admission_reserve_state_bytes=100,
            stateful_admission_max_pending_bytes=1000,
            stateful_admission_max_source_lag=20,
        ))
    assert admitted["ok"],admitted
    assert admitted["metrics"]["projected_state_bytes"]==700
    assert stateful_admission.decision_info(
        con,"new-d")["admitted"]

    # Crash/retry of an already registered candidate is not charged twice.
    con.execute(
        "INSERT INTO aggregate_task_descriptors VALUES('new-c','candidate')")
    retry=stateful_admission.admit(
        con,[item("new-c","starrocks.c")],dict(
            stateful_admission_max_tasks=2,
            stateful_admission_max_building=2,
        ))
    assert retry["ok"],retry
    assert retry["metrics"]["requested_tasks"]==0

    status=stateful_admission.status(con)
    assert status==dict(
        decisions=2,admitted=2,rejected=0)
    con.close()

    with tempfile.TemporaryDirectory(
        prefix="m2s-admission-ro-"
    ) as td:
        path=str(Path(td)/"state.sqlite3")
        rw=sqlite3.connect(path)
        stateful_admission.install(rw)
        rw.commit()
        rw.close()
        ro=sqlite3.connect(
            "file:"+path+"?mode=ro",uri=True)
        try:
            assert stateful_admission.status(ro)==dict(
                decisions=0,admitted=0,rejected=0)
        finally:
            ro.close()

    print(
        "stateful_admission_test ok tasks building state_bytes "
        "pending_bytes source_lag durable_decision retry_idempotence",
        flush=True,
    )


if __name__=="__main__":
    main()
