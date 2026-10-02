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

    queued=stateful_admission.queue_wait(
        con,additions,plan_version=7,
        reason=rejected["reason"],retry_seconds=30)
    assert queued==["new-c","new-d"]
    waits=stateful_admission.waiting_tasks(
        con,plan_version=7)
    assert [item["task_id"] for item in waits]==[
        "new-c","new-d"]
    plans=stateful_admission.waiting_plans(con)
    assert len(plans)==1
    assert plans[0]["plan_version"]==7
    assert plans[0]["tasks"]==2
    assert not stateful_admission.waiting_plans(
        con,now=plans[0]["next_retry"]-1,due_only=True)
    assert stateful_admission.waiting_plans(
        con,now=plans[0]["next_retry"]+1,due_only=True
    )[0]["plan_version"]==7

    # One task in a plan may have a later backoff generation. Retrying the
    # whole published plan before every waiter is due would bypass that task's
    # backoff and can hot-loop target/catalog validation.
    first_due=stateful_admission.waiting_tasks(
        con,plan_version=7)[0]["next_retry"]
    stateful_admission.queue_wait(
        con,[item("new-d","starrocks.d")],
        plan_version=7,reason=rejected["reason"],
        retry_seconds=30,max_retry_seconds=300)
    staggered=stateful_admission.waiting_tasks(
        con,plan_version=7)
    last_due=max(item["next_retry"] for item in staggered)
    assert last_due>first_due
    assert not stateful_admission.waiting_plans(
        con,now=(first_due+last_due)/2,due_only=True)
    due=stateful_admission.waiting_plans(
        con,now=last_due+1,due_only=True)
    assert len(due)==1 and due[0]["tasks"]==2

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

    assert not stateful_admission.waiting_tasks(con)
    status=stateful_admission.status(con)
    assert status==dict(
        decisions=2,admitted=2,rejected=0,
        reserved_pending_tasks=2,
        reserved_state_bytes=200,
        waiting_tasks=0,waiting_plans=0,
        next_retry=None)
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
                decisions=0,admitted=0,rejected=0,
                reserved_pending_tasks=0,
                reserved_state_bytes=0,
                waiting_tasks=0,waiting_plans=0,
                next_retry=None)
        finally:
            ro.close()

    # Cross-connection reservations close the window between admission and
    # descriptor registration. The second request must see the first request's
    # durable headroom even though no task descriptor exists yet.
    with tempfile.TemporaryDirectory(
        prefix="m2s-admission-reserve-"
    ) as td:
        path=str(Path(td)/"state.sqlite3")
        first=sqlite3.connect(
            path,isolation_level=None,timeout=5)
        second=sqlite3.connect(
            path,isolation_level=None,timeout=5)
        for db in (first,second):
            db.execute("PRAGMA busy_timeout=5000")
            stateful_admission.install(db)
        cfg=dict(
            stateful_admission_max_state_bytes=100,
            stateful_admission_reserve_state_bytes=60,
        )
        one=stateful_admission.admit(
            first,[item("reserve-a","starrocks.a")],cfg)
        assert one["ok"],one
        two=stateful_admission.admit(
            second,[item("reserve-b","starrocks.b")],cfg)
        assert not two["ok"],two
        assert two["reasons"]==["max_state_bytes"]
        assert two["metrics"]["reserved_state_bytes"]==60
        assert two["metrics"]["projected_state_bytes"]==120
        stateful_admission.queue_wait(
            second,[item("reserve-b","starrocks.b")],
            plan_version=12,reason=two["reason"],
            retry_seconds=0)
        due=stateful_admission.waiting_plans(
            second,due_only=True)
        assert len(due)==1 and due[0]["plan_version"]==12
        assert stateful_admission.waiting_tasks(
            second,plan_version=12)[0]["retry_count"]==0
        stateful_admission.queue_wait(
            second,[item("reserve-b","starrocks.b")],
            plan_version=12,reason=two["reason"],
            retry_seconds=0)
        assert stateful_admission.waiting_tasks(
            second,plan_version=12)[0]["retry_count"]==1
        released=stateful_admission.release_unregistered(
            first,[item("reserve-a","starrocks.a")],
            reason="synthetic_failure")
        assert released==["reserve-a"]
        retry=stateful_admission.admit(
            second,[item("reserve-b","starrocks.b")],cfg)
        assert retry["ok"],retry
        assert not stateful_admission.waiting_tasks(
            second,plan_version=12)
        assert stateful_admission.decision_info(
            first,"reserve-a")["reserved_state_bytes"]==0
        first.close()
        second.close()

    print(
        "stateful_admission_test ok tasks building state_bytes "
        "pending_bytes source_lag durable_decision retry_idempotence "
        "atomic_reservation release durable_wait_queue due_retry whole_plan_due_fence",
        flush=True,
    )


if __name__=="__main__":
    main()
