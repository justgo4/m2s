#!/usr/bin/env python3
import sqlite3
from pathlib import Path
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import source_state


def main():
    con=sqlite3.connect(":memory:",isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    schema=pa.schema([
        pa.field("id",pa.int64()),
        pa.field("value",pa.string()),
    ])
    source_state.register_relation(
        con,"mysql.events","epoch-1",schema,["id"])
    run_start=source_state.status(
        con)["pipeline_stats"]

    batch=pa.table({
        "id":pa.array([1,2],type=pa.int64()),
        "value":pa.array(["a","b"],type=pa.string()),
        "_sync_op":pa.array([0,0],type=pa.int8()),
        "_sync_order":pa.array([0,1],type=pa.int64()),
    })
    part=source_state.prepare_part(
        "mysql.events",batch)

    seq=source_state.log_commit(
        con,"epoch-1",("binlog.000001",100),
        "gtid-1",[part])
    assert seq==1

    first=source_state.status(con)["pipeline_stats"]
    assert first["log_commits"]==1
    assert first["log_parts"]==1
    assert first["log_rows"]==2
    assert first["log_payload_bytes"]>0
    assert first["log_work_seconds"]>=0

    # Durable source-position retry returns the same seq and must not inflate
    # cost counters.
    retry=source_state.log_commit(
        con,"epoch-1",("binlog.000001",100),
        "gtid-1",[part])
    assert retry==seq
    assert (
        source_state.status(con)["pipeline_stats"]
        ==first
    )

    assert source_state.apply_pending(con)==1
    applied=source_state.status(con)["pipeline_stats"]
    assert applied["log_commits"]==1
    assert applied["apply_commits"]==1
    assert applied["apply_input_rows"]==2
    assert applied["apply_actions"]==2
    assert applied["apply_work_seconds"]>=0

    run=j4.source_pipeline_stats_delta(
        run_start,applied)
    assert not run["counter_regressions"]
    assert run["pipeline_stats"]["log_commits"]==1
    assert run["pipeline_stats"]["log_rows"]==2
    assert run["pipeline_stats"]["apply_commits"]==1
    assert run["pipeline_stats"]["apply_input_rows"]==2
    assert (
        run["pipeline_stats"]["log_rows_per_second"]
        is not None
    )
    assert (
        run["pipeline_stats"]["apply_rows_per_second"]
        is not None
    )

    regressed=dict(applied)
    regressed["log_commits"]=0
    bad=j4.source_pipeline_stats_delta(
        applied,regressed)
    assert bad["counter_regressions"]
    assert bad["pipeline_stats"]["log_commits"]==0

    # Re-running apply at the same durable base watermark is a no-op and must
    # not count a second apply.
    assert source_state.apply_pending(con)==0
    assert (
        source_state.status(con)["pipeline_stats"]
        ==applied
    )

    # Offline status of a pre-metric state remains readable. This models the
    # status command before the upgraded daemon has executed install().
    legacy=sqlite3.connect(":memory:",isolation_level=None)
    legacy.executescript("""
        CREATE TABLE source_state_meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL);
        CREATE TABLE source_relations(
            table_name TEXT PRIMARY KEY,
            source_epoch TEXT NOT NULL,
            schema_epoch INTEGER NOT NULL,
            schema_hash TEXT NOT NULL,
            schema_bytes BLOB NOT NULL,
            columns_json TEXT NOT NULL,
            pk_json TEXT NOT NULL,
            complete_seq INTEGER,
            snapshot_cursor BLOB,
            snapshot_upper BLOB,
            snapshot_upper_set INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE source_pins(
            pin_id TEXT PRIMARY KEY,
            watermark INTEGER NOT NULL,
            owner TEXT NOT NULL,
            created REAL NOT NULL);
        CREATE TABLE source_consumers(
            consumer_id TEXT PRIMARY KEY,
            watermark INTEGER NOT NULL,
            owner TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE source_log_stats(
            table_name TEXT PRIMARY KEY,
            commits INTEGER NOT NULL,
            event_rows INTEGER NOT NULL,
            payload_bytes INTEGER NOT NULL);
    """)
    for key,value in (
        ("log_durable_seq","0"),
        ("base_applied_seq","0"),
        ("min_readable_seq","0"),
    ):
        legacy.execute(
            "INSERT INTO source_state_meta(key,value) VALUES(?,?)",
            (key,value))
    old=source_state.status(legacy)["pipeline_stats"]
    assert old["log_commits"]==0
    assert old["apply_commits"]==0
    legacy.close()

    con.close()
    print(
        "source_pipeline_metrics_test ok "
        "durable_log apply idempotent_retry run_delta "
        "counter_regression offline_upgrade_status",
        flush=True,
    )


if __name__=="__main__":
    main()
