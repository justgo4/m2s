#!/usr/bin/env python3
"""Lock snapshot bootstrap to set-wise SQLite DML and CDC-safe anti-joins."""
from pathlib import Path
import sqlite3
import sys
import tempfile

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import source_state


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("value",pa.string()),
    ])


def batch(rows):
    table=pa.table({
        "id":pa.array([row[0] for row in rows],type=pa.int64()),
        "value":pa.array([row[1] for row in rows],type=pa.string()),
    },schema=schema())
    return table.append_column(
        "_sync_op",
        pa.array([0]*len(rows),type=pa.int8())
    ).append_column(
        "_sync_order",
        pa.array(range(len(rows)),type=pa.int64())
    )


def mutation_batch(rows):
    table=pa.table({
        "id":pa.array([row[1] for row in rows],type=pa.int64()),
        "value":pa.array([row[2] for row in rows],type=pa.string()),
    },schema=schema())
    return table.append_column(
        "_sync_op",
        pa.array([row[0] for row in rows],type=pa.int8())
    ).append_column(
        "_sync_order",
        pa.array(range(len(rows)),type=pa.int64())
    )


def open_db(path):
    con=sqlite3.connect(
        path,timeout=30,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    return con


def current_values(con):
    pin=source_state.acquire_pin(
        con,"snapshot-setwise-check",["db.events"])
    try:
        table,_=source_state.read_snapshot_batch(
            con,pin["pin_id"],"db.events",limit=100)
        return {
            int(row["id"]):str(row["value"])
            for row in table.select(
                ["id","value"]).to_pylist()
        }
    finally:
        source_state.release_pin(
            con,pin["pin_id"])


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-snapshot-setwise-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.events","epoch-1",
            schema(),["id"])

        # CDC lands while history is still scanning. The touched key must win
        # over the older snapshot value when that history page arrives later.
        part=source_state.prepare_part(
            "db.events",
            mutation_batch([(0,2,"cdc-new")]))
        assert source_state.log_commit(
            con,"epoch-1",
            ("binlog.000001",100),None,[part])==1
        assert source_state.apply_pending(con)==1

        trace=[]
        con.set_trace_callback(trace.append)
        inserted=source_state.stage_snapshot_batch(
            con,"db.events",
            batch([
                (1,"snap-a"),
                (2,"snap-stale"),
                (3,"snap-c"),
            ]),
            cursor=(3,),is_last=False)
        con.set_trace_callback(None)
        assert inserted==2

        normalized=[
            " ".join(item.split()).upper()
            for item in trace
        ]
        base_inserts=[
            item for item in normalized
            if item.startswith(
                "INSERT INTO SOURCE_VERSIONS(")
        ]
        row_probes=[
            item for item in normalized
            if item.startswith(
                "SELECT 1 FROM SOURCE_TOUCHED")
            or item.startswith(
                "SELECT 1 FROM SOURCE_VERSIONS")
        ]
        assert len(base_inserts)==1,base_inserts
        assert not row_probes,row_probes
        assert con.execute("""
            SELECT 1 FROM sqlite_temp_master
            WHERE type='table' AND name='source_snapshot_rows'
        """).fetchone() is not None
        assert con.execute(
            "SELECT COUNT(*) FROM temp.source_snapshot_rows"
        ).fetchone()[0]==0
        assert con.execute(
            "SELECT COUNT(*) FROM main.source_snapshot_rows"
        ).fetchone()[0]==0
        assert source_state.status(con)[
            "pipeline_stats"]["snapshot_staging_rows"]==0

        # Replaying the same source snapshot page is idempotent at the backing
        # relation: current/touched anti-joins suppress duplicate base rows.
        assert source_state.stage_snapshot_batch(
            con,"db.events",
            batch([
                (1,"snap-a"),
                (2,"snap-stale"),
                (3,"snap-c"),
            ]),
            cursor=(3,),is_last=False)==0
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_to IS NULL
        """).fetchone()[0]==3

        # An impossible duplicate source PK is corruption, not "last row wins".
        # The entire staging transaction must roll back and leave no residue.
        try:
            source_state.stage_snapshot_batch(
                con,"db.events",
                batch([
                    (4,"dup-a"),
                    (4,"dup-b"),
                ]),
                cursor=(4,),is_last=False)
            raise AssertionError(
                "duplicate snapshot primary key was accepted")
        except sqlite3.IntegrityError:
            pass
        assert con.execute(
            "SELECT COUNT(*) FROM temp.source_snapshot_rows"
        ).fetchone()[0]==0
        assert con.execute(
            "SELECT COUNT(*) FROM main.source_snapshot_rows"
        ).fetchone()[0]==0
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_to IS NULL
        """).fetchone()[0]==3

        assert source_state.stage_snapshot_batch(
            con,"db.events",batch([]),
            cursor=(3,),is_last=True)==0
        assert source_state.snapshot_safe_watermark(
            con,["db.events"])==1
        assert current_values(con)=={
            1:"snap-a",
            2:"cdc-new",
            3:"snap-c",
        }
        con.close()

    print(
        "source_snapshot_setwise_test ok "
        "constant_probe cdc_wins idempotent temp_staging rollback",
        flush=True,
    )


if __name__=="__main__":
    main()
