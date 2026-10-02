#!/usr/bin/env python3
"""Lock source base apply to set-based SQLite DML and fail-closed corruption."""
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
    con.execute("PRAGMA foreign_keys=ON")
    source_state.install(con)
    return con


def current(con):
    return {
        int(row[0]):str(row[1])
        for row in con.execute("""
            SELECT
                CAST(json_extract(
                    CAST(row_payload AS TEXT),'$[0]')
                    AS INTEGER),
                CAST(json_extract(
                    CAST(row_payload AS TEXT),'$[1]')
                    AS TEXT)
            FROM source_versions
            WHERE valid_to IS NULL
              AND deleted=0
        """)
    }


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-source-setwise-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=open_db(path)
        source_state.register_relation(
            con,"db.events","epoch-1",
            schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.events",batch([]),
            cursor=None,is_last=True)

        rows=[
            (0,index,"v-%d" % index)
            for index in range(256)
        ]
        part=source_state.prepare_part(
            "db.events",batch(rows))
        assert source_state.log_commit(
            con,"epoch-1",
            ("binlog.000001",100),None,
            [part])==1

        trace=[]
        con.set_trace_callback(trace.append)
        assert source_state.apply_pending(con)==1
        con.set_trace_callback(None)

        normalized=[
            " ".join(item.split()).upper()
            for item in trace
        ]
        updates=[
            item for item in normalized
            if item.startswith(
                "UPDATE SOURCE_VERSIONS SET VALID_TO=")
        ]
        inserts=[
            item for item in normalized
            if item.startswith(
                "INSERT INTO SOURCE_VERSIONS(")
        ]
        touched=[
            item for item in normalized
            if item.startswith(
                "INSERT OR IGNORE INTO SOURCE_TOUCHED(")
        ]
        assert len(updates)==1,updates
        assert len(inserts)==1,inserts
        assert len(touched)==1,touched
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_from=1
              AND valid_to IS NULL
              AND deleted=0
        """).fetchone()[0]==256

        # The net-change stage still collapses repeated source-PK mutations
        # before the set-wise apply.
        part=source_state.prepare_part(
            "db.events",
            batch([
                (0,7,"first"),
                (1,8,"ignored"),
                (0,7,"last"),
                (0,8,"restored"),
            ]))
        assert source_state.log_commit(
            con,"epoch-1",
            ("binlog.000001",120),None,
            [part])==2
        assert source_state.apply_pending(con)==1
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_from=2
        """).fetchone()[0]==2

        # Durable metadata is part of the source-log contract. A row-count
        # mismatch must fail before the staged net changes can publish.
        part=source_state.prepare_part(
            "db.events",
            batch([(0,11,"metadata-check")]))
        assert source_state.log_commit(
            con,"epoch-1",
            ("binlog.000001",130),None,
            [part])==3
        con.execute("""
            UPDATE source_commit_parts
            SET nrows=nrows+1
            WHERE seq=3 AND part=0
        """)
        try:
            source_state.apply_pending(con)
            raise AssertionError(
                "source part row-count mismatch was accepted")
        except RuntimeError as exc:
            assert "row count differs" in str(exc)
        assert source_state.base_applied_seq(con)==2
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_apply_actions
            WHERE seq=3
        """).fetchone()[0]==0
        con.execute("""
            UPDATE source_commit_parts
            SET nrows=nrows-1
            WHERE seq=3 AND part=0
        """)
        assert source_state.apply_pending(con)==1
        assert source_state.base_applied_seq(con)==3

        # A corrupt backing state with two current versions must be detected
        # before any close/insert can partially publish the next commit.
        payload=con.execute("""
            SELECT row_payload
            FROM source_versions
            WHERE table_name='db.events'
              AND valid_to IS NULL
              AND deleted=0
              AND valid_from=1
            LIMIT 1
        """).fetchone()[0]
        pk=con.execute("""
            SELECT pk
            FROM source_versions
            WHERE table_name='db.events'
              AND valid_to IS NULL
              AND deleted=0
              AND valid_from=1
            LIMIT 1
        """).fetchone()[0]
        con.execute("""
            INSERT INTO source_versions(
                table_name,pk,valid_from,valid_to,
                deleted,row_payload,schema_epoch)
            VALUES('db.events',?,999,NULL,0,?,1)
        """,(pk,payload))
        # Identify the duplicated logical key so the next source mutation hits
        # exactly that corrupt current-version set.
        decoded=source_state.key_bytes([0])
        if bytes(pk)!=decoded:
            duplicate_id=None
            for candidate in range(256):
                if source_state.key_bytes(
                    [candidate])==bytes(pk):
                    duplicate_id=candidate
                    break
            assert duplicate_id is not None
        else:
            duplicate_id=0

        part=source_state.prepare_part(
            "db.events",
            batch([(0,duplicate_id,"after-corruption")]))
        assert source_state.log_commit(
            con,"epoch-1",
            ("binlog.000001",140),None,
            [part])==4
        try:
            source_state.apply_pending(con)
            raise AssertionError(
                "duplicate current source versions were accepted")
        except RuntimeError as exc:
            assert "multiple current source versions" in str(exc)
        assert source_state.base_applied_seq(con)==3
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_apply_actions
            WHERE seq=4
        """).fetchone()[0]==0
        assert con.execute("""
            SELECT base_applied
            FROM source_commits
            WHERE seq=4
        """).fetchone()[0]==0
        con.close()

    print(
        "source_apply_setwise_test ok constant_dml "
        "net_change fail_closed_corruption",
        flush=True,
    )


if __name__=="__main__":
    main()
