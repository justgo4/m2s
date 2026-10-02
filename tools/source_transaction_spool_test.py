#!/usr/bin/env python3
from pathlib import Path
import os
import sys
import tempfile
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import source_state


def batch(ids,values):
    return pa.table({
        "id":pa.array(ids,type=pa.int64()),
        "value":pa.array(values,type=pa.string()),
        "_sync_op":pa.array([0]*len(ids),type=pa.int8()),
        "_sync_order":pa.array(
            list(range(len(ids))),type=pa.int64()),
    })


def write_parts(spool):
    for ids,values in (
        ([1],["a"*2048]),
        ([2],["b"*2048]),
    ):
        part=source_state.prepare_part(
            "mysql.events",batch(ids,values))
        j4.write_source_part_record(spool,part)


def cfg(path,limit=16*1024*1024):
    return dict(
        state=path,
        txn_spool_max_bytes=int(limit),
        batch_bytes=1024*1024,
        min_free_bytes=0,
    )


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-source-spool-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        source_state.register_relation(
            con,"mysql.events","source-epoch",
            pa.schema([
                pa.field("id",pa.int64()),
                pa.field("value",pa.string()),
            ]),
            ["id"])
        with j4.state_transaction(con):
            j4.meta_set(
                con,"read_position",
                ("binlog.000001",4))

        # A corrupt disk-backed source-part spool must roll back the entire
        # SQLite source commit and leave the durable binlog cursor unchanged.
        with tempfile.SpooledTemporaryFile(
            max_size=128,dir=directory
        ) as source_spool, tempfile.SpooledTemporaryFile(
            max_size=128,dir=directory
        ) as target_spool:
            write_parts(source_spool)
            assert source_spool._rolled
            size=source_spool.tell()
            source_spool.truncate(size-1)
            source_spool.seek(0,os.SEEK_END)
            try:
                j4.commit_spool(
                    con,target_spool,
                    ("binlog.000001",120),
                    time.time(),{},
                    source_spool=source_spool,
                    source_epoch="source-epoch")
                raise AssertionError(
                    "truncated source spool advanced durable state")
            except RuntimeError as exc:
                assert "truncated source transaction spool" in str(exc)
            assert j4.meta_get(
                con,"read_position"
            )==("binlog.000001",4)
            assert con.execute(
                "SELECT COUNT(*) FROM source_commits"
            ).fetchone()[0]==0
            assert source_state.log_durable_seq(con)==0

        # The source spool has its own bounded disk budget. Exceeding it is
        # fail-closed before commit, but no longer tied to a RAM-sized list.
        with tempfile.SpooledTemporaryFile(
            max_size=128,dir=directory
        ) as source_spool:
            write_parts(source_spool)
            size=source_spool.tell()
            try:
                j4.source_part_spool_guard(
                    source_spool,cfg(path,size-1))
                raise AssertionError(
                    "source spool ignored configured transaction limit")
            except RuntimeError as exc:
                assert "CDC_TXN_SPOOL_MAX_BYTES" in str(exc)
            assert j4.meta_get(
                con,"read_position"
            )==("binlog.000001",4)

        # A valid disk-backed spool is copied into source_commit_parts inside
        # the same SQLite transaction that advances the durable cursor.
        with tempfile.SpooledTemporaryFile(
            max_size=128,dir=directory
        ) as source_spool, tempfile.SpooledTemporaryFile(
            max_size=128,dir=directory
        ) as target_spool:
            write_parts(source_spool)
            assert source_spool._rolled
            j4.source_part_spool_guard(
                source_spool,cfg(path),
                force=True,target_spool_bytes=0)
            changed=j4.commit_spool(
                con,target_spool,
                ("binlog.000001",120),
                time.time(),{},
                source_spool=source_spool,
                source_epoch="source-epoch")
            assert changed==set()

        assert j4.meta_get(
            con,"read_position"
        )==("binlog.000001",120)
        assert source_state.log_durable_seq(con)==1
        assert con.execute(
            "SELECT COUNT(*) FROM source_commit_parts"
        ).fetchone()[0]==2
        assert con.execute(
            "SELECT SUM(nrows) FROM source_commit_parts"
        ).fetchone()[0]==2

        assert source_state.apply_pending(con)==1
        assert source_state.base_applied_seq(con)==1
        assert con.execute("""
            SELECT COUNT(*) FROM source_versions
            WHERE table_name='mysql.events'
              AND valid_to IS NULL
              AND deleted=0
        """).fetchone()[0]==2
        con.close()

    print(
        "source_transaction_spool_test ok "
        "disk_backed bounded atomic_cursor rollback_on_truncate apply",
        flush=True,
    )


if __name__=="__main__":
    main()
