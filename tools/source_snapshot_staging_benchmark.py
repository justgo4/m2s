#!/usr/bin/env python3
"""Measure snapshot staging wall time, RSS and SQLite bytes at commit return."""
import argparse
import json
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import threading
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import source_state


def rss_bytes():
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])*1024
    except (OSError,ValueError,IndexError):
        pass
    value=int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform=="darwin" else value*1024


def storage_bytes(path):
    path=Path(path)
    result={}
    total=0
    for suffix in ("","-wal","-shm"):
        item=Path(str(path)+suffix)
        size=item.stat().st_size if item.exists() else 0
        result["db" if not suffix else suffix[1:]]=int(size)
        total+=size
    result["total"]=int(total)
    return result


def measure(fn):
    stop=threading.Event()
    samples=[rss_bytes()]
    def sample():
        while not stop.wait(.01):
            samples.append(rss_bytes())
    thread=threading.Thread(target=sample,daemon=True)
    thread.start()
    started=time.monotonic()
    try:
        result=fn()
    finally:
        elapsed=time.monotonic()-started
        samples.append(rss_bytes())
        stop.set()
        thread.join(2)
    return result,elapsed,max(samples)


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("value",pa.large_string()),
    ])


def snapshot_batch(start,stop):
    ids=list(range(int(start),int(stop)))
    table=pa.table({
        "id":pa.array(ids,type=pa.int64()),
        "value":pa.array(
            ["v-%012d" % value for value in ids],
            type=pa.large_string()),
    },schema=schema())
    return table.append_column(
        "_sync_op",
        pa.array([0]*len(ids),type=pa.int8())
    ).append_column(
        "_sync_order",
        pa.array(range(len(ids)),type=pa.int64())
    )


def run(rows,batch_rows):
    rows=int(rows)
    batch_rows=int(batch_rows)
    if rows<1 or batch_rows<1:
        raise ValueError("rows and batch_rows must be positive")

    with tempfile.TemporaryDirectory(
        prefix="m2s-source-snapshot-bench-"
    ) as directory:
        path=Path(directory)/"state.sqlite3"
        con=sqlite3.connect(
            path,timeout=30,isolation_level=None)
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=FULL")
        con.execute("PRAGMA foreign_keys=ON")
        source_state.install(con)
        source_state.register_relation(
            con,"db.events","benchmark-epoch",
            schema(),["id"])

        before_rss=rss_bytes()
        before_storage=storage_bytes(path)
        inserted=0

        def bootstrap():
            nonlocal inserted
            for start in range(0,rows,batch_rows):
                stop=min(rows,start+batch_rows)
                inserted+=source_state.stage_snapshot_batch(
                    con,"db.events",
                    snapshot_batch(start,stop),
                    cursor=(stop-1,),
                    is_last=stop==rows)
            return inserted

        _,wall_seconds,peak_rss=measure(bootstrap)
        after_storage=storage_bytes(path)
        current=int(con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE table_name='db.events'
              AND valid_to IS NULL
              AND deleted=0
        """).fetchone()[0])
        staged=int(con.execute("""
            SELECT COUNT(*)
            FROM source_snapshot_rows
        """).fetchone()[0])
        if inserted!=rows or current!=rows or staged:
            raise AssertionError(
                "snapshot staging mismatch inserted=%d current=%d staged=%d"
                % (inserted,current,staged))

        result=dict(
            format_version=1,
            kind="source_snapshot_staging_benchmark",
            rows=rows,
            batch_rows=batch_rows,
            batches=(rows+batch_rows-1)//batch_rows,
            sqlite_journal_mode=str(
                con.execute("PRAGMA journal_mode").fetchone()[0]),
            sqlite_synchronous=int(
                con.execute("PRAGMA synchronous").fetchone()[0]),
            wall_seconds=wall_seconds,
            wall_rows_per_second=(
                rows/wall_seconds if wall_seconds>0 else None),
            wall_scope="stage_snapshot_batch_includes_sqlite_commit_return",
            rss_bytes=dict(
                before=before_rss,
                peak=peak_rss,
                after=rss_bytes(),
            ),
            storage_bytes=dict(
                before=before_storage,
                after=after_storage,
            ),
            durable=dict(
                inserted_rows=inserted,
                current_rows=current,
                staged_rows=staged,
                snapshot_safe_seq=source_state.snapshot_safe_watermark(
                    con,["db.events"]),
            ),
        )
        con.close()
        return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows",type=int,default=100_000)
    parser.add_argument("--batch-rows",type=int,default=5_000)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    result=run(args.rows,args.batch_rows)
    payload=json.dumps(result,indent=2,sort_keys=True)+"\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(payload,encoding="utf-8")
    sys.stdout.write(payload)


if __name__=="__main__":
    main()
