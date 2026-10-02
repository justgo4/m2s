#!/usr/bin/env python3
"""Measure bounded source-log/apply staging including SQLite commit return time."""
import argparse
import json
import os
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


def measure_phase(fn):
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


def mutation_batch(start,stop,key_space):
    indexes=range(int(start),int(stop))
    ids=[index%int(key_space) for index in indexes]
    values=["v-%012d" % index for index in indexes]
    table=pa.table(
        {
            "id":pa.array(ids,type=pa.int64()),
            "value":pa.array(
                values,type=pa.large_string()),
        },
        schema=schema(),
    )
    return table.append_column(
        "_sync_op",
        pa.array(
            [0]*len(ids),type=pa.int8())
    ).append_column(
        "_sync_order",
        pa.array(
            range(int(start),int(stop)),
            type=pa.int64())
    )


def prepared_parts(rows,part_rows,key_space):
    for start in range(0,int(rows),int(part_rows)):
        stop=min(int(rows),start+int(part_rows))
        yield source_state.prepare_part(
            "db.events",
            mutation_batch(
                start,stop,key_space),
        )


def empty_batch():
    table=pa.table(
        {
            "id":pa.array([],type=pa.int64()),
            "value":pa.array(
                [],type=pa.large_string()),
        },
        schema=schema(),
    )
    return table.append_column(
        "_sync_op",pa.array([],type=pa.int8())
    ).append_column(
        "_sync_order",pa.array([],type=pa.int64())
    )


def run(rows,part_rows,key_space):
    rows=int(rows)
    part_rows=int(part_rows)
    key_space=int(key_space)
    if rows<1:
        raise ValueError("rows must be positive")
    if part_rows<1:
        raise ValueError("part_rows must be positive")
    if key_space<1 or key_space>rows:
        raise ValueError(
            "key_space must be between 1 and rows")

    with tempfile.TemporaryDirectory(
        prefix="m2s-source-apply-bench-"
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
        source_state.stage_snapshot_batch(
            con,"db.events",empty_batch(),
            cursor=None,is_last=True)

        before_rss=rss_bytes()
        before_storage=storage_bytes(path)
        seq,log_seconds,log_peak_rss=measure_phase(
            lambda:source_state.log_commit(
                con,"benchmark-epoch",
                ("binlog.000001",100),None,
                prepared_parts(
                    rows,part_rows,key_space)))
        if seq!=1:
            raise AssertionError(
                "unexpected source sequence")
        after_log_storage=storage_bytes(path)

        applied,apply_seconds,apply_peak_rss=measure_phase(
            lambda:source_state.apply_pending(con))
        if applied!=1:
            raise AssertionError(
                "source apply did not advance one commit")
        after_apply_storage=storage_bytes(path)

        status=source_state.status(con)
        pipeline=dict(status["pipeline_stats"])
        staged=int(pipeline["apply_staging_rows"])
        versions=int(con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_from=?
        """,(seq,)).fetchone()[0])
        current=int(con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_to IS NULL AND deleted=0
        """).fetchone()[0])
        if staged:
            raise AssertionError(
                "source apply staging leaked rows")
        if versions!=key_space or current!=key_space:
            raise AssertionError(
                "net-change action count mismatch "
                "versions=%d current=%d expected=%d"
                % (versions,current,key_space))
        if int(pipeline["apply_actions"])!=key_space:
            raise AssertionError(
                "pipeline apply action count mismatch")

        result=dict(
            format_version=1,
            kind="source_apply_staging_benchmark",
            rows=rows,
            part_rows=part_rows,
            parts=(rows+part_rows-1)//part_rows,
            key_space=key_space,
            sqlite_temp_store=source_state.temp_store_info(
                con),
            sqlite_journal_mode=str(
                con.execute(
                    "PRAGMA journal_mode").fetchone()[0]),
            sqlite_synchronous=int(
                con.execute(
                    "PRAGMA synchronous").fetchone()[0]),
            log_wall_seconds=log_seconds,
            apply_wall_seconds=apply_seconds,
            log_wall_rows_per_second=(
                rows/log_seconds
                if log_seconds>0 else None),
            apply_wall_rows_per_second=(
                rows/apply_seconds
                if apply_seconds>0 else None),
            wall_scope=(
                "outer_call_includes_sqlite_commit_return"
            ),
            rss_bytes=dict(
                before=before_rss,
                log_peak=log_peak_rss,
                apply_peak=apply_peak_rss,
                after=rss_bytes(),
            ),
            storage_bytes=dict(
                before=before_storage,
                after_log=after_log_storage,
                after_apply=after_apply_storage,
            ),
            durable=dict(
                log_durable_seq=int(
                    status["log_durable_seq"]),
                base_applied_seq=int(
                    status["base_applied_seq"]),
                staged_rows=staged,
                committed_versions=versions,
                current_rows=current,
            ),
            pipeline_stats=pipeline,
        )
        con.close()
        return result


def main():
    parser=argparse.ArgumentParser(
        description=__doc__)
    parser.add_argument(
        "--rows",type=int,default=100_000)
    parser.add_argument(
        "--part-rows",type=int,default=5_000)
    parser.add_argument(
        "--key-space",type=int,default=100_000)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    result=run(
        args.rows,args.part_rows,args.key_space)
    payload=json.dumps(
        result,indent=2,sort_keys=True)+"\n"
    if args.output is not None:
        args.output.parent.mkdir(
            parents=True,exist_ok=True)
        args.output.write_text(
            payload,encoding="utf-8")
    sys.stdout.write(payload)


if __name__=="__main__":
    main()
