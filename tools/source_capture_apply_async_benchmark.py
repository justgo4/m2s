#!/usr/bin/env python3
"""Measure overlapped durable source capture and asynchronous base apply."""
import argparse
import json
from pathlib import Path
import resource
import sys
import tempfile
import threading
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import source_state


def rss_bytes():
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])*1024
    except (OSError,ValueError,IndexError):
        pass
    value=int(resource.getrusage(
        resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform=="darwin" else value*1024


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("value",pa.large_string()),
    ])


def batch(start,count,key_space):
    indexes=list(range(
        int(start),int(start)+int(count)))
    ids=[
        value%int(key_space)
        for value in indexes
    ]
    table=pa.table({
        "id":pa.array(ids,type=pa.int64()),
        "value":pa.array(
            ["v-%012d" % value for value in indexes],
            type=pa.large_string()),
    },schema=schema())
    return table.append_column(
        "_sync_op",
        pa.array([0]*len(ids),type=pa.int8())
    ).append_column(
        "_sync_order",
        pa.array(
            range(int(start),int(start)+len(ids)),
            type=pa.int64())
    )


def parts(table_name,start,rows,part_rows,key_space):
    for offset in range(0,int(rows),int(part_rows)):
        count=min(
            int(part_rows),int(rows)-offset)
        yield source_state.prepare_part(
            table_name,
            batch(
                int(start)+offset,count,key_space))


def wait_applied(con,seq,timeout):
    deadline=time.monotonic()+float(timeout)
    while time.monotonic()<deadline:
        if source_state.base_applied_seq(con)>=int(seq):
            return
        time.sleep(.005)
    raise RuntimeError(
        "async source apply did not drain "
        "durable=%d applied=%d pending_bytes=%d"
        % (
            source_state.log_durable_seq(con),
            source_state.base_applied_seq(con),
            source_state.apply_pending_bytes(con),
        ))


def run(
        commits,rows_per_commit,part_rows,key_space,
        max_pending_bytes
):
    commits=int(commits)
    rows_per_commit=int(rows_per_commit)
    part_rows=int(part_rows)
    key_space=int(key_space)
    max_pending_bytes=int(max_pending_bytes)
    if min(
        commits,rows_per_commit,part_rows,
        key_space,max_pending_bytes
    )<1:
        raise ValueError(
            "all benchmark limits must be positive")

    with tempfile.TemporaryDirectory(
        prefix="m2s-source-async-bench-"
    ) as directory:
        path=Path(directory)/"state.sqlite3"
        capture=j4.init_state(str(path))
        source_state.register_relation(
            capture,"db.events","async-benchmark",
            schema(),["id"])
        empty=batch(0,0,key_space)
        source_state.stage_snapshot_batch(
            capture,"db.events",empty,
            cursor=None,is_last=True)

        stop=threading.Event()
        wake=threading.Event()
        runtime=dict(
            stop=stop,
            source_apply_event=wake,
        )
        worker=threading.Thread(
            target=j4.source_state_apply_worker,
            args=(dict(state=str(path)),runtime),
            name="source-async-benchmark")
        worker.start()

        samples=[]
        sample_stop=threading.Event()
        def sample():
            while not sample_stop.wait(.005):
                samples.append(rss_bytes())
        sampler=threading.Thread(
            target=sample,daemon=True)
        sampler.start()

        peak_pending=0
        backpressure_events=0
        total_rows=commits*rows_per_commit
        capture_started=time.monotonic()
        try:
            for commit_index in range(commits):
                start=commit_index*rows_per_commit
                seq=source_state.log_commit(
                    capture,"async-benchmark",
                    (
                        "binlog.000001",
                        100+commit_index*20,
                    ),
                    None,
                    parts(
                        "db.events",start,
                        rows_per_commit,part_rows,
                        key_space))
                if seq!=commit_index+1:
                    raise AssertionError(
                        "unexpected durable source sequence")
                wake.set()
                pending=source_state.apply_pending_bytes(
                    capture)
                peak_pending=max(
                    peak_pending,int(pending))
                if pending>=max_pending_bytes:
                    backpressure_events+=1
                while (
                    pending>=max_pending_bytes
                    and not stop.is_set()
                ):
                    wake.set()
                    time.sleep(.002)
                    pending=source_state.apply_pending_bytes(
                        capture)
            capture_seconds=(
                time.monotonic()-capture_started)
            drain_started=time.monotonic()
            wait_applied(
                capture,commits,
                max(30,commits*2))
            drain_tail_seconds=(
                time.monotonic()-drain_started)
        finally:
            sample_stop.set()
            sampler.join(2)
            stop.set()
            wake.set()
            worker.join(10)
        if worker.is_alive():
            raise RuntimeError(
                "async source apply worker did not stop")

        status=source_state.status(capture)
        current=int(capture.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE table_name='db.events'
              AND valid_to IS NULL
              AND deleted=0
        """).fetchone()[0])
        expected_current=min(
            key_space,total_rows)
        if current!=expected_current:
            raise AssertionError(
                "unexpected current source rows "
                "%d != %d"
                % (current,expected_current))
        if source_state.apply_pending_bytes(capture):
            raise AssertionError(
                "async source apply backlog did not drain")
        if int(status["pipeline_stats"][
            "apply_staging_rows"
        ]):
            raise AssertionError(
                "source apply staging residue remains")

        total_seconds=(
            capture_seconds+drain_tail_seconds)
        result=dict(
            format_version=1,
            kind="source_capture_apply_async_benchmark",
            commits=commits,
            rows_per_commit=rows_per_commit,
            total_rows=total_rows,
            part_rows=part_rows,
            key_space=key_space,
            max_pending_bytes=max_pending_bytes,
            capture_wall_seconds=capture_seconds,
            drain_tail_seconds=drain_tail_seconds,
            total_wall_seconds=total_seconds,
            capture_rows_per_second=(
                total_rows/capture_seconds
                if capture_seconds else None),
            end_to_end_rows_per_second=(
                total_rows/total_seconds
                if total_seconds else None),
            peak_pending_bytes=peak_pending,
            backpressure_events=backpressure_events,
            rss_bytes=dict(
                peak=max(samples or [rss_bytes()]),
                after=rss_bytes(),
            ),
            durable=dict(
                log_durable_seq=int(
                    status["log_durable_seq"]),
                base_applied_seq=int(
                    status["base_applied_seq"]),
                pending_bytes=int(
                    status["apply_pending_bytes"]),
                current_rows=current,
            ),
            pipeline_stats=dict(
                status["pipeline_stats"]),
        )
        capture.close()
        return result


def main():
    parser=argparse.ArgumentParser(
        description=__doc__)
    parser.add_argument(
        "--commits",type=int,default=100)
    parser.add_argument(
        "--rows-per-commit",type=int,default=1000)
    parser.add_argument(
        "--part-rows",type=int,default=250)
    parser.add_argument(
        "--key-space",type=int,default=50000)
    parser.add_argument(
        "--max-pending-bytes",type=int,
        default=32*1024**2)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    result=run(
        args.commits,args.rows_per_commit,
        args.part_rows,args.key_space,
        args.max_pending_bytes)
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
