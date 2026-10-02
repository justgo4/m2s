#!/usr/bin/env python3
"""Measure durable capture/apply latency with and without bounded source GC."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import threading
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import source_state

from source_capture_apply_async_benchmark import (
    batch,
    parts,
    percentile,
    rss_bytes,
    schema,
    wait_applied,
)


def latency_summary(values):
    values=[float(value) for value in values]
    return dict(
        samples=len(values),
        p50=percentile(values,.50),
        p95=percentile(values,.95),
        p99=percentile(values,.99),
        maximum=max(values or [0.0]),
    )


def ratio(numerator,denominator):
    if numerator is None or denominator in (None,0):
        return None
    return float(numerator)/float(denominator)


def run_case(
        commits,rows_per_commit,part_rows,key_space,
        gc_enabled,gc_version_rows,gc_commit_rows,gc_sleep_ms
):
    with tempfile.TemporaryDirectory(
        prefix="m2s-source-gc-overlap-"
    ) as directory:
        path=Path(directory)/"state.sqlite3"
        capture=j4.init_state(str(path))
        source_state.register_relation(
            capture,"db.events","gc-overlap",
            schema(),["id"])
        source_state.stage_snapshot_batch(
            capture,"db.events",batch(0,0,key_space),
            cursor=None,is_last=True)

        stop=threading.Event()
        wake=threading.Event()
        errors=[]
        apply_runtime=dict(
            stop=stop,
            source_apply_event=wake,
        )

        def apply_loop():
            try:
                j4.source_state_apply_worker(
                    dict(state=str(path)),apply_runtime)
            except BaseException as exc:
                errors.append(
                    ("apply",type(exc).__name__,str(exc)))
                stop.set()
                wake.set()

        apply_thread=threading.Thread(
            target=apply_loop,
            name="source-gc-overlap-apply")
        apply_thread.start()

        gc_latencies=[]
        gc_versions=[0]
        gc_commits=[0]
        gc_calls=[0]
        gc_thread=None

        if gc_enabled:
            def gc_loop():
                con=j4.open_state(str(path))
                try:
                    while not stop.is_set():
                        started=time.monotonic()
                        result=source_state.gc(
                            con,
                            version_limit=gc_version_rows,
                            commit_limit=gc_commit_rows,
                        )
                        gc_latencies.append(
                            time.monotonic()-started)
                        gc_calls[0]+=1
                        gc_versions[0]+=int(
                            result["versions"])
                        gc_commits[0]+=int(
                            result["commits"])
                        stop.wait(
                            float(gc_sleep_ms)/1000.0)
                except BaseException as exc:
                    errors.append(
                        ("gc",type(exc).__name__,str(exc)))
                    stop.set()
                    wake.set()
                finally:
                    con.close()

            gc_thread=threading.Thread(
                target=gc_loop,
                name="source-gc-overlap-gc")
            gc_thread.start()

        rss_stop=threading.Event()
        rss_samples=[rss_bytes()]
        def sample_rss():
            while not rss_stop.wait(.005):
                rss_samples.append(rss_bytes())
        sampler=threading.Thread(
            target=sample_rss,daemon=True)
        sampler.start()

        commit_latencies=[]
        peak_pending=0
        total_rows=int(commits)*int(rows_per_commit)
        capture_started=time.monotonic()
        try:
            for commit_index in range(int(commits)):
                if errors:
                    raise RuntimeError(
                        "background source worker failed: %r"
                        % (errors,))
                start=commit_index*int(rows_per_commit)
                commit_started=time.monotonic()
                seq=source_state.log_commit(
                    capture,"gc-overlap",
                    ("binlog.000001",100+commit_index*20),
                    None,
                    parts(
                        "db.events",start,
                        rows_per_commit,part_rows,key_space))
                commit_latencies.append(
                    time.monotonic()-commit_started)
                if seq!=commit_index+1:
                    raise AssertionError(
                        "unexpected durable source sequence")
                wake.set()
                peak_pending=max(
                    peak_pending,
                    source_state.apply_pending_bytes(
                        capture))
            capture_seconds=(
                time.monotonic()-capture_started)
            drain_started=time.monotonic()
            wait_applied(
                capture,commits,
                max(30,int(commits)*2))
            drain_seconds=(
                time.monotonic()-drain_started)
        finally:
            rss_stop.set()
            sampler.join(2)
            stop.set()
            wake.set()
            apply_thread.join(30)
            if gc_thread is not None:
                gc_thread.join(30)

        if apply_thread.is_alive():
            raise RuntimeError(
                "source apply worker did not stop")
        if gc_thread is not None and gc_thread.is_alive():
            raise RuntimeError(
                "source GC worker did not stop")
        if errors:
            raise RuntimeError(
                "background source worker failed: %r"
                % (errors,))

        cleanup_calls=0
        while True:
            cleanup=source_state.gc(
                capture,
                version_limit=gc_version_rows,
                commit_limit=gc_commit_rows,
            )
            cleanup_calls+=1
            if cleanup["complete"]:
                break
            if cleanup_calls>100000:
                raise RuntimeError(
                    "bounded source GC did not converge")

        status=source_state.status(capture)
        current=int(capture.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE table_name='db.events'
              AND valid_to IS NULL
              AND deleted=0
        """).fetchone()[0])
        expected_current=min(
            int(key_space),total_rows)
        if current!=expected_current:
            raise AssertionError(
                "source current state mismatch %d != %d"
                % (current,expected_current))
        if source_state.apply_pending_bytes(capture):
            raise AssertionError(
                "source apply backlog did not drain")
        retained_commits=int(capture.execute(
            "SELECT COUNT(*) FROM source_commits"
        ).fetchone()[0])
        if retained_commits!=1:
            raise AssertionError(
                "source GC retained unexpected commit count %d"
                % retained_commits)

        result=dict(
            gc_enabled=bool(gc_enabled),
            commits=int(commits),
            rows_per_commit=int(rows_per_commit),
            total_rows=total_rows,
            key_space=int(key_space),
            capture_wall_seconds=capture_seconds,
            drain_tail_seconds=drain_seconds,
            capture_rows_per_second=(
                total_rows/capture_seconds
                if capture_seconds else None),
            commit_latency_seconds=latency_summary(
                commit_latencies),
            peak_pending_bytes=int(peak_pending),
            rss_bytes=dict(
                peak=max(rss_samples),
                after=rss_bytes(),
            ),
            concurrent_gc=dict(
                calls=int(gc_calls[0]),
                deleted_versions=int(
                    gc_versions[0]),
                deleted_commits=int(
                    gc_commits[0]),
                latency_seconds=latency_summary(
                    gc_latencies),
            ),
            cleanup_calls=int(cleanup_calls),
            durable=dict(
                log_durable_seq=int(
                    status["log_durable_seq"]),
                base_applied_seq=int(
                    status["base_applied_seq"]),
                min_readable_seq=int(
                    status["min_readable_seq"]),
                pending_bytes=int(
                    status["apply_pending_bytes"]),
                current_rows=current,
                retained_commits=retained_commits,
            ),
        )
        capture.close()
        return result


def run(
        commits,rows_per_commit,part_rows,key_space,
        gc_version_rows,gc_commit_rows,gc_sleep_ms
):
    values=[
        int(commits),int(rows_per_commit),
        int(part_rows),int(key_space),
        int(gc_version_rows),int(gc_commit_rows),
        int(gc_sleep_ms),
    ]
    if min(values)<1:
        raise ValueError(
            "all benchmark sizes must be positive")
    if int(part_rows)>int(rows_per_commit):
        raise ValueError(
            "part_rows cannot exceed rows_per_commit")
    if int(key_space)>int(commits)*int(rows_per_commit):
        raise ValueError(
            "key_space cannot exceed total rows")

    baseline=run_case(
        commits,rows_per_commit,part_rows,key_space,
        False,gc_version_rows,gc_commit_rows,gc_sleep_ms)
    bounded=run_case(
        commits,rows_per_commit,part_rows,key_space,
        True,gc_version_rows,gc_commit_rows,gc_sleep_ms)
    base_latency=baseline["commit_latency_seconds"]
    gc_latency=bounded["commit_latency_seconds"]
    return dict(
        format_version=1,
        kind="source_gc_overlap_benchmark",
        scope=(
            "same_process_sqlite_capture_apply_with_and_without_"
            "bounded_history_gc"),
        gc_limits=dict(
            version_rows=int(gc_version_rows),
            commit_rows=int(gc_commit_rows),
            sleep_ms=int(gc_sleep_ms),
        ),
        baseline=baseline,
        bounded_gc=bounded,
        relative=dict(
            commit_p50_ratio=ratio(
                gc_latency["p50"],base_latency["p50"]),
            commit_p95_ratio=ratio(
                gc_latency["p95"],base_latency["p95"]),
            commit_p99_ratio=ratio(
                gc_latency["p99"],base_latency["p99"]),
            commit_max_ratio=ratio(
                gc_latency["maximum"],
                base_latency["maximum"]),
            capture_throughput_ratio=ratio(
                bounded["capture_rows_per_second"],
                baseline["capture_rows_per_second"]),
            peak_rss_ratio=ratio(
                bounded["rss_bytes"]["peak"],
                baseline["rss_bytes"]["peak"]),
        ),
        contract=dict(
            baseline_exact=True,
            bounded_gc_exact=True,
            bounded_gc_converged=True,
            no_slo_threshold_applied=True,
        ),
    )


def main():
    parser=argparse.ArgumentParser(
        description=__doc__)
    parser.add_argument(
        "--commits",type=int,default=80)
    parser.add_argument(
        "--rows-per-commit",type=int,default=500)
    parser.add_argument(
        "--part-rows",type=int,default=250)
    parser.add_argument(
        "--key-space",type=int,default=10000)
    parser.add_argument(
        "--gc-version-rows",type=int,default=512)
    parser.add_argument(
        "--gc-commit-rows",type=int,default=32)
    parser.add_argument(
        "--gc-sleep-ms",type=int,default=5)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    result=run(
        args.commits,args.rows_per_commit,
        args.part_rows,args.key_space,
        args.gc_version_rows,args.gc_commit_rows,
        args.gc_sleep_ms)
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
