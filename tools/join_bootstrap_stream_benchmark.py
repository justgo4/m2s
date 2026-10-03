#!/usr/bin/env python3
"""Isolated old/new atomic JOIN seed A/B; synthetic fan-out, no SLO claim."""
import argparse
import hashlib
import json
from pathlib import Path
import pickle
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/"tools")]
import join_bootstrap_stream_test as fixture
import join_outbox
import join_state


def measure(mode,left,right):
    with tempfile.TemporaryDirectory() as directory:
        con=fixture.open_db(str(Path(directory)/"state.sqlite3"))
        fixture.setup_state(con,left,right)
        started=time.monotonic()
        cpu=time.process_time()
        # Production activation wraps stream + seed in one write transaction.
        with join_outbox.transaction(con):
            if mode=="stream":
                result=join_outbox.seed_bootstrap(con,"consumer","state",1,"generation",0)
            else:
                join_outbox.ensure_stream(con,"consumer","state",1,"generation",0)
                rows=[(item["pair_id"],0,pickle.dumps(item["row"],protocol=5))
                      for item in join_state.read_pairs(con,"state")]
                for pair_id,_,_ in rows:
                    join_outbox._register_pair_identity_locked(con,"consumer",pair_id)
                join_outbox._insert_commit(con,"consumer",0,"bootstrap",rows)
                result=join_outbox.commit_info(con,"consumer",0)
        output=dict(mode=mode,left_rows=left,right_rows=right,output_pairs=result["nrows"],
                    digest=result["digest"],wall_seconds=time.monotonic()-started,
                    cpu_seconds=time.process_time()-cpu,
                    process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=="darwin" else 1024),
                    scope="synthetic_atomic_bootstrap",write_lock_duration_bounded=False,
                    software=dict(python=sys.version,sqlite=sqlite3.sqlite_version,
                                  source_sha256={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
                                                 for name in ["join_state.py","join_outbox.py"]}))
        con.close()
        return output


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left",type=int,default=1000)
    parser.add_argument("--right",type=int,default=100)
    parser.add_argument("--repeats",type=int,default=3)
    parser.add_argument("--mode",choices=["cache","stream"])
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    if min(args.left,args.right,args.repeats)<1:
        parser.error("sizes/repeats must be positive")
    if args.mode:
        print(json.dumps(measure(args.mode,args.left,args.right)),flush=True)
        return
    runs=[]
    for _ in range(args.repeats):
        for mode in ["cache","stream"]:
            completed=subprocess.run([sys.executable,__file__,"--mode",mode,
                                      "--left",str(args.left),"--right",str(args.right)],
                                     capture_output=True,text=True,check=True)
            runs.append(json.loads(completed.stdout))
    if len({item["digest"] for item in runs})!=1 or any(
            item["output_pairs"]!=args.left*args.right for item in runs):
        raise RuntimeError("old/new seed digest or exact pair count differs")
    result=dict(kind="m2s_join_bootstrap_stream_ab_v1",runs=runs,exact_match=True,
                scope="synthetic_atomic_bootstrap",not_certification=True)
    rendered=json.dumps(result,indent=2)+"\n"
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(rendered)
    print(rendered,flush=True)


if __name__=="__main__":
    main()
