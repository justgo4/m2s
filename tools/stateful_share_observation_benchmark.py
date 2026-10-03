#!/usr/bin/env python3
"""Synthetic identical idle observations on real WAL/FULL; no SLO claim."""
from pathlib import Path
import argparse
import hashlib
import json
import sqlite3
import sys
import tempfile
import time
from unittest.mock import patch

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import stateful_share_policy as policy
import stateful_share_observation_test as fixture


def measure(interval,followers,polls):
    with tempfile.TemporaryDirectory() as directory:
        con=fixture.open_db(str(Path(directory)/"state.sqlite3"))
        con.execute("PRAGMA wal_autocheckpoint=0")
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        initial=con.total_changes
        started=time.monotonic()
        for poll in range(polls):
            with patch.object(policy.time,"time",return_value=100+.05*poll):
                for follower in range(followers):
                    policy.observe(con,"follower-%d" % follower,10,10,10,10,
                        unchanged_interval=interval)
        elapsed=time.monotonic()-started
        rows=con.execute("SELECT SUM(samples),MAX(max_visible_lag),SUM(copied_sequences) FROM stateful_share_observations").fetchone()
        checkpoint=con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        result=dict(unchanged_interval=interval,poll_calls=followers*polls,
                    durable_observation_writes=con.total_changes-initial,
                    durable_samples=rows[0],max_visible_lag=rows[1],copied_sequences=rows[2],
                    wal_frames=checkpoint[1],wall_seconds=elapsed,
                    synchronous=con.execute("PRAGMA synchronous").fetchone()[0])
        con.close()
        return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--followers",type=int,default=100)
    parser.add_argument("--polls",type=int,default=40)
    parser.add_argument("--output",type=Path)
    args=parser.parse_args()
    if min(args.followers,args.polls)<1:
        parser.error("followers/polls must be positive")
    result=dict(kind="m2s_share_observation_idle_v1",scope="synthetic_idle_wal_full",
                not_certification=True,sqlite=sqlite3.sqlite_version,
                source_sha256=hashlib.sha256(Path(policy.__file__).read_bytes()).hexdigest(),
                runs=[measure(0,args.followers,args.polls),measure(1,args.followers,args.polls)])
    rendered=json.dumps(result,indent=2)+"\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered,flush=True)


if __name__=="__main__":
    main()
