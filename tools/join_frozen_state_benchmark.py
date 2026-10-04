#!/usr/bin/env python3
"""Fresh-process WAL/FULL follower bootstrap transactions, synthetic evidence."""
from pathlib import Path
import argparse
import hashlib
import json
import pickle
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tools')]
import join_state
import join_outbox
import join_output_build
import join_frozen_state
import join_bootstrap_stream_test as fixture


class Measured(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        operation=sql.strip().upper()
        start=time.monotonic()
        result=super().execute(sql,parameters)
        if getattr(self,'measuring',False):
            if operation=='BEGIN IMMEDIATE':
                self.started=start
            elif operation in ('COMMIT','ROLLBACK') and self.started is not None:
                self.intervals.append(time.monotonic()-self.started)
                self.started=None
        return result


def worker(mode,rows):
    with tempfile.TemporaryDirectory(prefix='m2s-frozen-benchmark-') as td:
        con=sqlite3.connect(str(Path(td)/'state.sqlite3'),isolation_level=None,factory=Measured)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=FULL')
        con.execute('PRAGMA foreign_keys=ON')
        join_state.install(con)
        join_outbox.install(con)
        spec=fixture.setup_state(con,left=rows,right=1)
        con.intervals=[]
        con.started=None
        con.measuring=True
        started=time.monotonic()
        if mode=='atomic':
            join_outbox.seed_bootstrap(con,'consumer','state',1,'generation',0)
        else:
            join_frozen_state.pin(con,'build','state',0)
            while not join_frozen_state.copy_step(con,'build','snapshot',spec)['done']:
                pass
            join_frozen_state.release(con,'build')
            while not join_output_build.step(con,'consumer','snapshot',1,'generation',0,
                                            stream_state_id='state')['done']:
                pass
            while not join_frozen_state.discard_step(con,'snapshot'):
                pass
            with join_state.transaction(con):
                con.execute('DELETE FROM join_output_builds')
                con.execute("DELETE FROM join_states WHERE state_id='snapshot'")
        elapsed=time.monotonic()-started
        measured_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        con.measuring=False
        # Full output/identity/payload oracle, independently enumerating known
        # input PKs; NULL rows intentionally produce no pair.
        expected={}
        for i in range(rows):
            import base64
            left=json.dumps([['int',str(i)]],separators=(',',':')).encode()
            right=b'[["int","0"]]'
            pair=json.dumps([['left',base64.b64encode(left).decode()],
                             ['right',base64.b64encode(right).decode()]],separators=(',',':')).encode()
            expected[pair]=dict(customer_name='same',amount=7)
        output=con.execute('''SELECT pair_id,op,row_payload FROM join_output_rows
            WHERE consumer_id='consumer' ORDER BY pair_id''').fetchall()
        actual={bytes(pk):pickle.loads(payload) for pk,op,payload in output if op==0}
        if actual!=expected or len(output)!=rows:
            raise RuntimeError('full synthetic output oracle mismatch')
        commit=join_outbox.commit_info(con,'consumer',0)
        if commit['digest']!=join_outbox._digest('bootstrap',output):
            raise RuntimeError('canonical digest mismatch')
        for pair, in con.execute('SELECT pair_id FROM join_output_identities'):
            if pair not in expected:
                raise RuntimeError('identity oracle mismatch')
        result=dict(mode=mode,source_rows=rows+3,output_rows=rows,full_oracle=True,
                    digest=commit['digest'],wall_seconds=elapsed,peak_rss_before_oracle_bytes=measured_rss,
                    transactions=len(con.intervals),max_transaction_seconds=max(con.intervals),
                    total_transaction_seconds=sum(con.intervals))
        con.close()
        return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,default=100000)
    parser.add_argument('--worker',choices=['atomic','frozen'])
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if args.rows<1:
        parser.error('rows must be positive')
    if args.worker:
        print(json.dumps(worker(args.worker,args.rows)),flush=True)
        return
    runs=[]
    for mode in ['atomic','frozen']:
        process=subprocess.run([sys.executable,__file__,'--rows',str(args.rows),'--worker',mode],
                               capture_output=True,text=True,check=True)
        runs.append(json.loads(process.stdout))
    if runs[0]['digest']!=runs[1]['digest']:
        raise RuntimeError('different output digests')
    result=dict(kind='synthetic_join_frozen_follower_v1',not_certification=True,
                sqlite=sqlite3.sqlite_version,runs=runs,
                source_sha256={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
                               for name in ['join_state.py','join_frozen_state.py','join_output_build.py']})
    rendered=json.dumps(result,indent=2)+'\n'
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(rendered)
    print(rendered,flush=True)


if __name__=='__main__':
    main()
