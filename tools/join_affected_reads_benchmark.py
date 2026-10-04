#!/usr/bin/env python3
"""Isolated synthetic comparison of full-range and changed-PK JOIN reads."""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import join_ir
import join_state


def _full_ranges(con,state_id,keys,changed):
    # Independent pre-candidate reference: read both complete ranges.
    return {key:dict(left=join_state._rows_for_join_blob(con,state_id,'left',key),
                     right=join_state._rows_for_join_blob(con,state_id,'right',key))
            for key in sorted(set(keys)) if key is not None}


def _bag(rows):
    count=total=0
    modulus=1<<256
    for row in rows:
        digest=hashlib.sha256(json.dumps(row,separators=(',',':')).encode()).digest()
        count+=1
        total=(total+int.from_bytes(digest,'big'))%modulus
    return dict(count=count,sha256_sum='%064x' % total)


def worker(rows,variant):
    def column(name,kind):
        return (name,kind,kind,'YES',None,'')
    ir=join_ir.compile_sql('db.events',
        [column('id','bigint'),column('k','bigint'),column('v','bigint')],'id',
        'db.dimensions',[column('id','bigint'),column('k','bigint'),column('label','varchar')],'id',
        'SELECT l.id AS id,l.v AS v,r.label AS label FROM left_batch l '
        'JOIN right_batch r ON l.k=r.k')
    with tempfile.TemporaryDirectory() as td:
        con=sqlite3.connect(str(Path(td)/'state.sqlite3'),isolation_level=None)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=FULL')
        join_state.install(con)
        join_state.begin_bootstrap(con,'state',join_ir.state_spec(ir),0)
        for offset in range(0,rows,10000):
            end=min(rows,offset+10000)
            join_state.apply_bootstrap_chunk(con,'state',0,'left',
                [dict(id=i,k=i%1024,v=i) for i in range(offset,end)],str(end).encode(),end==rows)
        join_state.apply_bootstrap_chunk(con,'state',0,'right',
            [dict(id=i,k=i,label='dim-'+str(i)) for i in range(1024)],b'end',True)
        original=join_state._rows_for_join_blob
        reads=dict(left=0,right=0)
        def counted(con,state,side,key):
            found=original(con,state,side,key)
            reads[side]+=len(found)
            return found
        snapshot=_full_ranges if variant=='full_ranges' else join_state._rows_for_keys_locked
        changed={}
        durations=[]
        delta_rows=0
        with patch.object(join_state,'_rows_for_keys_locked',side_effect=snapshot), \
             patch.object(join_state,'_rows_for_join_blob',side_effect=counted):
            for seq in range(1,11):
                changes=[]
                for index in range((seq-1)*50,seq*50):
                    changes.append(('left',dict(id=index,k=index%1024,v=-seq,_sync_op=0)))
                    changed[index]=-seq
                start=time.monotonic()
                result=join_state.apply_transaction(con,'state',seq,changes)
                durations.append(time.monotonic()-start)
                if len(result['deltas'])!=50 or any(row['op']!=0 for row in result['deltas']):
                    raise AssertionError('synthetic incremental output is not exact')
                delta_rows+=len(result['deltas'])
        actual=_bag([item['row']['id'],item['row']['v'],item['row']['label']]
                    for item in join_state.iter_pairs(con,'state'))
        oracle=_bag([i,changed.get(i,i),'dim-'+str(i%1024)] for i in range(rows))
        if actual!=oracle:
            raise AssertionError('complete independent output bag differs')
        con.close()
        return dict(variant=variant,source_rows=rows,dimension_rows=1024,transactions=10,
                    changed_rows_per_transaction=50,delta_rows=delta_rows,range_rows_read=reads,
                    total_apply_seconds=sum(durations),max_apply_seconds=max(durations),
                    full_bag=actual,full_bag_oracle=oracle,exact=True,
                    measurement='apply_transaction wall time includes BEGIN wait and FULL COMMIT; no daemon/remote/SLO')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--rows',type=int,nargs='+',default=[100000,1000000])
    parser.add_argument('--worker',choices=['full_ranges','changed_pk'])
    args=parser.parse_args()
    if any(rows<500 for rows in args.rows):
        parser.error('rows must be >=500')
    if args.worker:
        print(json.dumps(worker(args.rows[0],args.worker)))
        return
    results=[]
    for rows in args.rows:
        pair=[]
        for variant in ['full_ranges','changed_pk']:
            result=subprocess.run([sys.executable,__file__,'--rows',str(rows),'--worker',variant],
                                  capture_output=True,text=True)
            if result.returncode:
                raise RuntimeError('worker failed: '+result.stderr[-4000:])
            pair.append(json.loads(result.stdout))
        if pair[0]['full_bag']!=pair[1]['full_bag']:
            raise AssertionError('variants differ')
        results.extend(pair)
    print(json.dumps(dict(scope='synthetic SQLite JOIN compute; independent child per variant',
                          sqlite_version=sqlite3.sqlite_version,results=results),indent=2))


if __name__=='__main__':
    main()
