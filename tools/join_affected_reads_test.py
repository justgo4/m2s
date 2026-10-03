#!/usr/bin/env python3
"""Changed-PK read bounds with independent full-state/bag/delta oracles."""
from pathlib import Path
import random
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import join_ir
import join_state


def spec():
    def column(name,kind):
        return (name,kind,kind,'YES',None,'')
    ir=join_ir.compile_sql('db.left',
        [column('id','bigint'),column('k','bigint'),column('v','bigint')],'id',
        'db.right',[column('id','bigint'),column('k','bigint'),column('label','varchar')],'id',
        'SELECT l.v AS v,r.label AS label FROM left_batch l '
        'JOIN right_batch r ON l.k=r.k')
    return join_ir.state_spec(ir)


class AffectedReadsTest(unittest.TestCase):
    def setUp(self):
        self.con=sqlite3.connect(':memory:',isolation_level=None)
        self.addCleanup(self.con.close)
        join_state.install(self.con)
        self.spec=spec()
        self.rows=dict(left={},right={})

    def seed(self,left,right):
        join_state.begin_bootstrap(self.con,'state',self.spec,0)
        for side,rows in [('left',left),('right',right)]:
            self.rows[side]={row['id']:dict(row) for row in rows}
            join_state.apply_bootstrap_chunk(self.con,'state',0,side,rows,b'end',True)

    def oracle(self):
        result={}
        for left in self.rows['left'].values():
            for right in self.rows['right'].values():
                if left['k'] is None or right['k'] is None or left['k']!=right['k']:
                    continue
                identity=join_state._pair_id(join_state._pk_blob(self.spec,'left',left),
                                              join_state._pk_blob(self.spec,'right',right))
                result[identity]=dict(v=left['v'],label=right['label'])
        return result

    def apply(self,seq,changes):
        before=self.oracle()
        for side,image in changes:
            row={key:value for key,value in image.items() if key!='_sync_op'}
            if image['_sync_op']==1:
                del self.rows[side][row['id']]
            else:
                self.rows[side][row['id']]=row
        after=self.oracle()
        expected={key:(1,before[key]) if key not in after else (0,after[key])
                  for key in before.keys()|after.keys() if before.get(key)!=after.get(key)}
        result=join_state.apply_transaction(self.con,'state',seq,changes)
        actual={row['pair_id']:(row['op'],row['row']) for row in result['deltas']}
        self.assertEqual(actual,expected)
        self.assertEqual(len(actual),len(result['deltas']))
        self.assertEqual({row['pair_id']:row['row'] for row in join_state.read_pairs(self.con,'state')},after)
        return result

    def test_left_change_does_not_read_unchanged_left_range(self):
        self.seed([dict(id=i,k=1,v=7) for i in range(1000)],
                  [dict(id=i,k=1,label='same') for i in range(3)])
        original=join_state._rows_for_join_blob
        reads=[]
        def read(con,state,side,key):
            self.assertNotEqual(side,'left','unchanged left range was scanned')
            rows=original(con,state,side,key)
            reads.append(len(rows))
            return rows
        with patch.object(join_state,'_rows_for_join_blob',side_effect=read):
            # Check the incremental path without the deliberately full oracle read.
            result=join_state.apply_transaction(self.con,'state',1,[('left',dict(id=5,k=1,v=8,_sync_op=0))])
        self.rows['left'][5]['v']=8
        self.assertEqual(len(result['deltas']),3)
        self.assertEqual(reads,[3,3])
        self.assertEqual({row['pair_id']:row['row'] for row in join_state.read_pairs(self.con,'state')},self.oracle())

    def test_right_change_keeps_all_left_fanout_without_right_range(self):
        self.seed([dict(id=i,k=1,v=7) for i in range(300)],
                  [dict(id=i,k=1,label='same') for i in range(50)])
        original=join_state._rows_for_join_blob
        reads=[]
        def read(con,state,side,key):
            self.assertNotEqual(side,'right','unchanged right range was scanned')
            rows=original(con,state,side,key)
            reads.append(len(rows))
            return rows
        with patch.object(join_state,'_rows_for_join_blob',side_effect=read):
            result=join_state.apply_transaction(self.con,'state',1,[('right',dict(id=5,k=1,label='new',_sync_op=0))])
        self.rows['right'][5]['label']='new'
        self.assertEqual(len(result['deltas']),300)
        self.assertEqual(reads,[300,300])
        self.assertEqual({row['pair_id']:row['row'] for row in join_state.read_pairs(self.con,'state')},self.oracle())

    def test_bilateral_rekeys_null_and_repeated_pk_have_exact_net_deltas(self):
        self.seed([dict(id=1,k=1,v=7),dict(id=2,k=None,v=7)],
                  [dict(id=1,k=1,label='same'),dict(id=2,k=2,label='same')])
        self.apply(1,[('left',dict(id=1,k=2,v=8,_sync_op=0)),
                      ('right',dict(id=2,k=1,label='moved',_sync_op=0)),
                      ('left',dict(id=2,k=1,v=7,_sync_op=0)),
                      ('right',dict(id=1,k=None,label='same',_sync_op=0)),
                      ('left',dict(id=1,k=1,v=9,_sync_op=0))])
        result=self.apply(2,[('left',dict(id=1,k=1,v=9,_sync_op=1)),
                            ('left',dict(id=1,k=1,v=9,_sync_op=0))])
        self.assertEqual(result['deltas'],[])

    def test_randomized_full_bag_and_delta_oracle_with_rollback_retry(self):
        rng=random.Random(27491)
        self.seed([dict(id=i,k=i%4,v=i%3) for i in range(20)],
                  [dict(id=i,k=i%4,label=str(i%2)) for i in range(20)])
        for seq in range(1,151):
            changes=[]
            working={side:{key:dict(row) for key,row in rows.items()} for side,rows in self.rows.items()}
            for _ in range(rng.randrange(1,9)):
                side=rng.choice(['left','right'])
                key=rng.randrange(25)
                current=working[side].get(key)
                if current and rng.randrange(4)==0:
                    changes.append((side,dict(current,_sync_op=1)))
                    del working[side][key]
                else:
                    row=dict(id=key,k=rng.choice([None,0,1,2,3,4]))
                    row.update(dict(v=rng.randrange(4)) if side=='left' else dict(label=str(rng.randrange(3))))
                    changes.append((side,dict(row,_sync_op=0)))
                    working[side][key]=row
            if seq%13==0:
                def crash(_):
                    raise RuntimeError('synthetic rollback')
                with self.assertRaisesRegex(RuntimeError,'synthetic rollback'):
                    join_state.apply_transaction(self.con,'state',seq,changes,fault_after_rows=crash)
                self.assertEqual(join_state.state_info(self.con,'state')['watermark'],seq-1)
                self.assertEqual({row['pair_id']:row['row'] for row in join_state.read_pairs(self.con,'state')},self.oracle())
            self.apply(seq,changes)
            self.assertEqual(join_state.apply_transaction(self.con,'state',seq,changes),dict(applied=False,deltas=[]))


if __name__=='__main__':
    unittest.main()
