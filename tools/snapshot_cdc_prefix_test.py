#!/usr/bin/env python3
"""Snapshot plus immediate CDC suffix keeps durable per-lane FIFO contracts."""
import json
from pathlib import Path
import sys
import unittest

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import j4
import cdc_bundle_test as fixture


class SnapshotCdcPrefixTest(unittest.TestCase):
    def setUp(self):
        fixture.CdcBundleTest.setUp(self)
        self.cfg['snapshot_bundle_max_lanes']=16
        j4.bootstrap(self.con,'fixture','source',('binlog.000001',4),['sink'])
        self.group('first',1,False)

    def group(self,name,sequence,last):
        self.con.execute('''INSERT INTO snapshot_groups(
            id,table_name,cursor,is_last,stage_seq,plan_version)
            VALUES(?,'sink',?,?,?,0)''',(name,j4.pack((sequence,)),int(last),sequence))

    def add(self,lane,kind='cdc',group=None,**kw):
        fixture.CdcBundleTest.add(self,lane,kind=kind,**kw)
        identity=self.con.execute('SELECT max(id) FROM jobs').fetchone()[0]
        if group is not None:
            self.con.execute('UPDATE jobs SET group_id=? WHERE id=?',(group,identity))
        return identity

    def claim(self):
        return j4.claim_snapshot_bundle(self.con,'sink',0,self.cfg,self.runtime)

    def assigned(self,delivery):
        return [row[0] for row in self.con.execute(
            'SELECT job_id FROM job_assignments WHERE delivery_id=? ORDER BY job_id',(delivery,))]

    def ack(self,delivery):
        self.con.execute('UPDATE deliveries SET prepared=1 WHERE id=?',(delivery,))
        self.con.execute('UPDATE load_parts SET visible=1 WHERE delivery_id=?',(delivery,))
        j4.acknowledge_delivery(self.con,delivery)

    def test_fifo_membership_group_and_cdc_frontiers_survive_restart(self):
        selected=[]
        for lane in range(16):
            selected.append(self.add(lane,'snapshot','first'))
        for lane in range(16):
            selected.append(self.add(lane,seq=2))
        delivery=self.claim()
        self.assertEqual(self.assigned(delivery),sorted(selected))
        self.add(1,seq=3)
        self.con.close()
        self.con=j4.open_state(self.path)
        self.assertEqual(self.assigned(delivery),sorted(selected))
        self.assertEqual(j4.merge_candidate_lanes(self.con,'sink'),[0])
        with self.assertRaises(RuntimeError):
            j4.acknowledge_delivery(self.con,delivery)
        self.ack(delivery)
        self.assertIsNone(self.con.execute("SELECT 1 FROM snapshot_groups WHERE id='first'").fetchone())
        self.assertEqual({r['source_seq'] for r in j4.visible_frontiers(self.con,'sink')},{2})
        self.assertEqual(j4.merge_candidate_lanes(self.con,'sink'),[1])

    def test_snapshot_and_plan_boundaries_stop_each_suffix(self):
        self.group('second',2,True)
        first=self.add(0,'snapshot','first')
        tail=self.add(0,seq=2)
        second=self.add(0,'snapshot','second',seq=3)
        later=self.add(0,seq=4)
        other=self.add(1,'snapshot','first')
        wrong=self.add(1,version=1,seq=2)
        blocked=self.add(1,version=0,seq=3)
        delivery=self.claim()
        self.assertEqual(self.assigned(delivery),sorted([first,tail,other]))
        self.assertTrue(set([second,later,wrong,blocked]).isdisjoint(self.assigned(delivery)))
        self.ack(delivery)
        self.assertIsNotNone(self.con.execute("SELECT 1 FROM snapshot_groups WHERE id='second'").fetchone())
        self.assertEqual(j4.unpack(self.con.execute("SELECT cursor FROM table_state WHERE name='sink'").fetchone()[0]),(1,))
        self.assertEqual(self.assigned(self.claim()),[second,later])

    def test_existing_row_byte_and_reservation_budgets_bound_suffixes(self):
        head=self.add(0,'snapshot','first',rows=2,size=100)
        tail=self.add(0,seq=2,rows=2,size=100)
        self.add(0,seq=3,rows=2,size=100)
        for rows,bytes_,expected in [(3,1000,[head]),(100,250,[head,tail])]:
            self.cfg.update(batch_rows=rows,batch_bytes=bytes_)
            delivery=self.claim()
            self.assertEqual(self.assigned(delivery),expected)
            self.con.execute('DELETE FROM deliveries WHERE id=?',(delivery,))
        self.cfg['max_prepared_bytes']=1
        self.con.execute("INSERT INTO deliveries(id,table_name,lane) VALUES('busy','other',0)")
        self.assertTrue(j4.prepare_reservation_set_locked(self.con,'busy',1,self.cfg))
        self.assertIsNone(self.claim())
        self.assertEqual(self.con.execute('SELECT count(*) FROM job_assignments').fetchone()[0],0)

    def test_arrow_net_update_delete_wire_and_visibility_fence(self):
        import pyarrow as pa
        self.cfg.update(batch_bytes=65536,max_row_bytes=65536,max_prepared_bytes=1048576,
                        compression='',duckdb_memory='64MB',state=self.path)
        mapping=dict(src_table='sink',sr_table='sink',primary_key='id',
                     sql='SELECT id,v FROM arrow_batch',full_filter=None,
                     _schema=[('id',pa.int64()),('v',pa.int64())],
                     _target_sequence=False,_output_columns=['id','v'])
        j4.validate_mapping(mapping)
        engine=j4.transform_engine(self.cfg)
        self.addCleanup(engine.close)
        for lane,mutations in [(0,[(0,dict(id=0,v=1))]),(1,[(0,dict(id=1,v=1))])]:
            head=self.add(lane,'snapshot','first')
            payload=j4.arrow_job_payload(mapping,mutations,self.cfg,engine)
            self.con.execute('UPDATE jobs SET payload=?,logical_bytes=? WHERE id=?',(payload,len(payload),head))
        for lane,mutations in [(0,[(1,dict(id=0,v=1)),(0,dict(id=0,v=9))]),(1,[(1,dict(id=1,v=1))])]:
            tail=self.add(lane,seq=2,rows=len(mutations))
            payload=j4.arrow_job_payload(mapping,mutations,self.cfg,engine)
            self.con.execute('UPDATE jobs SET payload=?,logical_bytes=? WHERE id=?',(payload,len(payload),tail))
        delivery=self.claim()
        self.assertEqual(len(self.assigned(delivery)),4)
        self.assertTrue(j4.prepare_delivery(self.con,engine,mapping,delivery,self.cfg))
        parts=self.con.execute('SELECT payload FROM load_parts WHERE delivery_id=? ORDER BY part',(delivery,)).fetchall()
        actual=[json.loads(line) for payload, in parts for line in payload.splitlines()]
        self.assertEqual(sorted((r['id'],r['v'],r['__op']) for r in actual),[(0,9,0),(1,1,1)])
        for payload, in parts:
            self.assertFalse(j4.merge_payload_profile(mapping,payload)['replay_safe'])
        future=self.add(0,seq=3)
        payload=j4.arrow_job_payload(mapping,[(1,dict(id=0,v=9))],self.cfg,engine)
        self.con.execute('UPDATE jobs SET payload=?,logical_bytes=? WHERE id=?',(payload,len(payload),future))
        self.con.close()
        self.con=j4.open_state(self.path)
        self.assertEqual(j4.claim_cdc_bundle(self.con,'sink',0,self.cfg,self.runtime),delivery)
        self.assertTrue(j4.prepare_delivery(self.con,engine,mapping,delivery,self.cfg))
        self.assertEqual(self.con.execute('SELECT payload FROM load_parts WHERE delivery_id=? ORDER BY part',(delivery,)).fetchall(),parts)
        with self.assertRaisesRegex(RuntimeError,'invisible'):
            j4.acknowledge_delivery(self.con,delivery)
        self.assertIsNotNone(self.con.execute("SELECT 1 FROM snapshot_groups WHERE id='first'").fetchone())
        self.ack(delivery)
        self.assertEqual(self.con.execute('SELECT count(*) FROM active_jobs').fetchone()[0],1)
        self.assertEqual({r['source_seq'] for r in j4.visible_frontiers(self.con,'sink')},{2})
        later=j4.claim_cdc_bundle(self.con,'sink',0,self.cfg,self.runtime)
        self.assertNotEqual(later,delivery)
        self.assertEqual(self.assigned(later),[future])


if __name__=='__main__':
    unittest.main()
