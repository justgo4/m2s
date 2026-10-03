#!/usr/bin/env python3
"""Real SQLite contracts for cross-page snapshot FIFO and visible frontiers."""
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import j4


class SnapshotBundleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = str(Path(self.temporary.name)/"state.sqlite3")
        self.con = j4.init_state(self.path)
        self.addCleanup(lambda: self.con.close())
        j4.bootstrap(self.con,"test","source",("binlog.000001",4),["sink"])
        self.cfg = dict(key_partitions=16, batch_rows=1000, batch_bytes=1000,
                        max_inflight_deliveries=4, max_prepared_bytes=10000,
                        snapshot_bundle_max_lanes=8, max_row_bytes=1000)
        self.runtime = dict(control_lock=threading.Lock(), active_writers={"sink":1})

    def group(self, seq, last=False, version=0):
        group = "page_"+str(seq)
        self.con.execute("""
            INSERT INTO snapshot_groups(id,table_name,cursor,is_last,stage_seq,plan_version)
            VALUES(?,'sink',?,?,?,?)
        """,(group,j4.pack((seq,)),int(last),seq,version))
        return group

    def add(self, lane, group=None, kind="snapshot", version=0, rows=1, size=10):
        cursor = self.con.execute("""
            INSERT INTO jobs(table_name,lane,kind,payload,nrows,logical_bytes,
                             plan_version,group_id,created)
            VALUES('sink',?,?,X'00',?,?,?,?,?)
        """,(lane,kind,rows,size,version,group,time.time()))
        return cursor.lastrowid

    def claim(self, lane=0):
        return j4.claim_snapshot_bundle(self.con,"sink",lane,self.cfg,self.runtime)

    def members(self, delivery):
        return [row[0] for row in self.con.execute(
            "SELECT job_id FROM job_assignments WHERE delivery_id=? ORDER BY job_id",
            (delivery,))]

    def visible(self, delivery):
        self.con.execute("UPDATE deliveries SET prepared=1 WHERE id=?",(delivery,))
        j4.acknowledge_delivery(self.con,delivery)

    def test_multiple_pages_restart_and_cdc_fence(self):
        pages = [self.group(1),self.group(2,last=True)]
        expected = [self.add(lane,page) for page in pages for lane in range(8)]
        after = [self.add(lane,kind="cdc") for lane in range(8)]
        # A later snapshot cannot pass the intervening CDC in its lane.
        self.add(0,self.group(3))
        delivery = self.claim()
        self.assertEqual(self.members(delivery),expected)
        self.assertEqual(j4.delivery_lanes(self.con,delivery),list(range(8)))
        self.con.close()
        self.con = j4.open_state(self.path)
        self.assertEqual(self.members(self.claim()),expected)
        for lane in range(1,8):
            self.assertIsNone(self.claim(lane))
            self.assertIsNone(j4.claim_cdc_bundle(self.con,"sink",lane,self.cfg,self.runtime))
        self.assertEqual(self.con.execute("SELECT cursor FROM table_state").fetchone()[0],None)
        with self.assertRaises(RuntimeError):
            j4.acknowledge_delivery(self.con,delivery)
        self.visible(delivery)
        cursor,done = self.con.execute("SELECT cursor,snapshot_done FROM table_state").fetchone()
        self.assertEqual((j4.unpack(cursor),done),((2,),1))
        self.assertEqual([row[0] for row in self.con.execute(
            "SELECT id FROM active_jobs WHERE kind='cdc' ORDER BY id")],after)
        self.assertEqual(j4.merge_candidate_lanes(self.con,"sink"),list(range(8)))

    def test_later_page_visibility_cannot_skip_incomplete_earlier_page(self):
        first,second = self.group(1),self.group(2,last=True)
        early = self.add(0,first)
        later = self.add(1,second)
        # Width=1 lets a later independent lane complete first.
        self.cfg["snapshot_bundle_max_lanes"] = 1
        self.visible(self.claim(1))
        self.assertEqual(self.con.execute("SELECT cursor,snapshot_done FROM table_state").fetchone(),(None,0))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM snapshot_groups").fetchone()[0],2)
        self.visible(self.claim(0))
        cursor,done = self.con.execute("SELECT cursor,snapshot_done FROM table_state").fetchone()
        self.assertEqual((j4.unpack(cursor),done),((2,),1))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM snapshot_groups").fetchone()[0],0)

    def test_plan_group_and_assignment_boundaries(self):
        page = self.group(1)
        first = self.add(0,page,version=1)
        self.add(0,page,version=2)
        self.add(0,page,version=1)
        second = self.add(1,page,version=1)
        self.add(1,None,version=1)
        self.add(1,page,version=1)
        self.add(2,page,version=2)
        self.con.execute("INSERT INTO deliveries(id,table_name,lane,plan_version) VALUES('busy','sink',3,1)")
        busy = self.add(4,page,version=1)
        j4.assign_jobs(self.con,"busy",[busy])
        self.add(4,page,version=1)
        self.assertEqual(self.members(self.claim()),[first,second])
        self.assertIsNone(self.claim(4))

    def test_budgets_and_oom_shrink_preserve_page_membership(self):
        first,second = self.group(1),self.group(2,last=True)
        a,b = self.add(0,first,size=100,rows=2),self.add(0,second,size=100,rows=2)
        c,d = self.add(1,first,size=100,rows=2),self.add(1,second,size=100,rows=2)
        self.cfg.update(batch_rows=6,batch_bytes=350)
        delivery = self.claim()
        self.assertEqual(self.members(delivery),[a,b,c])
        shrunk = j4.shrink_unprepared_delivery(self.con,delivery)
        self.assertTrue(shrunk["shrunk"])
        self.assertEqual(self.members(delivery),[a,b])
        self.visible(delivery)
        self.assertEqual(self.con.execute("SELECT cursor FROM table_state").fetchone()[0],None)
        self.cfg["max_prepared_bytes"] = 0
        self.assertIsNone(self.claim(1))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM job_assignments").fetchone()[0],0)
        self.cfg["max_prepared_bytes"] = 10000
        self.runtime["snapshot_transform_bytes_cap"] = {"sink":150}
        delivery = self.claim(1)
        self.assertEqual(self.members(delivery),[c])
        self.visible(delivery)
        cursor,done = self.con.execute("SELECT cursor,snapshot_done FROM table_state").fetchone()
        self.assertEqual((j4.unpack(cursor),done),((1,),0))
        self.assertEqual(self.members(self.claim(1)),[d])

    def test_owner_prefix_halving_and_claim_job_count_bound(self):
        page = self.group(1,last=True)
        self.cfg["batch_rows"] = 10000
        ids = [self.add(0,page,size=0) for _ in range(4100)]
        delivery = self.claim()
        self.assertEqual(self.members(delivery),ids[:4096])
        self.assertTrue(j4.shrink_unprepared_delivery(self.con,delivery)["shrunk"])
        self.assertEqual(self.members(delivery),ids[:2048])
        self.visible(delivery)
        self.assertEqual(self.con.execute("SELECT snapshot_done FROM table_state").fetchone()[0],0)
        self.assertEqual(self.members(self.claim()),ids[2048:])

    def test_multi_page_arrow_prepare_and_invisible_parts_guard(self):
        import gzip
        import json
        import pyarrow as pa

        self.cfg.update(batch_bytes=65536,max_row_bytes=65536,
                        max_prepared_bytes=1048576,compression="gzip",
                        duckdb_memory="64MB",state=self.path)
        mapping = dict(src_table="sink",sr_table="sink",primary_key="id",
                       sql="SELECT id,v FROM arrow_batch",full_filter="v >= 0",
                       _schema=[("id",pa.int64()),("v",pa.int64())],
                       _target_sequence=False,_output_columns=["id","v"])
        j4.validate_mapping(mapping)
        engine = j4.transform_engine(self.cfg)
        self.addCleanup(engine.close)
        pages = [self.group(1),self.group(2,last=True)]
        for page,rows in zip(pages,[[{"id":1,"v":10}],[{"id":2,"v":20}]]):
            job = self.add(0,page)
            payload = j4.arrow_job_payload(mapping,[(0,row) for row in rows],self.cfg,engine)
            self.con.execute("UPDATE jobs SET payload=?,logical_bytes=? WHERE id=?",
                             (payload,len(payload),job))
        delivery = self.claim()
        self.assertTrue(j4.prepare_delivery(self.con,engine,mapping,delivery,self.cfg))
        payloads = self.con.execute("SELECT payload FROM load_parts WHERE delivery_id=? ORDER BY part",
                                    (delivery,)).fetchall()
        output = [json.loads(line) for payload, in payloads for line in gzip.decompress(payload).splitlines()]
        self.assertEqual(sorted((row["id"],row["v"]) for row in output),[(1,10),(2,20)])
        self.con.close()
        self.con = j4.open_state(self.path)
        with self.assertRaisesRegex(RuntimeError,"invisible"):
            j4.acknowledge_delivery(self.con,delivery)
        self.con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=?",(delivery,))
        j4.acknowledge_delivery(self.con,delivery)
        self.assertEqual(self.con.execute("SELECT snapshot_done FROM table_state").fetchone()[0],1)


if __name__ == "__main__":
    unittest.main()
