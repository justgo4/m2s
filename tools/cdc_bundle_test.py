#!/usr/bin/env python3
"""Durable FIFO/resource boundaries for constrained-writer CDC bundles."""
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import cdc_catalog
import j4


class CdcBundleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = str(Path(self.temporary.name)/"state.sqlite3")
        self.con = j4.init_state(self.path)
        self.addCleanup(lambda: self.con.close())
        self.cfg = dict(key_partitions=16, batch_rows=1000, batch_bytes=1000,
                        max_inflight_deliveries=4, max_prepared_bytes=1024*1024,
                        max_row_bytes=1000)
        self.runtime = dict(control_lock=threading.Lock(), active_writers={"sink": 1})

    def add(self, lane, version=0, kind="cdc", seq=1, size=10, rows=1):
        now = time.time()
        self.con.execute("""
            INSERT INTO jobs(table_name,lane,kind,payload,nrows,logical_bytes,plan_version,
                             source_file,source_pos,source_seq,source_time,created)
            VALUES('sink',?,?,X'00',?,?,?,'binlog.000001',?,?,?,?)
        """, (lane,kind,rows,size,version,seq*100,seq,now,now))

    def claim(self, lane=0):
        return j4.claim_cdc_bundle(self.con, "sink", lane, self.cfg, self.runtime)

    def test_wide_membership_and_fifo_survive_restart(self):
        for lane in range(16):
            self.add(lane)
        delivery = self.claim()
        self.assertEqual(j4.delivery_lanes(self.con, delivery), list(range(16)))
        for lane in range(16):
            self.add(lane, seq=2)
        self.con.close()
        self.con = j4.open_state(self.path)
        self.assertEqual(j4.merge_candidate_lanes(self.con, "sink"), [0])
        for lane in range(1,16):
            self.assertEqual(j4.lane_blocking_delivery(self.con, "sink", lane), delivery)
            self.assertIsNone(self.claim(lane))
        with self.assertRaises(RuntimeError):
            j4.acknowledge_delivery(self.con, delivery)
        self.con.execute("UPDATE deliveries SET prepared=1 WHERE id=?", (delivery,))
        j4.acknowledge_delivery(self.con, delivery)
        self.assertEqual(j4.merge_candidate_lanes(self.con, "sink"), list(range(16)))
        self.assertEqual({x["source_seq"] for x in j4.visible_frontiers(self.con, "sink")}, {1})
        self.assertEqual(j4.delivery_lanes(self.con, self.claim()), list(range(16)))

    def test_rows_bytes_and_prepared_budget_bound_the_wide_bundle(self):
        for lane in range(16):
            self.add(lane, size=100, rows=2)
        self.cfg.update(batch_rows=100, batch_bytes=350)
        self.assertEqual(j4.delivery_lanes(self.con, self.claim()), [0,1,2])
        self.cfg.update(batch_rows=4, batch_bytes=1000)
        self.assertEqual(j4.delivery_lanes(self.con, self.claim(3)), [3,4])
        self.cfg["max_prepared_bytes"] = 1
        before = self.con.execute("SELECT COUNT(*) FROM job_assignments").fetchone()[0]
        self.assertIsNone(self.claim(5))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM job_assignments").fetchone()[0], before)

    def test_plan_versions_and_snapshot_heads_remain_separate(self):
        self.add(0, version=1)
        self.add(0, version=2, seq=2)
        self.add(1, version=2)
        self.add(2, version=1, kind="snapshot")
        self.add(2, version=1, seq=2)
        self.add(3, version=1)
        delivery = self.claim()
        self.assertEqual(j4.delivery_lanes(self.con, delivery), [0,3])
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM job_assignments WHERE delivery_id=?",
                                          (delivery,)).fetchone()[0], 2)
        self.assertIsNone(self.claim(2))
        self.assertIsNotNone(self.claim(1))

    def test_explicit_cap_and_writer_parallelism(self):
        for lane in range(16):
            self.add(lane)
        self.cfg["cdc_bundle_max_lanes"] = 4
        self.assertEqual(j4.delivery_lanes(self.con, self.claim()), [0,1,2,3])
        self.cfg["cdc_bundle_max_lanes"] = 16
        self.runtime["active_writers"]["sink"] = 4
        self.assertEqual(j4.delivery_lanes(self.con, self.claim(4)), [4,5,6,7])
        catalog = str(Path(self.temporary.name)/"catalog.sqlite3")
        cdc_catalog.execute_batch(catalog, ["SET VARIABLE CDC_CDC_BUNDLE_MAX_LANES = 4"])
        self.assertEqual(int(cdc_catalog.variables_get(catalog)["CDC_CDC_BUNDLE_MAX_LANES"]), 4)

    def test_wide_arrow_prepare_shrink_restart_and_visibility_fence(self):
        import json
        import pyarrow as pa

        self.cfg.update(batch_bytes=65536,max_row_bytes=65536,
                        max_prepared_bytes=1048576,compression="",
                        duckdb_memory="64MB",state=self.path)
        mapping = dict(src_table="sink",sr_table="sink",primary_key="id",
                       sql="SELECT id,v FROM arrow_batch",full_filter="v >= 0",
                       _schema=[("id",pa.int64()),("v",pa.int64())],
                       _target_sequence=False,_output_columns=["id","v"])
        j4.validate_mapping(mapping)
        engine = j4.transform_engine(self.cfg)
        self.addCleanup(engine.close)
        for lane in range(16):
            self.add(lane)
            payload = j4.arrow_job_payload(
                mapping,[(0,dict(id=lane,v=lane*10))],self.cfg,engine)
            self.con.execute(
                "UPDATE jobs SET payload=?,logical_bytes=? WHERE table_name='sink' AND lane=?",
                (payload,len(payload),lane))
        delivery = self.claim()
        self.assertEqual(j4.delivery_lanes(self.con,delivery),list(range(16)))
        self.assertTrue(j4.shrink_unprepared_delivery(self.con,delivery)["shrunk"])
        self.assertEqual(j4.delivery_lanes(self.con,delivery),[0])
        self.assertTrue(j4.prepare_delivery(self.con,engine,mapping,delivery,self.cfg))
        with self.assertRaisesRegex(RuntimeError,"invisible"):
            j4.acknowledge_delivery(self.con,delivery)
        self.con.execute("UPDATE load_parts SET visible=1 WHERE delivery_id=?",(delivery,))
        j4.acknowledge_delivery(self.con,delivery)
        delivery = self.claim(1)
        self.assertEqual(j4.delivery_lanes(self.con,delivery),list(range(1,16)))
        self.assertTrue(j4.prepare_delivery(self.con,engine,mapping,delivery,self.cfg))
        parts = self.con.execute(
            "SELECT payload FROM load_parts WHERE delivery_id=? ORDER BY part",(delivery,)).fetchall()
        rows = [json.loads(line) for payload, in parts for line in payload.splitlines()]
        self.assertEqual(sorted((r["id"],r["v"]) for r in rows),[(i,i*10) for i in range(1,16)])
        self.cfg['merge_visibility_pipeline']=True
        self.runtime.update(stop=threading.Event(),version_recovery={})
        self.con.execute("UPDATE load_parts SET txn_id=99 WHERE delivery_id=?",(delivery,))
        with patch.object(j4,'curl_request',side_effect=AssertionError('known payload resent')), \
             patch.object(j4,'wait_visible',return_value=('pending',{})) as poll:
            self.assertIsNone(j4.merge_async_delivery(None,self.con,mapping,delivery,self.cfg,self.runtime))
            self.assertEqual(poll.call_count,1)
        self.con.close()
        self.con = j4.open_state(self.path)
        self.assertEqual(j4.delivery_lanes(self.con,delivery),list(range(1,16)))
        with self.assertRaisesRegex(RuntimeError,"invisible"):
            j4.acknowledge_delivery(self.con,delivery)
        with patch.object(j4,'curl_request',side_effect=AssertionError('known payload resent')), \
             patch.object(j4,'wait_visible',return_value=('visible',{})) as poll:
            j4.merge_async_delivery(None,self.con,mapping,delivery,self.cfg,self.runtime)
            self.assertEqual(poll.call_count,1)
        j4.acknowledge_delivery(self.con,delivery)
        self.assertEqual({row["source_seq"] for row in j4.visible_frontiers(self.con,"sink")},{1})


if __name__ == "__main__":
    unittest.main()
