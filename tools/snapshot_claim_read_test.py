#!/usr/bin/env python3
"""Read-only claim decisions and fresh-claim rechecks on real SQLite WAL."""
import contextlib
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import j4
import source_state
import cdc_bundle_test as fixture


class SnapshotClaimReadTest(unittest.TestCase):
    setUp=fixture.CdcBundleTest.setUp
    def add(self,lane,kind="snapshot",**kwargs):
        fixture.CdcBundleTest.add(self,lane,kind=kind,**kwargs)
        if kind=="snapshot":
            self.con.execute("UPDATE jobs SET group_id='group' WHERE id=last_insert_rowid()")

    def claim(self,lane=0):
        return j4.claim_snapshot_bundle(self.con,"sink",lane,self.cfg,self.runtime)

    @contextlib.contextmanager
    def competing_writer(self):
        writer=j4.open_state(self.path)
        self.con.execute('PRAGMA busy_timeout=20')
        writer.execute('BEGIN IMMEDIATE')
        try:
            yield writer
        finally:
            writer.rollback()
            writer.close()

    def test_idle_and_cdc_head_require_no_writer(self):
        with self.competing_writer():
            self.assertIsNone(self.claim())
            self.assertFalse(self.con.in_transaction)
        self.add(0,kind='cdc')
        self.add(0,kind='cdc',seq=2)
        with self.competing_writer():
            self.assertIsNone(self.claim())
            self.assertFalse(self.con.in_transaction)
        self.assertEqual(self.con.execute('SELECT count(*) FROM deliveries').fetchone()[0],0)

    def test_missing_group_and_caller_owned_read_snapshot(self):
        self.add(0)
        self.con.execute("UPDATE jobs SET group_id=NULL")
        with self.competing_writer():
            self.assertIsNone(self.claim())
        with source_state.read_snapshot(self.con):
            self.assertIsNone(self.claim())
            self.assertTrue(self.con.in_transaction)
        self.assertFalse(self.con.in_transaction)

    def test_existing_and_member_ownership_survive_restart_without_writer(self):
        for lane in range(8):
            self.add(lane)
        delivery=self.claim()
        self.con.close()
        self.con=j4.open_state(self.path)
        with self.competing_writer():
            self.assertEqual(self.claim(),delivery)
            for lane in range(1,8):
                self.assertIsNone(self.claim(lane))
            self.assertEqual(j4.delivery_lanes(self.con,delivery),list(range(8)))
            self.assertFalse(self.con.in_transaction)
        with self.assertRaises(RuntimeError):
            j4.acknowledge_delivery(self.con,delivery)

    def test_resource_rejection_is_read_only_but_new_claim_needs_writer(self):
        self.add(0)
        for budget in ('max_inflight_deliveries','max_prepared_bytes'):
            with self.subTest(budget=budget):
                saved=self.cfg[budget]
                self.cfg[budget]=0
                try:
                    with self.competing_writer():
                        self.assertIsNone(self.claim())
                        self.assertFalse(self.con.in_transaction)
                finally:
                    self.cfg[budget]=saved
        with self.competing_writer():
            with self.assertRaises(sqlite3.OperationalError):
                self.claim()
            self.assertFalse(self.con.in_transaction)
        self.assertIsNotNone(self.claim())

    def test_fresh_claim_rechecks_concurrent_assignment_after_read_snapshot(self):
        for lane in range(8):
            self.add(lane)
        writer=j4.open_state(self.path)
        self.addCleanup(writer.close)
        published=[]
        original=source_state.read_snapshot
        @contextlib.contextmanager
        def publish_after_read(connection):
            with original(connection):
                yield
            if connection is self.con and not published:
                published.append(j4.claim_snapshot_bundle(writer,'sink',0,self.cfg,self.runtime))
        with patch.object(source_state,'read_snapshot',side_effect=publish_after_read):
            delivery=self.claim()
        self.assertEqual(published,[delivery])
        self.assertEqual(self.con.execute('SELECT count(*) FROM deliveries').fetchone()[0],1)
        self.assertEqual(j4.delivery_lanes(self.con,delivery),list(range(8)))
        self.assertFalse(self.con.in_transaction)

    def test_resource_budget_is_rechecked_after_concurrent_other_lane_claim(self):
        self.cfg.update(max_inflight_deliveries=1,snapshot_bundle_max_lanes=1)
        self.add(0)
        self.add(1)
        writer=j4.open_state(self.path)
        self.addCleanup(writer.close)
        published=[]
        original=source_state.read_snapshot
        @contextlib.contextmanager
        def publish_after_read(connection):
            with original(connection):
                yield
            if connection is self.con and not published:
                published.append(j4.claim_snapshot_bundle(writer,'sink',1,self.cfg,self.runtime))
        with patch.object(source_state,'read_snapshot',side_effect=publish_after_read):
            self.assertIsNone(self.claim())
        self.assertEqual(j4.delivery_lanes(self.con,published[0]),[1])
        self.assertEqual(self.con.execute('SELECT count(*) FROM deliveries').fetchone()[0],1)
        self.assertEqual(self.con.execute("SELECT count(*) FROM active_jobs j JOIN job_assignments a ON a.job_id=j.id WHERE j.lane=0").fetchone()[0],0)
        self.assertFalse(self.con.in_transaction)


if __name__=='__main__':
    unittest.main()
