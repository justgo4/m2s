#!/usr/bin/env python3
"""Real SQLite BUSY, catalog retry, cancellation and fail-closed source apply."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))
import j4
import physical_state_catalog
import source_apply_worker_test as fixture
import source_state


def wait_for(predicate):
    deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("contention test did not reach expected phase")


class ApplyContentionTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path=str(Path(self.temporary.name)/"state.sqlite3")
        self.con=j4.init_state(self.path)
        self.addCleanup(self.con.close)
        source_state.register_relation(self.con,"db.events","epoch",fixture.schema(),["id"])
        source_state.stage_snapshot_batch(self.con,"db.events",fixture.batch(0,0),None,is_last=True)
        part=source_state.prepare_part("db.events",fixture.batch(0,3))
        source_state.log_commit(self.con,"epoch",("binlog.000001",100),None,[part])
        self.runtime=dict(stop=threading.Event(),source_apply_event=threading.Event(),
                          error_lock=threading.Lock(),errors=[])
        self.open=j4.open_state
        self.sync=j4.sync_source_base_catalog

    def short_timeout(self,path):
        con=self.open(path)
        con.execute("PRAGMA busy_timeout=10")
        return con

    def worker(self):
        return threading.Thread(target=j4.guarded_worker,
                                args=(j4.source_state_apply_worker,self.runtime,dict(state=self.path)))

    def contention(self,stop_while_busy):
        entered,locked,synced=threading.Event(),threading.Event(),threading.Event()

        def sync(con):
            if not entered.is_set():
                entered.set()
                if not locked.wait(5):
                    raise RuntimeError("lock was not acquired")
            result=self.sync(con)
            synced.set()
            return result

        with patch.object(j4,"open_state",side_effect=self.short_timeout), \
             patch.object(j4,"sync_source_base_catalog",side_effect=sync):
            thread=self.worker()
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(source_state.base_applied_seq(self.con),1)
                self.con.execute("BEGIN IMMEDIATE")
                locked.set()
                wait_for(lambda:self.runtime.get("source_apply_busy_retries",0)>0)
                self.assertTrue(thread.is_alive())
                self.assertFalse(self.runtime["stop"].is_set())
                if stop_while_busy:
                    self.runtime["stop"].set()
                    thread.join(2)
                    self.assertFalse(thread.is_alive())
                self.con.execute("ROLLBACK")
                if not stop_while_busy:
                    self.assertTrue(synced.wait(5))
                    self.assertEqual(physical_state_catalog.status(self.con)[0]["watermark"],1)
                    self.assertEqual(source_state.log_durable_seq(self.con),1)
                    self.assertEqual(source_state.base_applied_seq(self.con),1)
                    self.assertEqual(self.con.execute("SELECT COUNT(*) FROM source_versions WHERE valid_to IS NULL").fetchone()[0],3)
                    self.assertEqual(source_state.apply_pending_bytes(self.con),0)
            finally:
                if self.con.in_transaction:
                    self.con.rollback()
                locked.set()
                self.runtime["stop"].set()
                self.runtime["source_apply_event"].set()
                thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(self.runtime["errors"],[])

    def test_applied_prefix_and_catalog_sync_retry_without_new_commit(self):
        self.contention(False)

    def test_cancel_during_real_busy_retry(self):
        self.contention(True)

    def test_non_busy_sqlite_error_is_still_fatal(self):
        exc=sqlite3.OperationalError("synthetic disk full")
        exc.sqlite_errorcode=sqlite3.SQLITE_FULL
        with patch.object(j4,"sync_source_base_catalog",side_effect=exc):
            thread=self.worker()
            thread.start()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.runtime["stop"].is_set())
        self.assertEqual(self.runtime["errors"],[("source_state_apply_worker","synthetic disk full")])
        self.assertEqual(self.runtime.get("source_apply_busy_retries",0),0)

    def test_busy_with_open_transaction_is_not_retried(self):
        def unsafe_busy(con):
            con.execute("BEGIN IMMEDIATE")
            j4.meta_set(con,"synthetic_uncommitted",1)
            exc=sqlite3.OperationalError("synthetic open transaction")
            exc.sqlite_errorcode=sqlite3.SQLITE_BUSY
            raise exc
        with patch.object(j4,"sync_source_base_catalog",side_effect=unsafe_busy):
            thread=self.worker()
            thread.start()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.runtime["stop"].is_set())
        self.assertEqual(self.runtime.get("source_apply_busy_retries",0),0)
        self.assertIsNone(j4.meta_get(self.con,"synthetic_uncommitted"))


if __name__=="__main__":
    unittest.main()
