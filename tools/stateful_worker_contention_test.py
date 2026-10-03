#!/usr/bin/env python3
"""Real SQLite worker contention: durable resume, registry sync and stop."""
from decimal import Decimal
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
import aggregate_runtime_test as fixture
import aggregate_outbox
import aggregate_task_catalog
import j4
import source_state


def wait_for(predicate):
    deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("worker did not reach expected durable phase")


class StatefulContentionTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path=str(Path(self.temporary.name)/"state.sqlite3")
        self.con=j4.init_state(self.path)
        self.addCleanup(self.con.close)
        source_state.register_relation(self.con,"db.orders","source-1",fixture.source_schema(),["id"])
        source_state.stage_snapshot_batch(self.con,"db.orders",fixture.source_table([
            (1,"a",Decimal("10.00"),1),(2,"a",Decimal("5.00"),1)]),cursor=(2,),is_last=True)
        self.task=fixture.register_task(self.con,fixture.ir())
        self.item=dict(kind="aggregate",task=self.task,mapping=fixture.mapping(self.task))
        self.cfg=fixture.cfg(self.path)
        self.cfg["snapshot_rows"]=1
        self.runtime=dict(stop=threading.Event(),plan_lock=threading.RLock(),
                          error_lock=threading.Lock(),errors=[],
                          stateful_active_task_ids={self.task["task_id"]},
                          stateful_worker_threads={})
        self.open=j4.open_state
        self.step=j4.stateful_rebuild_guarded_step
        self.sync=j4.stateful_physical_registry.sync_runtime_result

    def short_timeout(self,path):
        con=self.open(path)
        con.execute("PRAGMA busy_timeout=10")
        return con

    def worker(self):
        thread=threading.Thread(target=j4.guarded_worker,
                                args=(j4.stateful_task_worker,self.runtime,self.item,self.cfg))
        thread.start()
        return thread

    def stop_worker(self,thread):
        if self.con.in_transaction:
            self.con.rollback()
        self.runtime["stop"].set()
        thread.join(5)
        self.assertFalse(thread.is_alive())

    def retries(self):
        return self.runtime.get("stateful_busy_retries",{}).get(self.task["task_id"],0)

    def contention(self,cancel=False):
        self.con.execute("BEGIN IMMEDIATE")
        with patch.object(j4,"open_state",side_effect=self.short_timeout), \
             patch.object(j4,"wake_loaders"):
            thread=self.worker()
            try:
                wait_for(self.retries)
                self.assertFalse(self.runtime["stop"].is_set())
                # A failed bootstrap transaction must not publish output.
                self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_output_commits").fetchone()[0],0)
                if cancel:
                    self.runtime["stop"].set()
                    thread.join(2)
                    self.assertFalse(thread.is_alive())
                else:
                    self.con.rollback()
                    wait_for(lambda:self.con.execute("SELECT COUNT(*) FROM aggregate_job_links").fetchone()[0]>0)
                    fixture.ack_all(self.con)
                    wait_for(lambda:aggregate_task_catalog.task_info(self.con,self.task["task_id"])["status"]=="active")
                    self.assertEqual(aggregate_outbox.visible_frontier(self.con,self.task["consumer_id"]),0)
                    self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_output_commits").fetchone()[0],1)
                    self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_output_rows").fetchone()[0],1)
                    self.assertEqual(self.con.execute("SELECT COUNT(*) FROM source_pins").fetchone()[0],0)
            finally:
                self.stop_worker(thread)
        self.assertEqual(self.runtime["errors"],[])

    def test_real_busy_rollback_and_exactly_once_bootstrap_resume(self):
        self.contention()

    def test_stop_interrupts_busy_backoff(self):
        self.contention(cancel=True)

    def test_registry_sync_retains_committed_result_without_restepping(self):
        entered,locked,synced=threading.Event(),threading.Event(),threading.Event()
        calls=[]
        def step(*args,**kwargs):
            calls.append(1)
            return self.step(*args,**kwargs)
        def sync(con,item,result):
            if not entered.is_set():
                entered.set()
                if not locked.wait(5):
                    raise RuntimeError("test registry lock missing")
            # A real write transaction at the post-step registry boundary.
            with j4.state_transaction(con):
                con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('test_registry',?)",(j4.pack(1),))
            synced.set()
            return self.sync(con,item,result)
        with patch.object(j4,"open_state",side_effect=self.short_timeout), \
             patch.object(j4,"wake_loaders"), \
             patch.object(j4,"stateful_rebuild_guarded_step",side_effect=step), \
             patch.object(j4.stateful_physical_registry,"sync_runtime_result",side_effect=sync):
            thread=self.worker()
            try:
                self.assertTrue(entered.wait(5))
                self.con.execute("BEGIN IMMEDIATE")
                locked.set()
                wait_for(self.retries)
                self.assertEqual(len(calls),1)
                self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_groups").fetchone()[0],1)
                self.con.rollback()
                self.assertTrue(synced.wait(5))
            finally:
                locked.set()
                self.stop_worker(thread)
        self.assertEqual(self.runtime["errors"],[])

    def test_non_busy_and_active_transaction_remain_fatal(self):
        for code,active in [(sqlite3.SQLITE_FULL,False),(sqlite3.SQLITE_CORRUPT,False),
                            (sqlite3.SQLITE_LOCKED,False),(sqlite3.SQLITE_BUSY,True)]:
            with self.subTest(code=code,active=active):
                def failure(con,*args,**kwargs):
                    if active:
                        con.execute("BEGIN IMMEDIATE")
                    exc=sqlite3.OperationalError("synthetic stateful storage failure")
                    exc.sqlite_errorcode=code
                    raise exc
                self.runtime["stop"].clear()
                self.runtime["errors"].clear()
                self.runtime.pop("stateful_busy_retries",None)
                with patch.object(j4,"stateful_rebuild_guarded_step",side_effect=failure):
                    thread=self.worker()
                    thread.join(5)
                self.assertFalse(thread.is_alive())
                self.assertTrue(self.runtime["stop"].is_set())
                self.assertEqual(len(self.runtime["errors"]),1)
                self.assertEqual(self.retries(),0)
                # Closing the failed connection rolled back its open transaction.
                self.con.execute("BEGIN IMMEDIATE")
                self.con.rollback()


if __name__=="__main__":
    unittest.main()
