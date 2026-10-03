#!/usr/bin/env python3
"""Known remote visibility: local SQLite retry must never resubmit the load."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))
import j4
from source_apply_contention_test import wait_for


class MergeVisibleContentionTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path=str(Path(self.temporary.name)/"state.sqlite3")
        self.con=j4.init_state(self.path)
        self.addCleanup(self.con.close)
        self.delivery="delivery"
        self.mapping=dict(src_table="events",sr_table="events")
        self.con.execute("INSERT INTO deliveries(id,table_name,lane) VALUES('delivery','events',0)")
        for part in range(2):
            self.con.execute("INSERT INTO load_parts(delivery_id,part,label,payload,nrows,txn_id) "
                             "VALUES('delivery',?,?,X'00',1,99)",(part,"local-%d"%part))
        self.stop=threading.Event()

    def contention(self,cancel):
        visible,locked,busy=threading.Event(),threading.Event(),threading.Event()
        errors=[]
        original_wait=self.stop.wait
        def wait(timeout):
            busy.set()
            return original_wait(timeout)
        def remote_visible(*args):
            visible.set()
            if not locked.wait(5):
                raise AssertionError("test writer lock missing")
            return "visible",{}
        def worker():
            con=j4.open_state(self.path)
            con.execute("PRAGMA busy_timeout=10")
            try:
                j4.merge_async_delivery(None,con,self.mapping,self.delivery,{},dict(stop=self.stop))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        with patch.object(j4,"submit_merge_async",return_value=(99,{})) as submit, \
             patch.object(j4,"wait_visible",side_effect=remote_visible) as check, \
             patch.object(self.stop,"wait",side_effect=wait):
            thread=threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(visible.wait(5))
                self.con.execute("BEGIN IMMEDIATE")
                locked.set()
                wait_for(lambda:busy.is_set() or errors)
                self.assertEqual(errors,[])
                self.assertTrue(busy.is_set())
                self.assertEqual(self.con.execute("SELECT SUM(visible) FROM load_parts").fetchone()[0],0)
                if cancel:
                    self.stop.set()
                else:
                    self.con.rollback()
                thread.join(5)
                self.assertFalse(thread.is_alive())
                self.assertEqual(submit.call_count,2)
                self.assertEqual(check.call_count,1)
            finally:
                if self.con.in_transaction:
                    self.con.rollback()
                locked.set()
                self.stop.set()
                thread.join(5)
        if cancel:
            self.assertEqual(len(errors),1)
            self.assertIn("journal retained",str(errors[0]))
            self.assertEqual(self.con.execute("SELECT SUM(visible) FROM load_parts").fetchone()[0],0)
            self.stop.clear()
            # Real submit helper recovers the durable txn id without HTTP.
            with patch.object(j4,"wait_visible",return_value=("visible",{})) as check, \
                 patch.object(j4,"curl_request",side_effect=AssertionError("HTTP replay")):
                j4.merge_async_delivery(None,self.con,self.mapping,self.delivery,{},dict(stop=self.stop))
            self.assertEqual(check.call_count,1)
        else:
            self.assertEqual(errors,[])
        self.assertEqual(self.con.execute("SELECT part,label,txn_id,visible FROM load_parts ORDER BY part").fetchall(),
                         [(0,"local-0",99,1),(1,"local-1",99,1)])

    def test_known_visible_local_busy_resumes_without_remote_replay(self):
        self.contention(False)

    def test_stop_retains_journal_and_restart_recovers_without_http(self):
        self.contention(True)

    def test_non_busy_storage_errors_and_active_transaction_remain_fatal(self):
        for code,active in [(sqlite3.SQLITE_FULL,False),(sqlite3.SQLITE_CORRUPT,False),
                            (sqlite3.SQLITE_LOCKED,False),(sqlite3.SQLITE_BUSY,True)]:
            with self.subTest(code=code,active=active):
                exc=sqlite3.OperationalError("storage failure")
                exc.sqlite_errorcode=code
                if active:
                    self.con.execute("BEGIN IMMEDIATE")
                with patch.object(j4,"state_transaction",side_effect=exc), \
                     patch.object(self.stop,"wait") as wait:
                    with self.assertRaises(sqlite3.OperationalError):
                        j4.mark_merge_transaction_visible(self.con,self.delivery,99,self.stop)
                self.assertEqual(wait.call_count,0)
                if active:
                    self.con.rollback()
                self.assertEqual(self.con.execute("SELECT SUM(visible) FROM load_parts").fetchone()[0],0)


if __name__=="__main__":
    unittest.main()
