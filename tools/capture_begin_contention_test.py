#!/usr/bin/env python3
"""Retry BEGIN before consuming source spool; never retry body or COMMIT."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/"tools")]
import source_transaction_spool_test as fixture
from source_apply_contention_test import wait_for
import j4
import source_state


class ExecuteFault:
    def __init__(self,con,statement,code):
        self.con,self.statement,self.code=con,statement,code
        self.calls=[]

    @property
    def in_transaction(self):
        return self.con.in_transaction

    def execute(self,sql,*args):
        self.calls.append(sql)
        if sql==self.statement:
            exc=sqlite3.OperationalError("synthetic storage failure")
            exc.sqlite_errorcode=self.code
            raise exc
        return self.con.execute(sql,*args)


class CaptureBeginContentionTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"state.sqlite3")
        self.con=j4.init_state(self.path)
        self.addCleanup(self.con.close)
        source_state.register_relation(self.con,"mysql.events","source-epoch",
                                       fixture.pa.schema([fixture.pa.field("id",fixture.pa.int64()),
                                                          fixture.pa.field("value",fixture.pa.string())]),["id"])
        j4.meta_set(self.con,"read_position",("binlog.000001",4))
        self.stop=threading.Event()

    def contention(self,cancel):
        busy=threading.Event()
        errors=[]
        results=[]
        original_wait=self.stop.wait
        def wait(timeout):
            busy.set()
            return original_wait(timeout)
        def worker(source_spool,target_spool):
            con=j4.open_state(self.path)
            con.execute("PRAGMA busy_timeout=10")
            try:
                results.append(j4.commit_spool(con,target_spool,("binlog.000001",120),1.0,{},
                                              source_spool=source_spool,source_epoch="source-epoch",stop=self.stop))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        with tempfile.SpooledTemporaryFile(max_size=128) as source_spool, \
             tempfile.SpooledTemporaryFile(max_size=128) as target_spool:
            fixture.write_parts(source_spool)
            self.assertTrue(source_spool._rolled)
            source_spool.seek(0)
            expected=[]
            while True:
                part=j4.read_source_part_record(source_spool)
                if part is None:
                    break
                expected.append(part["payload"])
            size=source_spool.tell()
            self.con.execute("BEGIN IMMEDIATE")
            with patch.object(self.stop,"wait",side_effect=wait), \
                 patch.object(j4,"read_source_part_record",wraps=j4.read_source_part_record) as read:
                thread=threading.Thread(target=worker,args=(source_spool,target_spool))
                thread.start()
                try:
                    wait_for(lambda:busy.is_set() or errors)
                    self.assertEqual(errors,[])
                    self.assertEqual(read.call_count,0)
                    self.assertEqual(source_spool.tell(),size)
                    self.assertEqual(source_state.log_durable_seq(self.con),0)
                    self.assertEqual(j4.meta_get(self.con,"read_position"),("binlog.000001",4))
                    if cancel:
                        self.stop.set()
                    else:
                        self.con.rollback()
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                finally:
                    if self.con.in_transaction:
                        self.con.rollback()
                    self.stop.set()
                    thread.join(5)
            if cancel:
                self.assertEqual(len(errors),1)
                self.assertIn("durable journal retained",str(errors[0]))
                self.assertEqual(source_state.log_durable_seq(self.con),0)
                self.assertEqual(source_spool.tell(),size)
            else:
                self.assertEqual(errors,[])
                self.assertEqual(results,[set()])
                self.assertEqual(source_state.log_durable_seq(self.con),1)
                self.assertEqual(j4.meta_get(self.con,"read_position"),("binlog.000001",120))
                self.assertEqual([x[0] for x in self.con.execute("SELECT payload FROM source_commit_parts ORDER BY part")],expected)
                self.stop.clear()
                j4.commit_spool(self.con,target_spool,("binlog.000001",120),1.0,{},
                                source_spool=source_spool,source_epoch="source-epoch",stop=self.stop)
                self.assertEqual(source_state.log_durable_seq(self.con),1)
                self.assertEqual(self.con.execute("SELECT COUNT(*) FROM source_commits").fetchone()[0],1)

    def test_real_begin_busy_releases_then_consumes_sealed_spool_exactly_once(self):
        self.contention(False)

    def test_stop_before_body_retains_cursor_and_unconsumed_source_spool(self):
        self.contention(True)

    def test_default_busy_remains_fatal(self):
        self.con.execute("BEGIN IMMEDIATE")
        con=j4.open_state(self.path)
        con.execute("PRAGMA busy_timeout=10")
        try:
            with self.assertRaises(sqlite3.OperationalError):
                with j4.state_transaction(con):
                    self.fail("default BEGIN should not retry")
        finally:
            con.close()
            self.con.rollback()

    def test_body_commit_non_busy_and_active_errors_never_retry(self):
        exc=sqlite3.OperationalError("body busy")
        exc.sqlite_errorcode=sqlite3.SQLITE_BUSY
        with patch.object(self.stop,"wait") as wait:
            with self.assertRaises(sqlite3.OperationalError):
                with j4.state_transaction(self.con,stop=self.stop):
                    j4.meta_set(self.con,"uncommitted",1)
                    raise exc
            self.assertEqual(wait.call_count,0)
            self.assertIsNone(j4.meta_get(self.con,"uncommitted"))
            for statement,code,active in [("COMMIT",sqlite3.SQLITE_BUSY,False),
                                          ("BEGIN IMMEDIATE",sqlite3.SQLITE_FULL,False),
                                          ("BEGIN IMMEDIATE",sqlite3.SQLITE_CORRUPT,False),
                                          ("BEGIN IMMEDIATE",sqlite3.SQLITE_LOCKED,False),
                                          ("BEGIN IMMEDIATE",sqlite3.SQLITE_BUSY,True)]:
                with self.subTest(statement=statement,code=code,active=active):
                    if active:
                        self.con.execute("BEGIN IMMEDIATE")
                    proxy=ExecuteFault(self.con,statement,code)
                    bodies=[]
                    with self.assertRaises(sqlite3.OperationalError):
                        with j4.state_transaction(proxy,stop=self.stop):
                            bodies.append(1)
                            j4.meta_set(self.con,"uncommitted",1)
                    self.assertEqual(len(bodies),1 if statement=="COMMIT" else 0)
                    self.assertEqual(wait.call_count,0)
                    self.assertEqual(proxy.calls.count(statement),1)
                    if self.con.in_transaction:
                        self.con.rollback()
                    self.assertIsNone(j4.meta_get(self.con,"uncommitted"))


if __name__=="__main__":
    unittest.main()
