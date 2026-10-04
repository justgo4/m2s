#!/usr/bin/env python3
"""FIFO explicit writers preserve real SQLite timeout/recovery semantics."""
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import j4
import source_state
import sqlite_writer as fair
import sqlite_write_timing as timing


class FairTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=str(Path(self.temp.name)/'state.sqlite3')
        self.con=self.connect()
        self.addCleanup(self.con.close)
        self.con.execute('CREATE TABLE rows(id INTEGER PRIMARY KEY)')

    def connect(self,path=None,factory=fair.FairConnection,timeout=1000):
        con=sqlite3.connect(path or self.path,isolation_level=None,factory=factory)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA busy_timeout='+str(timeout))
        return con

    def wait_queued(self,count):
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            with fair._LOCK:
                queue=fair._QUEUES.get(os.path.realpath(self.path))
            if queue is not None:
                with queue.condition:
                    if len(queue.waiters)==count:
                        return
            threading.Event().wait(0.001)
        self.fail('writer did not queue')

    def test_fifo_ack_cannot_be_overtaken_by_bootstrap_reacquisition(self):
        order=[]; errors=[]; prepared=[threading.Event() for _ in range(3)]
        start=[threading.Event() for _ in range(3)]
        def writer(index):
            con=self.connect(timeout=5000)
            try:
                prepared[index].set(); start[index].wait(5)
                with source_state.transaction(con):
                    order.append(index)
                    con.execute('INSERT INTO rows VALUES(?)',(index,))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        threads=[threading.Thread(target=writer,args=(i,)) for i in range(3)]
        for thread in threads:thread.start()
        try:
            for ready in prepared:self.assertTrue(ready.wait(5))
            self.con.execute('BEGIN IMMEDIATE')
            for i in range(3):
                start[i].set(); self.wait_queued(i+1)
            self.con.commit()
            # The just-finished bootstrap queues behind all existing requests.
            with source_state.transaction(self.con):
                order.append(3); self.con.execute('INSERT INTO rows VALUES(3)')
            for thread in threads:thread.join(5)
            self.assertEqual(errors,[])
            self.assertEqual(order,[0,1,2,3])
            self.assertEqual(self.con.execute('SELECT COUNT(*) FROM rows').fetchone(),(4,))
            self.assertEqual(fair._QUEUES,{})
        finally:
            if self.con.in_transaction:self.con.rollback()
            for event in start:event.set()
            for thread in threads:thread.join(5)

    def test_queue_timeout_busy_code_does_not_begin_or_leak(self):
        other=self.connect(timeout=20)
        try:
            self.con.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.OperationalError) as caught:
                other.execute('BEGIN IMMEDIATE')
            self.assertEqual(caught.exception.sqlite_errorcode,sqlite3.SQLITE_BUSY)
            self.assertFalse(other.in_transaction)
            self.assertIsNone(other._fair_queue)
            self.con.rollback()
            other.execute('BEGIN IMMEDIATE'); other.commit()
            self.assertEqual(other.execute('PRAGMA busy_timeout').fetchone(),(20,))
            self.assertEqual(fair._QUEUES,{})
        finally:other.close()

    def test_external_writer_still_uses_sqlite_busy_and_retry(self):
        external=self.connect(factory=sqlite3.Connection)
        other=self.connect(timeout=20)
        try:
            external.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.OperationalError) as caught:
                other.execute('BEGIN IMMEDIATE')
            self.assertEqual(caught.exception.sqlite_errorcode&255,sqlite3.SQLITE_BUSY)
            self.assertFalse(other.in_transaction)
            self.assertEqual(fair._QUEUES,{})
            external.rollback()
            other.execute('BEGIN IMMEDIATE'); other.commit()
        finally:external.close();other.close()

    def test_queue_and_external_wait_share_existing_timeout_budget(self):
        ready=threading.Event();start=threading.Event();results=[]
        external=self.connect(factory=sqlite3.Connection)
        key=os.path.realpath(self.path);slot=fair.acquire(key,1)
        def writer():
            con=self.connect(timeout=100)
            try:
                ready.set();start.wait(5);began=time.monotonic()
                with self.assertRaises(sqlite3.OperationalError):con.execute('BEGIN IMMEDIATE')
                results.append((time.monotonic()-began,con.in_transaction,
                    con.execute('PRAGMA busy_timeout').fetchone()[0]))
            finally:con.close()
        thread=threading.Thread(target=writer);thread.start()
        try:
            self.assertTrue(ready.wait(5));external.execute('BEGIN IMMEDIATE')
            start.set();self.wait_queued(1)
            threading.Event().wait(0.06)
            fair.release(key,slot);slot=None
            thread.join(5);self.assertFalse(thread.is_alive())
            self.assertEqual(len(results),1)
            seconds,active,timeout=results[0]
            self.assertGreater(seconds,0.08);self.assertLess(seconds,0.14)
            self.assertFalse(active);self.assertEqual(timeout,100)
            self.assertEqual(fair._QUEUES,{})
        finally:
            if slot is not None:fair.release(key,slot)
            start.set();external.rollback();external.close();thread.join(5)

    def test_failed_commit_nested_begin_and_body_error_retain_holder(self):
        self.con.execute('PRAGMA foreign_keys=ON')
        self.con.execute('CREATE TABLE child(id REFERENCES rows(id) DEFERRABLE INITIALLY DEFERRED)')
        self.con.execute('BEGIN IMMEDIATE')
        self.con.execute('INSERT INTO child VALUES(99)')
        for sql in ['COMMIT','BEGIN IMMEDIATE','INSERT INTO absent VALUES(1)']:
            with self.assertRaises(sqlite3.DatabaseError):self.con.execute(sql)
            self.assertTrue(self.con.in_transaction)
            self.assertIsNotNone(self.con._fair_queue)
        self.con.rollback()
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM child').fetchone(),(0,))
        self.assertEqual(fair._QUEUES,{})

    def test_automatic_rollback_close_scripts_context_and_reopen_release(self):
        self.con.execute('INSERT INTO rows VALUES(1)')
        self.con.execute('BEGIN IMMEDIATE')
        with self.assertRaises(sqlite3.IntegrityError):
            self.con.execute('INSERT OR ROLLBACK INTO rows VALUES(1)')
        self.assertFalse(self.con.in_transaction); self.assertEqual(fair._QUEUES,{})
        self.con.execute('BEGIN IMMEDIATE')
        self.con.executescript('INSERT INTO rows VALUES(2);')
        self.assertEqual(fair._QUEUES,{})
        with self.con:
            self.con.execute('BEGIN EXCLUSIVE TRANSACTION')
            self.con.execute('INSERT INTO rows VALUES(3)')
        self.assertEqual(fair._QUEUES,{})
        self.con.execute('BEGIN IMMEDIATE'); self.con.execute('INSERT INTO rows VALUES(4)')
        self.con.close()
        self.assertEqual(fair._QUEUES,{})
        reopened=self.connect()
        try:
            reopened.execute('BEGIN IMMEDIATE');reopened.rollback()
            self.assertEqual(reopened.execute('SELECT id FROM rows ORDER BY id').fetchall(),[(1,),(2,),(3,)])
        finally:reopened.close()

    def test_wal_reader_independent_file_and_memory_do_not_queue(self):
        reader=self.connect()
        other=self.connect(path=str(Path(self.temp.name)/'other.sqlite3'))
        mem1=self.connect(path=':memory:');mem2=self.connect(path=':memory:')
        try:
            self.con.execute('BEGIN IMMEDIATE')
            with source_state.read_snapshot(reader):
                self.assertEqual(reader.execute('SELECT COUNT(*) FROM rows').fetchone(),(0,))
            for con in [other,mem1,mem2]:con.execute('BEGIN IMMEDIATE')
            for con in [other,mem1,mem2]:con.rollback()
            self.con.rollback();self.assertEqual(fair._QUEUES,{})
        finally:reader.close();other.close();mem1.close();mem2.close()

    def test_path_alias_and_timing_factory_use_same_admission(self):
        alias=Path(self.temp.name)/'alias.sqlite3';alias.symlink_to(self.path)
        collector=timing.Collector()
        other=self.connect(path=str(alias),factory=lambda *a,**kw:timing.TimingConnection(*a,collector=collector,**kw),timeout=20)
        try:
            self.con.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.OperationalError):other.execute('BEGIN IMMEDIATE')
            self.assertTrue(any(row['phase']=='acquire' and row['outcome']=='busy' for row in collector.snapshot()['operations']))
            self.con.rollback();other.execute('BEGIN IMMEDIATE');other.commit()
            self.assertEqual(collector.snapshot()['active'],[])
            self.assertEqual(fair._QUEUES,{})
        finally:other.close()

    def test_open_state_factory_is_fair_with_and_without_profiler(self):
        for value in ['0','1']:
            with patch.dict(os.environ,CDC_SQLITE_WRITE_TIMING=value):
                con=j4.open_state(self.path)
            try:
                self.assertIsInstance(con,fair.FairConnection)
                with j4.state_transaction(con):con.execute('SELECT 1')
            finally:con.close()
        self.assertEqual(fair._QUEUES,{})


if __name__=='__main__':unittest.main()
