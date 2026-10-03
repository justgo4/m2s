#!/usr/bin/env python3
"""Local pre-HTTP preparation resumes without replaying transform or transactions."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tools')]
import j4
import pyarrow as pa
from source_apply_contention_test import wait_for
from capture_begin_contention_test import ExecuteFault


class PrepareBeginTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/'state.sqlite3')
        self.con=j4.init_state(self.path)
        self.addCleanup(lambda:self.con.close())
        self.stop=threading.Event()
        self.mapping=dict(src_table='events',sr_table='events',primary_key='id',
                          _schema=[('id',pa.int64())],_output_columns=['id'],
                          _target_constraints={'id':dict()})
        raw=j4.raw_arrow(self.mapping,[(0,dict(id=7))])
        raw=raw.append_column("_sync_key",pa.array(["7"],type=pa.string()))
        raw=raw.append_column("_sync_lane",pa.array([0],type=pa.uint16()))
        wire=j4.arrow_table_payload(raw)
        self.con.execute("INSERT INTO jobs(table_name,lane,kind,payload,nrows,logical_bytes,created) "
                         "VALUES('events',0,'cdc',?,1,?,1)",(wire,len(wire)))
        self.con.execute("INSERT INTO deliveries(id,table_name,lane) VALUES('delivery','events',0)")
        self.con.execute("INSERT INTO job_assignments(job_id,delivery_id) VALUES(1,'delivery')")
        self.cfg=dict(state=self.path,batch_bytes=1024,max_row_bytes=65536,
                      max_prepared_bytes=65536,compression='none')

    def contend(self,phase,cancel=False):
        entered,locked,busy=threading.Event(),threading.Event(),threading.Event()
        errors=[]
        results=[]
        transforms=[]
        original_wait=self.stop.wait
        original_persist=j4.persist_field_overflows
        def wait(timeout):
            busy.set()
            return original_wait(timeout)
        def barrier():
            entered.set()
            if not locked.wait(5):
                raise AssertionError('test writer lock missing')
        def transform(*args,**kwargs):
            transforms.append(1)
            if phase=='overflow':
                barrier()
            yield pa.chunked_array([pa.array(['{"id":7}\n'])]),[]
        def persist(*args,**kwargs):
            result=original_persist(*args,**kwargs)
            if phase=='final':
                barrier()
            return result
        def worker():
            con=j4.open_state(self.path)
            con.execute('PRAGMA busy_timeout=10')
            try:
                results.append(j4.prepare_delivery(con,None,self.mapping,'delivery',self.cfg,stop=self.stop))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        with patch.object(self.stop,'wait',side_effect=wait), \
             patch.object(j4,'transformed_line_batches',side_effect=transform), \
             patch.object(j4,'persist_field_overflows',side_effect=persist), \
             patch.object(j4,'curl_request',side_effect=AssertionError('preparation must not send HTTP')):
            if phase=='initial':
                self.con.execute('BEGIN IMMEDIATE')
                locked.set()
            thread=threading.Thread(target=worker)
            thread.start()
            try:
                if phase!='initial':
                    self.assertTrue(entered.wait(5))
                    self.con.execute('BEGIN IMMEDIATE')
                    locked.set()
                wait_for(lambda:busy.is_set() or errors)
                self.assertEqual(errors,[])
                self.assertTrue(busy.is_set())
                self.assertEqual(self.con.execute("SELECT prepared FROM deliveries").fetchone()[0],0)
                self.assertEqual(self.con.execute('SELECT COUNT(*) FROM load_parts').fetchone()[0],0)
                if cancel:
                    self.stop.set()
                else:
                    self.con.rollback()
                thread.join(5)
                self.assertFalse(thread.is_alive())
            finally:
                if self.con.in_transaction:
                    self.con.rollback()
                locked.set()
                self.stop.set()
                thread.join(5)
        if cancel:
            self.assertEqual(len(errors),1)
            self.assertIn('journal retained',str(errors[0]))
            self.assertEqual(results,[])
            self.stop.clear()
            self.con.close()
            self.con=j4.open_state(self.path)
            with patch.object(j4,'transformed_line_batches',side_effect=transform), \
                 patch.object(j4,'curl_request',side_effect=AssertionError('HTTP replay')):
                self.assertTrue(j4.prepare_delivery(self.con,None,self.mapping,'delivery',self.cfg,stop=self.stop))
        else:
            self.assertEqual(errors,[])
            self.assertEqual(results,[True])
            self.assertEqual(len(transforms),1)
        self.assertEqual(self.con.execute('SELECT part,payload,nrows,json_bytes FROM load_parts').fetchall(),
                         [(0,b'{"id":7}\n',1,9)])
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM prepare_reservations').fetchone()[0],0)
        self.assertEqual(self.con.execute('SELECT prepared FROM deliveries').fetchone()[0],1)

    def test_initial_lock(self):
        self.contend('initial')

    def test_overflow_evidence_lock_keeps_transform(self):
        self.contend('overflow')

    def test_final_parts_lock_keeps_transform(self):
        self.contend('final')

    def test_cancel_retains_unprepared_journal_and_reopen_recovers(self):
        self.contend('final',True)

    def test_non_busy_and_body_errors_execute_once(self):
        for code in (sqlite3.SQLITE_FULL,sqlite3.SQLITE_CORRUPT,sqlite3.SQLITE_LOCKED):
            failed=ExecuteFault(self.con,'BEGIN IMMEDIATE',code)
            with self.assertRaises(sqlite3.OperationalError):
                j4.prepare_delivery(failed,None,self.mapping,'delivery',self.cfg,stop=self.stop)
            self.assertEqual(failed.calls.count('BEGIN IMMEDIATE'),1)
            self.assertFalse(self.con.in_transaction)
        failed=ExecuteFault(self.con,'DELETE FROM load_parts WHERE delivery_id=?',sqlite3.SQLITE_BUSY)
        with self.assertRaises(sqlite3.OperationalError):
            j4.prepare_delivery(failed,None,self.mapping,'delivery',self.cfg,stop=self.stop)
        self.assertEqual(failed.calls.count('BEGIN IMMEDIATE'),1)
        self.assertFalse(self.con.in_transaction)


if __name__=='__main__':
    unittest.main()
