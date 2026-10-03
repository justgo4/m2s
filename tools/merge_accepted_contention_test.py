#!/usr/bin/env python3
"""Accepted HTTP identity: BEGIN-only local retry, never a second upload."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'tools'))
import j4
from source_apply_contention_test import wait_for


class MergeAcceptedContentionTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path=str(Path(self.temporary.name)/'state.sqlite3')
        self.con=j4.init_state(self.path)
        self.addCleanup(self.con.close)
        self.mapping=dict(src_table='events',sr_table='events',
                          _target_sequence=False,_output_columns=['id'])
        self.cfg=dict(load_timeout=30,merge_commit_interval_ms=100,
                      merge_commit_parallel=1,compression='',
                      sr=dict(host='127.0.0.1',http_port=8030,database='synthetic'))
        self.con.execute("INSERT INTO deliveries(id,table_name,lane) VALUES('delivery','events',0)")
        self.con.execute("INSERT INTO load_parts(delivery_id,part,label,payload,nrows) "
                         "VALUES('delivery',0,'local-label',?,1)",(b'{"id":1,"__op":0}\n',))
        self.stop=threading.Event()
        self.runtime=dict(stop=self.stop,control_lock=threading.Lock(),
                          quarantined_tables={},version_recovery={})
        self.result=dict(Status='Success',TxnId=99,Label='remote-label')

    def submit(self,con=None):
        return j4.submit_merge_async(None,con or self.con,self.mapping,
                                    'delivery',0,self.cfg,self.stop,self.runtime)

    def marker(self):
        return self.con.execute("SELECT reason FROM merge_uncertain").fetchall()

    def contention(self,cancel):
        response,locked,busy=threading.Event(),threading.Event(),threading.Event()
        errors=[]
        original_wait=self.stop.wait
        def wait(timeout):
            busy.set()
            return original_wait(timeout)
        def accepted(*args):
            response.set()
            if not locked.wait(5):
                raise AssertionError('test writer lock missing')
            return 200,self.result
        def worker():
            con=j4.open_state(self.path)
            con.execute('PRAGMA busy_timeout=10')
            try:
                self.assertEqual(self.submit(con),(99,self.result))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        with patch.object(j4,'curl_request',side_effect=accepted) as send, \
             patch.object(self.stop,'wait',side_effect=wait):
            thread=threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(response.wait(5))
                self.assertEqual(self.marker(),[('request_inflight_no_txn_id',)])
                self.con.execute('BEGIN IMMEDIATE')
                locked.set()
                wait_for(lambda:busy.is_set() or errors)
                self.assertEqual(errors,[])
                self.assertTrue(busy.is_set())
                if cancel:
                    self.stop.set()
                else:
                    self.con.rollback()
                thread.join(5)
                self.assertFalse(thread.is_alive())
                self.assertEqual(send.call_count,1)
            finally:
                if self.con.in_transaction:
                    self.con.rollback()
                self.stop.set()
                locked.set()
                thread.join(5)
        self.stop.clear()
        if cancel:
            self.assertEqual(len(errors),1)
            self.assertIn('journal retained',str(errors[0]))
            self.assertEqual(self.marker(),[('request_inflight_no_txn_id',)])
            self.assertEqual(self.con.execute('SELECT txn_id FROM load_parts').fetchone(),(None,))
            restored=j4.open_state(self.path)
            try:
                with redirect_stdout(io.StringIO()), \
                     patch.object(j4,'curl_request',side_effect=AssertionError('HTTP replay')) as send:
                    self.assertEqual(j4.quarantine_pending_merges(restored,self.runtime),1)
                    self.assertTrue(j4.merge_table_quarantined(self.runtime,'events'))
                    self.assertEqual(send.call_count,0)
            finally:
                restored.close()
        else:
            self.assertEqual(errors,[])
            self.assertEqual(self.marker(),[])
            with patch.object(j4,'curl_request',side_effect=AssertionError('HTTP replay')):
                self.assertEqual(self.submit(),(99,dict(Status='LOCAL_PENDING',TxnId=99)))

    def request_contention(self,cancel):
        busy=threading.Event()
        errors=[]
        results=[]
        original_wait=self.stop.wait
        def wait(timeout):
            busy.set()
            return original_wait(timeout)
        def worker():
            con=j4.open_state(self.path)
            con.execute('PRAGMA busy_timeout=10')
            try:
                results.append(self.submit(con))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        def accepted(*args):
            check=j4.open_state(self.path)
            try:
                self.assertEqual(check.execute('SELECT reason FROM merge_uncertain').fetchall(),
                                 [('request_inflight_no_txn_id',)])
            finally:
                check.close()
            return 200,self.result
        # Open the second WAL connection before holding the writer lock.
        started=threading.Event()
        original_open=j4.open_state
        def opened(path):
            con=original_open(path)
            started.set()
            locked.wait(5)
            return con
        locked=threading.Event()
        with patch.object(j4,'open_state',side_effect=opened), \
             patch.object(j4,'curl_request',side_effect=accepted) as send, \
             patch.object(self.stop,'wait',side_effect=wait):
            thread=threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(started.wait(5))
                self.con.execute('BEGIN IMMEDIATE')
                locked.set()
                wait_for(lambda:busy.is_set() or errors)
                self.assertEqual(errors,[])
                self.assertTrue(busy.is_set())
                self.assertEqual(send.call_count,0)
                self.assertEqual(self.marker(),[])
                if cancel:
                    self.stop.set()
                else:
                    self.con.rollback()
                thread.join(5)
                self.assertFalse(thread.is_alive())
            finally:
                self.stop.set()
                locked.set()
                if self.con.in_transaction:
                    self.con.rollback()
                thread.join(5)
        self.stop.clear()
        if cancel:
            self.assertEqual(len(errors),1)
            self.assertIn('journal retained',str(errors[0]))
            self.assertEqual(send.call_count,0)
            self.assertEqual(self.marker(),[])
            restored=original_open(self.path)
            try:
                with patch.object(j4,'curl_request',side_effect=accepted) as restarted:
                    self.assertEqual(self.submit(restored),(99,self.result))
                    self.assertEqual(restarted.call_count,1)
            finally:
                restored.close()
        else:
            self.assertEqual(errors,[])
            self.assertEqual(results,[(99,self.result)])
            self.assertEqual(send.call_count,1)
        self.assertEqual(self.marker(),[])

    def test_request_begin_busy_waits_before_single_http_send(self):
        self.request_contention(False)

    def test_request_begin_cancel_does_not_send_or_quarantine_unsent_payload(self):
        self.request_contention(True)

    def test_request_intent_body_failure_never_sends(self):
        self.con.execute("CREATE TRIGGER reject_intent BEFORE INSERT ON merge_uncertain "
                         "BEGIN SELECT RAISE(ABORT,'synthetic intent failure'); END")
        with patch.object(j4,'curl_request') as send, patch.object(self.stop,'wait') as wait:
            with self.assertRaises(sqlite3.IntegrityError):
                self.submit()
        self.assertEqual(send.call_count,0)
        self.assertEqual(wait.call_count,0)
        self.assertEqual(self.marker(),[])
        self.assertFalse(self.con.in_transaction)

    def test_request_fatal_begin_or_active_transaction_never_sends(self):
        for code,active in [(sqlite3.SQLITE_FULL,False),(sqlite3.SQLITE_CORRUPT,False),
                            (sqlite3.SQLITE_LOCKED,False),(sqlite3.SQLITE_BUSY,True)]:
            with self.subTest(code=code,active=active):
                con=self.con
                class FaultConnection:
                    @property
                    def in_transaction(self):
                        return active
                    def execute(self,sql,*args):
                        if sql=='BEGIN IMMEDIATE':
                            exc=sqlite3.OperationalError('synthetic intent begin failure')
                            exc.sqlite_errorcode=code
                            raise exc
                        return con.execute(sql,*args)
                with patch.object(j4,'curl_request') as send, patch.object(self.stop,'wait') as wait:
                    with self.assertRaises(sqlite3.OperationalError):
                        self.submit(FaultConnection())
                self.assertEqual(send.call_count,0)
                self.assertEqual(wait.call_count,0)
                self.assertEqual(self.marker(),[])

    def test_request_intent_commit_busy_never_sends_or_retries_body(self):
        con=self.con
        statements=[]
        class FaultConnection:
            @property
            def in_transaction(self):
                return con.in_transaction
            def execute(self,sql,*args):
                statements.append(sql)
                if sql=='COMMIT':
                    exc=sqlite3.OperationalError('synthetic intent commit busy')
                    exc.sqlite_errorcode=sqlite3.SQLITE_BUSY
                    raise exc
                return con.execute(sql,*args)
        with patch.object(j4,'curl_request') as send, patch.object(self.stop,'wait') as wait:
            with self.assertRaises(sqlite3.OperationalError):
                self.submit(FaultConnection())
        self.assertEqual(send.call_count,0)
        self.assertEqual(wait.call_count,0)
        self.assertEqual(statements.count('BEGIN IMMEDIATE'),1)
        self.assertEqual(statements.count('COMMIT'),1)
        self.assertEqual(self.marker(),[])
        self.assertFalse(con.in_transaction)

    def test_local_begin_busy_records_identity_without_http_replay(self):
        self.contention(False)

    def test_cancel_keeps_unknown_marker_and_restart_quarantines(self):
        self.contention(True)

    def test_shutdown_drains_success_when_lock_available(self):
        def accepted(*args):
            self.stop.set()
            return 200,self.result
        with patch.object(j4,'curl_request',side_effect=accepted) as send:
            self.assertEqual(self.submit(),(99,self.result))
        self.assertEqual(send.call_count,1)
        self.assertEqual(self.marker(),[])

    def test_body_failure_is_rolled_back_once_and_never_resubmitted(self):
        self.con.execute("CREATE TRIGGER reject_identity BEFORE UPDATE OF txn_id ON load_parts "
                         "BEGIN SELECT RAISE(ABORT,'synthetic body failure'); END")
        with patch.object(j4,'curl_request',return_value=(200,self.result)) as send:
            with self.assertRaises(sqlite3.IntegrityError):
                self.submit()
        self.assertEqual(send.call_count,1)
        self.assertEqual(self.marker(),[('request_inflight_no_txn_id',)])
        self.assertEqual(self.con.execute('SELECT txn_id FROM load_parts').fetchone(),(None,))
        self.assertFalse(self.con.in_transaction)

    def test_commit_busy_is_not_retried(self):
        # WAL COMMIT contention is not reproducible with a reader: inject exactly
        # this storage boundary while the real body and rollback use SQLite.
        con=self.con
        class FaultConnection:
            @property
            def in_transaction(self):
                return con.in_transaction
            def execute(self,sql,*args):
                if sql=='COMMIT':
                    exc=sqlite3.OperationalError('synthetic commit busy')
                    exc.sqlite_errorcode=sqlite3.SQLITE_BUSY
                    raise exc
                return con.execute(sql,*args)
        payload=self.con.execute('SELECT payload FROM load_parts').fetchone()[0]
        j4.begin_merge_request(self.con,self.mapping,'delivery',0,'local-label',payload)
        with patch.object(self.stop,'wait') as wait:
            with self.assertRaises(sqlite3.OperationalError):
                j4.record_merge_acceptance(FaultConnection(),'delivery',0,99,self.stop)
        self.assertEqual(wait.call_count,0)
        self.assertEqual(self.marker(),[('request_inflight_no_txn_id',)])
        self.assertEqual(self.con.execute('SELECT txn_id FROM load_parts').fetchone(),(None,))

    def test_fatal_begin_or_active_transaction_never_retries(self):
        for code,active in [(sqlite3.SQLITE_FULL,False),(sqlite3.SQLITE_CORRUPT,False),
                            (sqlite3.SQLITE_LOCKED,False),(sqlite3.SQLITE_BUSY,True)]:
            with self.subTest(code=code,active=active):
                con=self.con
                class FaultConnection:
                    @property
                    def in_transaction(self):
                        return active
                    def execute(self,*args):
                        exc=sqlite3.OperationalError('synthetic begin failure')
                        exc.sqlite_errorcode=code
                        raise exc
                with patch.object(self.stop,'wait') as wait:
                    with self.assertRaises(sqlite3.OperationalError):
                        j4.record_merge_acceptance(FaultConnection(),'delivery',0,99,self.stop)
                self.assertEqual(wait.call_count,0)


if __name__=='__main__':
    unittest.main()
