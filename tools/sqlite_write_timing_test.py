#!/usr/bin/env python3
"""Real SQLite transaction timing without SQL or data in observations."""
from pathlib import Path
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sqlite_write_timing as timing


class TimingTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=str(Path(self.temp.name)/'private-state.sqlite3')
        self.collector=timing.Collector()
        self.con=self.connect()
        self.addCleanup(self.con.close)
        self.con.execute('CREATE TABLE private_table(value TEXT)')

    def connect(self):
        con=sqlite3.connect(self.path,isolation_level=None,
            factory=lambda *args,**kw:timing.TimingConnection(*args,collector=self.collector,**kw))
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA busy_timeout=20')
        return con

    def phase(self,phase,outcome):
        return [row for row in self.collector.snapshot()['operations']
                if row['phase']==phase and row['outcome']==outcome]

    def test_real_busy_acquisition_and_holder_are_separate(self):
        con=self.connect()
        try:
            self.con.execute('BEGIN IMMEDIATE')
            self.con.execute('INSERT INTO private_table VALUES(?)',('private-payload',))
            with self.assertRaises(sqlite3.OperationalError):
                con.execute('BEGIN IMMEDIATE')
            self.assertFalse(con.in_transaction)
            self.assertGreater(self.phase('acquire','busy')[0]['total_seconds'],0.01)
            self.assertEqual(len(self.collector.snapshot()['active']),1)
            self.assertEqual(self.phase('hold','commit'),[])
            self.con.execute('COMMIT')
            con.execute('BEGIN IMMEDIATE')
            con.execute('INSERT INTO private_table VALUES(?)',('second-payload',))
            con.execute('COMMIT')
            self.assertEqual(con.execute('SELECT COUNT(*) FROM private_table').fetchone(),(2,))
            self.assertEqual(sum(x['count'] for x in self.phase('hold','commit')),2)
            self.assertEqual(self.collector.snapshot()['active'],[])
            evidence=json.dumps(self.collector.snapshot())
            for private in [self.path,'private_table','private-payload','second-payload','INSERT']:
                self.assertNotIn(private,evidence)
        finally:
            con.close()

    def test_independent_writer_waits_then_commits_exactly_once(self):
        ready,done=threading.Event(),threading.Event()
        errors=[]
        def writer():
            con=self.connect()
            try:
                con.execute('PRAGMA busy_timeout=1000')
                ready.set()
                held.wait(5)
                con.execute('BEGIN IMMEDIATE')
                con.execute('INSERT INTO private_table VALUES(?)',('writer',))
                con.execute('COMMIT')
                done.set()
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        held=threading.Event()
        thread=threading.Thread(target=writer)
        thread.start()
        try:
            self.assertTrue(ready.wait(5))
            self.con.execute('BEGIN IMMEDIATE')
            held.set()
            self.assertFalse(done.wait(0.05))
            self.con.execute('ROLLBACK')
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors,[])
            self.assertEqual(self.con.execute('SELECT COUNT(*) FROM private_table').fetchone(),(1,))
            self.assertGreater(max(row['max_seconds'] for row in self.phase('acquire','success')),0.025)
            self.assertEqual(self.collector.snapshot()['active'],[])
        finally:
            if self.con.in_transaction:
                self.con.rollback()
            held.set()
            thread.join(5)

    def test_body_rollback_and_nested_begin_do_not_lose_holder(self):
        self.con.execute('BEGIN IMMEDIATE')
        with self.assertRaises(sqlite3.OperationalError):
            self.con.execute('BEGIN IMMEDIATE')
        with self.assertRaises(sqlite3.OperationalError):
            self.con.execute('INSERT INTO missing VALUES(1)')
        self.assertTrue(self.con.in_transaction)
        self.assertEqual(len(self.collector.snapshot()['active']),1)
        self.con.rollback()
        self.assertEqual(len(self.phase('acquire','error')),1)
        self.assertEqual(len(self.phase('hold','rollback')),1)

    def test_failed_commit_retains_transaction_until_rollback(self):
        self.con.execute('PRAGMA foreign_keys=ON')
        self.con.execute('CREATE TABLE parent(id INTEGER PRIMARY KEY)')
        self.con.execute('CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)')
        self.con.execute('BEGIN IMMEDIATE')
        self.con.execute('INSERT INTO child VALUES(1)')
        with self.assertRaises(sqlite3.IntegrityError):
            self.con.execute('COMMIT')
        self.assertTrue(self.con.in_transaction)
        self.assertEqual(len(self.phase('end_call','error')),1)
        self.assertEqual(self.phase('hold','commit'),[])
        self.con.execute('ROLLBACK')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM child').fetchone(),(0,))
        self.assertEqual(len(self.phase('hold','rollback')),1)

    def test_scripts_deferred_transactions_and_close_have_explicit_coverage(self):
        self.con.executescript('BEGIN; INSERT INTO private_table VALUES("excluded"); COMMIT;')
        self.assertEqual(self.collector.snapshot()['excluded_scripts'],1)
        self.assertEqual(self.collector.snapshot()['operations'],[])
        self.con.execute('BEGIN')
        self.con.execute('ROLLBACK')
        self.assertEqual(self.collector.snapshot()['operations'],[])
        con=self.connect()
        con.execute('BEGIN IMMEDIATE')
        con.execute('INSERT INTO private_table VALUES("rolled-back-on-close")')
        con.close()
        self.assertEqual(len(self.phase('hold','close')),1)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM private_table').fetchone(),(1,))

    def test_automatic_rollback_releases_record_and_preserves_exception(self):
        self.con.execute("CREATE TRIGGER reject_payload BEFORE INSERT ON private_table "
                         "BEGIN SELECT RAISE(ROLLBACK,'synthetic rejected row'); END")
        self.con.execute('BEGIN IMMEDIATE')
        with self.assertRaisesRegex(sqlite3.IntegrityError,'synthetic rejected row'):
            self.con.execute('INSERT INTO private_table VALUES(?)',('secret-row',))
        self.assertFalse(self.con.in_transaction)
        self.assertEqual(len(self.phase('hold','automatic_end')),1)
        self.assertEqual(self.collector.snapshot()['active'],[])
        self.assertNotIn('secret-row',json.dumps(self.collector.snapshot()))

    def test_script_does_not_report_old_holder_through_unmeasured_script(self):
        self.con.execute('BEGIN IMMEDIATE')
        self.con.execute('INSERT INTO private_table VALUES("committed-by-script")')
        self.con.executescript('SELECT 1;')
        self.assertFalse(self.con.in_transaction)
        evidence=self.collector.snapshot()
        self.assertEqual(evidence['excluded_holds'],1)
        self.assertEqual(evidence['active'],[])
        self.assertEqual(self.phase('hold','commit'),[])
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM private_table').fetchone(),(1,))

    def test_collector_cardinality_is_bounded(self):
        collector=timing.Collector(limit=4)
        for index in range(1000):
            collector.record('operation_'+str(index),'acquire','success',0.1)
        evidence=collector.snapshot()
        self.assertEqual(len(evidence['operations']),4)
        self.assertEqual(sum(row['count'] for row in evidence['operations']),1000)

    def test_context_failed_commit_records_automatic_rollback(self):
        self.con.execute('PRAGMA foreign_keys=ON')
        self.con.execute('CREATE TABLE parent(id INTEGER PRIMARY KEY)')
        self.con.execute('CREATE TABLE child(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)')
        with self.assertRaises(sqlite3.IntegrityError):
            with self.con:
                self.con.execute('BEGIN IMMEDIATE')
                self.con.execute('INSERT INTO child VALUES(1)')
        self.assertFalse(self.con.in_transaction)
        self.assertEqual(len(self.phase('hold','rollback')),1)
        self.assertEqual(self.phase('hold','commit'),[])
        self.assertEqual(self.collector.snapshot()['active'],[])
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM child').fetchone(),(0,))

    def test_j4_uses_fair_connection_without_collecting_when_disabled(self):
        import j4
        for enabled in ['0','1']:
            with self.subTest(enabled=enabled),patch.dict(os.environ,CDC_SQLITE_WRITE_TIMING=enabled):
                con=j4.open_state(self.path)
                try:
                    self.assertEqual(type(con),timing.TimingConnection if enabled=='1' else timing.sqlite_writer.FairConnection)
                    self.assertEqual(hasattr(con,'timing'),enabled=='1')
                    self.assertEqual(con.execute('PRAGMA busy_timeout').fetchone(),(30000,))
                    self.assertEqual(con.execute('PRAGMA synchronous').fetchone(),(2,))
                finally:
                    con.close()


if __name__=='__main__':
    unittest.main()
