#!/usr/bin/env python3
"""Observe actual WAL retirement/GC boundaries without disposable services."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parent))
import stateful_catalog_e2e as contract


class RetirementPollTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory=Path(self.temp.name)
        self.path=self.directory/'state.sqlite3'
        self.writer=sqlite3.connect(self.path,isolation_level=None)
        self.addCleanup(self.writer.close)
        self.writer.execute('PRAGMA journal_mode=WAL')
        self.writer.executescript('''
            CREATE TABLE aggregate_task_descriptors(task_id,sink_key,status);
            CREATE TABLE join_task_descriptors(task_id,sink_key,status);
            CREATE TABLE stateful_retirements(task_id,sink_key,frontier);
            CREATE TABLE task_generations(sink_key,plan_version,status,source_pin_released);
            CREATE TABLE active_jobs(id);
            CREATE TABLE deliveries(id);
            CREATE TABLE source_consumers(consumer_id);
            CREATE TABLE source_relations(table_name,complete_seq);
            CREATE TABLE aggregate_shared_followers(follower_task_id,leader_task_id,shared_state_id,fixed_w);
            CREATE TABLE join_shared_followers(follower_task_id,leader_task_id,shared_state_id,fixed_w);
            CREATE TABLE stateful_rebuilds(sink_key,old_task_id,new_task_id,frontier,phase,error);
        ''')

    def fixture(self,kind):
        table='aggregate' if kind=='aggregate' else 'join'
        self.writer.execute('INSERT INTO '+table+'_task_descriptors VALUES(?,?,?)',
                            ('task-hot','starrocks.hot','retired'))
        self.writer.execute('INSERT INTO '+table+'_task_descriptors VALUES(?,?,?)',
                            ('task-subview','starrocks.subview','active'))
        for follower in ['task-hot','task-subview']:
            self.writer.execute('INSERT INTO '+table+'_shared_followers VALUES(?,?,?,?)',
                                (follower,'task-owner','state',2))
        return table

    def wait(self,kind='inner_join',timeout=180):
        return contract.wait_stateful_retired(None,None,self.directory,'starrocks.hot',
                                              kind=kind,timeout=timeout)

    def test_retired_status_waits_for_gc_and_preserves_other_binding(self):
        for kind in ['aggregate','inner_join']:
            with self.subTest(kind=kind):
                table=self.fixture(kind)
                def gc(_):
                    self.writer.execute('DELETE FROM '+table+'_shared_followers WHERE follower_task_id=?',
                                        ('task-hot',))
                with patch.object(contract,'live'),patch.object(contract.time,'sleep',side_effect=gc) as sleep:
                    result=self.wait(kind)
                self.assertEqual(sleep.call_count,1)
                self.assertEqual(result[table+'_shared'],[('task-subview','task-owner','state',2)])

    def test_owner_reference_also_waits_for_promotion(self):
        self.fixture('inner_join')
        self.writer.execute('DELETE FROM join_shared_followers WHERE follower_task_id="task-hot"')
        self.writer.execute('UPDATE join_shared_followers SET leader_task_id="task-hot"')
        def promote(_):
            self.writer.execute('DELETE FROM join_shared_followers')
        with patch.object(contract,'live'),patch.object(contract.time,'sleep',side_effect=promote) as sleep:
            result=self.wait()
        self.assertEqual(sleep.call_count,1)
        self.assertEqual(result['join_shared'],[])

    def test_unrelated_exact_id_binding_does_not_delay_retirement(self):
        self.fixture('inner_join')
        self.writer.execute('UPDATE join_shared_followers SET follower_task_id="task-hot-extra" '
                            'WHERE follower_task_id="task-hot"')
        with patch.object(contract,'live'),patch.object(contract.time,'sleep') as sleep:
            result=self.wait()
        self.assertEqual(sleep.call_count,0)
        self.assertEqual(len(result['join_shared']),2)

    def test_stuck_binding_fails_at_original_deadline(self):
        self.fixture('inner_join')
        with patch.object(contract,'live'),patch.object(contract.time,'monotonic',side_effect=[0,0,2]), \
             patch.object(contract.time,'sleep') as sleep:
            with self.assertRaisesRegex(AssertionError,'did not retire online'):
                self.wait(timeout=1)
        self.assertEqual(sleep.call_count,1)

    def test_multi_table_read_uses_one_wal_snapshot(self):
        self.fixture('inner_join')
        original=sqlite3.connect
        writer=self.writer
        class Reader:
            def __init__(self,*args,**kwargs):
                self.con=original(*args,**kwargs)
            def execute(self,sql,*args):
                result=self.con.execute(sql,*args)
                if 'FROM aggregate_task_descriptors' in sql:
                    writer.execute('BEGIN IMMEDIATE')
                    writer.execute('UPDATE join_task_descriptors SET status="active" WHERE task_id="task-hot"')
                    writer.execute('DELETE FROM join_shared_followers WHERE follower_task_id="task-hot"')
                    writer.execute('COMMIT')
                return result
            def close(self):
                self.con.close()
        with patch.object(contract.sqlite3,'connect',side_effect=Reader):
            before=contract.state(self.path)
        # A single observation cannot mix the old descriptor with newer GC.
        self.assertIn(('task-hot','starrocks.hot','retired'),before['join_task_rows'])
        self.assertEqual(len(before['join_shared']),2)
        after=contract.state(self.path)
        self.assertIn(('task-hot','starrocks.hot','active'),after['join_task_rows'])
        self.assertEqual(len(after['join_shared']),1)


if __name__=='__main__':
    unittest.main()
