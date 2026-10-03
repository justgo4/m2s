#!/usr/bin/env python3
"""Real WAL writer interleaving, unpublished chunks, restart and last ack."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'tools'))
from join_bridge_stream_test import JoinBridgeStreamTest
import join_job_bridge_test as fixture
import join_job_bridge
import join_outbox
import j4


class JoinPublicationTest(JoinBridgeStreamTest):
    def setUp(self):
        super().setUp()
        self.cfg.update(join_enqueue_jobs=2,join_enqueue_bytes=4096,
                        max_inflight_deliveries=100,max_prepared_bytes=2**30)

    def test_real_writer_between_bounded_chunks_and_hidden_claims(self):
        other=j4.open_state(self.path)
        self.addCleanup(other.close)
        original=join_job_bridge._register_chunk
        chunks=[]
        def register(*args):
            records=args[4]
            self.assertLessEqual(len(records),2)
            result=original(*args)
            chunks.append(len(records))
            with j4.state_transaction(other):
                j4.meta_set(other,'interleaving',len(chunks))
            if not args[-1]:
                self.assertEqual(other.execute('SELECT COUNT(*) FROM active_jobs').fetchone()[0],0)
                for lane in range(4):
                    self.assertIsNone(j4.pending_batch(other,'starrocks.join_sink',lane,self.cfg))
                    self.assertIsNone(j4.claim_delivery(other,'starrocks.join_sink',lane,self.cfg))
            return result
        with patch.object(join_job_bridge,'_register_chunk',side_effect=register):
            result=self.stage()
        self.assertGreater(len(chunks),1)
        self.assertEqual(j4.meta_get(other,'interleaving',0),len(chunks))
        self.assertEqual(other.execute('SELECT COUNT(*) FROM active_jobs').fetchone()[0],len(result['job_ids']))
        self.assert_payloads(result)

    def test_spool_reads_and_validation_never_hold_the_write_lock(self):
        other=j4.open_state(self.path)
        other.execute('PRAGMA busy_timeout=1')
        self.addCleanup(other.close)
        original=j4.read_spool_record
        calls=[]
        def read(spool):
            result=original(spool)
            with j4.state_transaction(other):
                j4.meta_set(other,'spool_interleave',len(calls))
            calls.append(1)
            return result
        with patch.object(j4,'read_spool_record',side_effect=read):
            self.assert_payloads(self.stage())
        self.assertGreater(len(calls),2)

    def crash_after_first(self):
        original=join_job_bridge._register_chunk
        def register(*args):
            result=original(*args)
            raise RuntimeError('crash after committed chunk')
        with patch.object(join_job_bridge,'_register_chunk',side_effect=register):
            with self.assertRaisesRegex(RuntimeError,'crash after committed chunk'):
                self.stage()

    def test_crash_retains_hidden_jobs_accounting_and_exact_restart_last_ack(self):
        self.crash_after_first()
        before=self.con.execute('SELECT id,payload FROM jobs ORDER BY id').fetchall()
        self.assertTrue(before)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM active_jobs').fetchone()[0],0)
        size=self.con.execute('SELECT SUM(logical_bytes) FROM jobs').fetchone()[0]
        self.assertEqual(j4.meta_get(self.con,'pending_bytes',0),size)
        delivery=fixture.create_delivery(self.con,before[0][0])
        with self.assertRaisesRegex(RuntimeError,'unsealed JOIN'):
            j4.acknowledge_delivery(self.con,delivery)
        self.con.execute('DELETE FROM load_parts WHERE delivery_id=?',(delivery,))
        self.con.execute('DELETE FROM deliveries WHERE id=?',(delivery,))
        fixture.stage_delta(self.con,2,[])
        with self.assertRaisesRegex(RuntimeError,'earlier JOIN'):
            join_job_bridge.stage_commit(self.con,'join-consumer',2,self.mapping,self.cfg)
        self.assertEqual(join_outbox.visible_frontier(self.con,'join-consumer'),0)
        self.con.close()
        self.con=j4.open_state(self.path)
        result=self.stage()
        self.assertEqual(self.con.execute('SELECT id,payload FROM jobs ORDER BY id LIMIT ?',(len(before),)).fetchall(),before)
        self.assert_payloads(result)
        for job_id in result['job_ids']:
            j4.acknowledge_delivery(self.con,fixture.create_delivery(self.con,job_id))
            expected=1 if job_id==result['job_ids'][-1] else 0
            self.assertEqual(join_outbox.visible_frontier(self.con,'join-consumer'),expected)
        self.assertEqual(j4.meta_get(self.con,'pending_bytes',0),0)

    def test_stale_registration_is_noop_and_changed_restart_spool_fails_closed(self):
        self.crash_after_first()
        original=join_job_bridge._register_chunk
        def register(*args):
            self.assertTrue(original(*args))
            before=j4.meta_get(self.con,'pending_bytes',0)
            self.assertFalse(original(*args))
            self.assertEqual(j4.meta_get(self.con,'pending_bytes',0),before)
            return True
        self.cfg['batch_rows']=4
        with self.assertRaisesRegex(RuntimeError,'spool changed'):
            self.stage()
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM active_jobs').fetchone()[0],0)
        self.cfg['batch_rows']=3
        with patch.object(join_job_bridge,'_register_chunk',side_effect=register):
            self.assert_payloads(self.stage())

    def test_chunk_body_error_rolls_back_cursor_jobs_links_and_accounting(self):
        self.crash_after_first()
        before=self.con.execute('SELECT * FROM join_job_staging').fetchall()
        size=j4.meta_get(self.con,'pending_bytes',0)
        ids=self.con.execute('SELECT id FROM jobs').fetchall()
        self.con.execute("CREATE TEMP TRIGGER fail_jobs BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT,'disk fault'); END")
        with self.assertRaisesRegex(Exception,'disk fault'):
            self.stage()
        self.assertEqual(self.con.execute('SELECT * FROM join_job_staging').fetchall(),before)
        self.assertEqual(self.con.execute('SELECT id FROM jobs').fetchall(),ids)
        self.assertEqual(j4.meta_get(self.con,'pending_bytes',0),size)
        self.con.execute('DROP TRIGGER fail_jobs')
        self.assert_payloads(self.stage())

    def test_crash_after_seal_reuses_jobs_and_gc_removes_manifest(self):
        original=join_job_bridge._register_chunk
        def register(*args):
            result=original(*args)
            if args[-1]:
                raise RuntimeError('crash after seal')
            return result
        with patch.object(join_job_bridge,'_register_chunk',side_effect=register):
            with self.assertRaisesRegex(RuntimeError,'crash after seal'):
                self.stage()
        ids=join_job_bridge._already_staged(self.con,'join-consumer',1)
        self.con.close()
        self.con=j4.open_state(self.path)
        with patch.object(j4,'raw_arrow',side_effect=AssertionError('sealed spool rebuilt')):
            self.assertEqual(self.stage()['job_ids'],ids)
        for job_id in ids:
            j4.acknowledge_delivery(self.con,fixture.create_delivery(self.con,job_id))
        self.con.execute("DELETE FROM join_output_streams WHERE consumer_id='join-consumer'")
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_job_staging').fetchone()[0],0)
        self.assertEqual(j4.meta_get(self.con,'pending_bytes',0),0)

    def test_legacy_completed_jobs_remain_claimable_after_view_upgrade(self):
        result=self.stage()
        self.con.execute('DELETE FROM join_job_staging')
        self.con.execute('DROP VIEW active_jobs')
        self.con.execute('CREATE VIEW active_jobs AS SELECT j.* FROM jobs j LEFT JOIN retired_jobs r ON r.job_id=j.id WHERE r.job_id IS NULL')
        j4.meta_set(self.con,'join_job_staging_view_v1',0)
        self.con.close()
        self.con=j4.init_state(self.path)
        self.assertEqual(self.stage()['job_ids'],result['job_ids'])
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM active_jobs').fetchone()[0],len(result['job_ids']))
        self.assert_payloads(result)


if __name__=='__main__':
    unittest.main()
