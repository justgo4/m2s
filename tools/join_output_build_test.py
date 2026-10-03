#!/usr/bin/env python3
"""Fixed-W output chunks: fan-out, scan bounds, WAL writers, pins and crashes."""
from pathlib import Path
import pickle
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tools')]
import j4
import join_outbox
import join_state
import join_output_build
import join_job_bridge
import join_job_bridge_test as jobs
import join_bootstrap_stream_test as pairs
import join_generation_test as generation_fixture
import join_generation
import join_task_catalog
import stateful_catalog_runtime
import join_runtime_test as runtime_fixture
import join_log_consumer
import source_state
import task_generation


class JoinOutputBuildTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/'state.sqlite3')
        self.con=j4.init_state(self.path)
        self.addCleanup(lambda:self.con.close())
        join_state.install(self.con)
        join_outbox.install(self.con)
        source_state.install(self.con)
        self.spec=pairs.setup_state(self.con)
        self.expected=[(x['pair_id'],0,pickle.dumps(x['row'],protocol=5))
                       for x in join_state.read_pairs(self.con,'state')]

    def step(self,**kw):
        config=dict(row_limit=3,scan_limit=8,byte_limit=512,max_row_bytes=4096)
        config.update(kw)
        return join_output_build.step(self.con,'consumer','state',1,'generation',0,**config)

    def finish(self,**kw):
        for _ in range(500):
            result=self.step(**kw)
            self.assertLessEqual(result['scan_work'],kw.get('scan_limit',8))
            self.assertLessEqual(result['nrows'],kw.get('row_limit',3))
            if result['done']:
                return
        self.fail('output build did not finish')

    def assert_exact(self):
        commit=join_outbox.commit_info(self.con,'consumer',0)
        self.assertTrue(commit['sealed'])
        self.assertEqual(commit['nrows'],72)
        self.assertEqual(commit['digest'],join_outbox._digest('bootstrap',self.expected))
        actual=self.con.execute("SELECT pair_id,op,row_payload FROM join_output_rows ORDER BY pair_id").fetchall()
        self.assertEqual(actual,self.expected)

    def test_fanout_two_key_cursor_restart_and_complete_bag_digest(self):
        self.assertFalse(self.step()['done'])
        manifest=join_output_build._manifest(self.con,'consumer',0)
        self.assertIsNotNone(manifest[3])
        self.assertIsNotNone(manifest[4])
        first=self.con.execute('SELECT pair_id,row_payload FROM join_output_rows ORDER BY pair_id').fetchall()
        self.con.close()
        self.con=j4.open_state(self.path)
        self.finish()
        self.assert_exact()
        for pair,payload in first:
            self.assertEqual(self.con.execute('SELECT row_payload FROM join_output_rows WHERE pair_id=?',(pair,)).fetchone()[0],payload)
        with patch.object(join_output_build,'_scan',side_effect=AssertionError('sealed build rescanned')):
            self.assertTrue(self.step()['done'])

    def test_unsealed_output_is_hidden_from_reads_staging_copy_and_visibility(self):
        self.step()
        self.assertEqual(join_outbox.pending_commits(self.con,'consumer'),[])
        self.assertEqual(join_outbox.visible_frontier(self.con,'consumer'),-1)
        with self.assertRaisesRegex(RuntimeError,'unsealed'):
            join_outbox.commit_rows(self.con,'consumer',0)
        with self.assertRaisesRegex(RuntimeError,'unsealed'):
            join_outbox.mark_visible(self.con,'consumer',0)
        with self.assertRaisesRegex(RuntimeError,'unsealed'):
            join_job_bridge.stage_commit(self.con,'consumer',0,jobs.mapping(),jobs.cfg(self.path))
        join_outbox.ensure_stream(self.con,'copy','state',2,'copy-generation',0)
        with self.assertRaisesRegex(RuntimeError,'unsealed'):
            join_outbox.copy_commit(self.con,'consumer','copy',0)
        with self.assertRaisesRegex(RuntimeError,'unsealed'):
            join_outbox.copy_commit_projected(self.con,'consumer','copy',0,self.spec)
        self.finish()
        self.assertEqual(len(join_outbox.pending_commits(self.con,'consumer')),1)
        self.assert_exact()

    def test_wal_writer_between_chunks_and_during_canonical_digest(self):
        other=j4.open_state(self.path)
        other.execute('PRAGMA busy_timeout=1')
        self.addCleanup(other.close)
        commit=join_output_build._commit_chunk
        digest=join_outbox._digest
        chunks=[]
        def chunk(*args):
            result=commit(*args)
            with j4.state_transaction(other):
                j4.meta_set(other,'build_interleave',len(chunks))
            chunks.append(1)
            return result
        def unlocked_digest(kind,rows):
            def unlocked_rows():
                for row in rows:
                    with j4.state_transaction(other):
                        j4.meta_set(other,'digest_interleave',1)
                    yield row
            return digest(kind,unlocked_rows())
        with patch.object(join_output_build,'_commit_chunk',side_effect=chunk),patch.object(join_outbox,'_digest',side_effect=unlocked_digest):
            self.finish()
        self.assertGreater(len(chunks),1)
        self.assertEqual(j4.meta_get(other,'digest_interleave',0),1)
        self.assert_exact()

    def test_chunk_crash_stale_retry_and_body_rollback(self):
        original=join_output_build._commit_chunk
        def crash(*args):
            self.assertTrue(original(*args))
            count=self.con.execute('SELECT COUNT(*) FROM join_output_rows').fetchone()[0]
            self.assertFalse(original(*args))
            self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_output_rows').fetchone()[0],count)
            raise RuntimeError('after chunk commit')
        with patch.object(join_output_build,'_commit_chunk',side_effect=crash):
            with self.assertRaisesRegex(RuntimeError,'after chunk commit'):
                self.step()
        saved=join_output_build._manifest(self.con,'consumer',0)
        nrows=join_outbox.commit_info(self.con,'consumer',0)['nrows']
        self.con.execute("CREATE TEMP TRIGGER fail_output BEFORE INSERT ON join_output_rows BEGIN SELECT RAISE(ABORT,'body fault'); END")
        with self.assertRaisesRegex(Exception,'body fault'):
            self.step()
        self.assertEqual(join_output_build._manifest(self.con,'consumer',0),saved)
        self.assertEqual(join_outbox.commit_info(self.con,'consumer',0)['nrows'],nrows)
        self.con.execute('DROP TRIGGER fail_output')
        self.con.close()
        self.con=j4.open_state(self.path)
        self.finish()
        self.assert_exact()

    def test_crash_after_seal_and_state_move_fail_closed(self):
        seal=join_output_build._seal
        def crash(*args):
            seal(*args)
            raise RuntimeError('after seal commit')
        with patch.object(join_output_build,'_seal',side_effect=crash):
            with self.assertRaisesRegex(RuntimeError,'after seal commit'):
                self.finish()
        self.con.close()
        self.con=j4.open_state(self.path)
        self.assertTrue(self.step()['done'])
        self.assert_exact()
        join_state.apply_transaction(self.con,'state',1,[])
        with self.assertRaisesRegex(RuntimeError,'fixed-W'):
            self.step()

    def test_unmatched_scan_work_and_singleton_byte_cap(self):
        # 200 unmatched left rows, no output: output LIMIT alone cannot help.
        with join_state.transaction(self.con):
            for index in range(100,300):
                join_state._put_row_locked(self.con,'state','left',self.spec,dict(id=index,customer_id=999,amount=7))
        iterations=0
        while True:
            r=self.step(row_limit=1000,scan_limit=4,byte_limit=32)
            self.assertLessEqual(r['scan_work'],4)
            if r['serialized_bytes']>32:
                self.assertEqual(r['nrows'],1)
                self.assertLessEqual(r['serialized_bytes'],4096)
            iterations+=1
            if r['done']:
                break
            self.assertLess(iterations,500)
        self.assertGreater(iterations,100)
        self.assert_exact()

    def test_oversized_row_and_changed_state_before_chunk_never_publish(self):
        with self.assertRaisesRegex(ValueError,'max_row_bytes'):
            self.step(max_row_bytes=1)
        self.assertEqual(join_outbox.commit_info(self.con,'consumer',0)['nrows'],0)
        scan=join_output_build._scan
        def move(*args):
            result=scan(*args)
            join_state.apply_transaction(self.con,'state',1,[])
            return result
        with patch.object(join_output_build,'_scan',side_effect=move):
            with self.assertRaisesRegex(RuntimeError,'fixed-W'):
                self.step()
        self.assertEqual(join_outbox.pending_commits(self.con,'consumer'),[])
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_output_rows').fetchone()[0],0)

    def test_discard_restartable_chunks_removes_unpublished_rows_and_identities(self):
        self.step()
        self.step()
        other=j4.open_state(self.path)
        self.addCleanup(other.close)
        deletes=[]
        self.con.set_trace_callback(lambda sql:deletes.append(sql) if sql.startswith('DELETE FROM join_output_rows') else None)
        self.assertTrue(join_output_build.discard_unactivated(self.con,'consumer',0,limit=2))
        self.assertGreater(len(deletes),2)
        for table in ['join_output_commits','join_output_rows','join_output_identities','join_output_builds']:
            self.assertEqual(other.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)
        self.assertFalse(join_output_build.discard_unactivated(self.con,'consumer',0))

    def test_legacy_commit_sealed_migration_preserves_rows_and_exact_digest(self):
        join_outbox.seed_bootstrap(self.con,'consumer','state',1,'generation',0)
        previous=join_outbox.commit_info(self.con,'consumer',0)
        self.con.execute('ALTER TABLE join_output_commits DROP COLUMN sealed')
        join_outbox.ensure_installed(self.con)
        self.assertEqual(join_outbox.commit_info(self.con,'consumer',0),previous)
        self.assertTrue(self.step()['done'])
        self.assert_exact()


class GenerationOutputBuildTest(unittest.TestCase):
    def test_retirement_cleanup_crash_keeps_pin_and_intent_then_restarts(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'state.sqlite3')
            con=j4.init_state(path)
            ir=generation_fixture.plan()
            source_state.register_relation(con,'db.orders','source-join',generation_fixture.left_schema(),['id'])
            source_state.register_relation(con,'db.customers','source-join',generation_fixture.right_schema(),['id'])
            source_state.stage_snapshot_batch(con,'db.orders',generation_fixture.table(generation_fixture.left_schema(),
                [dict(id=i,customer_id=10,amount=5) for i in range(20)]),cursor=(19,),is_last=True)
            source_state.stage_snapshot_batch(con,'db.customers',generation_fixture.table(generation_fixture.right_schema(),
                [dict(id=10,name='alice')]),cursor=(10,),is_last=True)
            task=join_task_catalog.register_task(con,'task','join-sink',1,ir,'join_sink','state','consumer',runtime_fixture.target_schema())
            begin=join_generation.begin(con,'join-sink',1,ir,'state')
            pin=begin['pin']['pin_id']
            while not join_generation.process_next_chunk(con,'join-sink',1,ir,'state',limit=8)['done']:
                pass
            for _ in range(3):
                join_generation.activate_catchup(con,'join-sink',1,'consumer',ir,'state',output_limit=2)
            stateful_catalog_runtime.stage_retirement(con,'inner_join',task,0)
            con.execute("CREATE TEMP TRIGGER fail_discard BEFORE DELETE ON join_output_rows WHEN (SELECT COUNT(*) FROM join_output_rows)=4 BEGIN SELECT RAISE(ABORT,'discard fault'); END")
            discard=join_output_build.discard_unactivated
            with patch.object(join_output_build,'discard_unactivated',side_effect=lambda c,u,w:discard(c,u,w,limit=2)):
                with self.assertRaisesRegex(Exception,'discard fault'):
                    stateful_catalog_runtime.retire_task(con,jobs.cfg(path),'inner_join',task)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM join_output_rows').fetchone()[0],4)
            self.assertEqual(source_state.pin_watermark(con,pin),0)
            self.assertEqual(task_generation.info(con,'join-sink',1)['status'],'building')
            self.assertEqual(len(stateful_catalog_runtime.pending_retirements(con)),1)
            with self.assertRaisesRegex(RuntimeError,'being discarded'):
                join_generation.activate_catchup(con,'join-sink',1,'consumer',ir,'state',output_limit=2)
            con.close()
            con=j4.init_state(path)
            result=stateful_catalog_runtime.retire_task(con,jobs.cfg(path),'inner_join',task)
            self.assertEqual(result['task']['status'],'retired')
            self.assertEqual(stateful_catalog_runtime.pending_retirements(con),[])
            with self.assertRaises(KeyError):
                source_state.pin_watermark(con,pin)
            for table in ['join_output_rows','join_output_identities','join_output_builds']:
                self.assertEqual(con.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)
            con.close()

    def test_pin_survives_chunks_and_atomic_consumer_handoff_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            path=str(Path(directory)/'state.sqlite3')
            con=generation_fixture.open_db(path)
            ir=generation_fixture.plan()
            source_state.register_relation(con,'db.orders','source-join',generation_fixture.left_schema(),['id'])
            source_state.register_relation(con,'db.customers','source-join',generation_fixture.right_schema(),['id'])
            source_state.stage_snapshot_batch(con,'db.orders',generation_fixture.table(generation_fixture.left_schema(),
                [dict(id=i,customer_id=10,amount=5) for i in range(20)]),cursor=(19,),is_last=True)
            source_state.stage_snapshot_batch(con,'db.customers',generation_fixture.table(generation_fixture.right_schema(),
                [dict(id=10,name='alice')]),cursor=(10,),is_last=True)
            begin=join_generation.begin(con,'sink',1,ir,'state')
            pin=begin['pin']['pin_id']
            while not join_generation.process_next_chunk(con,'sink',1,ir,'state',limit=8)['done']:
                pass
            result=join_generation.activate_catchup(con,'sink',1,'consumer',ir,'state',output_limit=2)
            self.assertIsNone(result['consumer'])
            self.assertFalse(result['generation']['source_pin_released'])
            self.assertEqual(source_state.pin_watermark(con,pin),0)
            with self.assertRaises(KeyError):
                source_state.consumer_info(con,'consumer')
            generation_fixture.add_commit(con,None,[dict(id=10,name='alice',_sync_op=1),dict(id=10,name='alicia',_sync_op=0)],100)
            def crash():
                raise RuntimeError('consumer handoff crash')
            with self.assertRaisesRegex(RuntimeError,'handoff crash'):
                for _ in range(40):
                    join_generation.activate_catchup(con,'sink',1,'consumer',ir,'state',output_limit=2,fault_after_consumer=crash)
            self.assertEqual(source_state.pin_watermark(con,pin),0)
            self.assertTrue(join_outbox.commit_info(con,'consumer',0)['sealed'])
            with self.assertRaises(KeyError):
                source_state.consumer_info(con,'consumer')
            con.close()
            con=generation_fixture.open_db(path)
            result=join_generation.activate_catchup(con,'sink',1,'consumer',ir,'state',output_limit=2)
            self.assertTrue(result['generation']['source_pin_released'])
            with self.assertRaises(KeyError):
                source_state.pin_watermark(con,pin)
            self.assertEqual(result['consumer']['watermark'],0)
            join_log_consumer.process_next(con,'consumer',ir)
            self.assertEqual(source_state.consumer_info(con,'consumer')['watermark'],1)
            self.assertEqual(len(join_state.read_pairs(con,'state')),20)
            self.assertTrue(all(x['row']['customer_name']=='alicia' for x in join_state.read_pairs(con,'state')))
            con.close()


if __name__=='__main__':
    unittest.main()
