#!/usr/bin/env python3
"""Real WAL cuts, moving leaders, failure/restart, output fences and handoff."""
from pathlib import Path
import pickle
import random
import sys
import threading
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tools')]
import j4
import join_frozen_state as frozen
import join_state
import join_shared_runtime as shared
import join_outbox
import join_output_build
import join_log_consumer
import join_ir
import join_task_catalog
import join_task_runner
import source_state
import task_generation
import stateful_catalog_runtime
import stateful_physical_registry
import join_bootstrap_stream_test as pairs
import join_shared_runtime_test as runtime


class FrozenTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/'state.sqlite3')
        self.con=j4.init_state(self.path)
        self.addCleanup(lambda:self.con.close())
        self.spec=pairs.setup_state(self.con,left=32,right=5)
        self.expected=join_state.read_pairs(self.con,'state')

    def copy(self,owner='pin',target='copy',spec=None,**kw):
        config=dict(limit=3,byte_limit=1024,max_row_bytes=4096)
        config.update(kw)
        return frozen.copy_step(self.con,owner,target,self.spec if spec is None else spec,**config)

    def finish(self,**kw):
        for _ in range(1000):
            result=self.copy(**kw)
            self.assertLessEqual(result['scan_work'],kw.get('limit',3))
            if result['done']:
                return
        self.fail('frozen copy did not finish')

    def test_cleanup_byte_budget_includes_join_key_blob_and_singleton_progress(self):
        join_state.begin_bootstrap(self.con,'wide-cleanup',self.spec,0)
        join_state.apply_bootstrap_chunk(self.con,'wide-cleanup',0,'left',
            [dict(id=i,customer_id='wide-key-'+'x'*256,amount=7) for i in (1,2)],None,True)
        join_state.apply_bootstrap_chunk(self.con,'wide-cleanup',0,'right',[],None,True)
        sizes=self.con.execute('''SELECT length(pk_blob)+length(row_payload),
            length(pk_blob)+length(row_payload)+coalesce(length(join_blob),0)
            FROM join_rows WHERE state_id='wide-cleanup' ''').fetchall()
        budget=sum(row[0] for row in sizes)
        self.assertLess(max(row[1] for row in sizes),budget)
        self.assertGreater(sum(row[1] for row in sizes),budget)
        self.assertFalse(frozen.discard_step(self.con,'wide-cleanup',byte_limit=budget))
        self.assertEqual(self.con.execute("SELECT count(*) FROM join_rows WHERE state_id='wide-cleanup'").fetchone()[0],1)
        self.assertTrue(frozen.discard_step(self.con,'wide-cleanup',byte_limit=1))

    def test_moving_cut_random_bilateral_rekeys_inserts_deletes_and_restart(self):
        frozen.pin(self.con,'pin','state',0)
        self.copy()
        expected_one=None
        rng=random.Random(192)
        for seq in range(1,81):
            side=rng.choice(['left','right'])
            stored=self.con.execute('SELECT row_payload FROM join_rows WHERE state_id=? AND side=? ORDER BY pk_blob',
                                    ('state',side)).fetchall()
            old=pickle.loads(rng.choice(stored)[0])
            new=dict(old)
            if side=='left':
                new['customer_id']=rng.choice([42,17,None])
                new['amount']=seq
            else:
                new['bucket']=rng.choice([42,17,None])
                new['name']='value'+str(seq)
            # Include same-PK intermediate values and new PK absence at W.
            inserted=dict(new,id=1000+seq)
            changes=[(side,dict(old,_sync_op=1)),(side,dict(new,_sync_op=0)),
                     (side,dict(inserted,_sync_op=0))]
            join_state.apply_transaction(self.con,'state',seq,changes)
            if seq==1:
                expected_one=join_state.read_pairs(self.con,'state')
                frozen.pin(self.con,'pin1','state',1)
            frozen.gc_step(self.con,limit=2)
            if seq%10==0:
                self.copy()
                self.con.close()
                self.con=j4.init_state(self.path)
        self.finish()
        self.assertEqual(join_state.read_pairs(self.con,'copy'),self.expected)
        self.finish(owner='pin1',target='copy1')
        self.assertEqual(join_state.read_pairs(self.con,'copy1'),expected_one)
        frozen.release(self.con,'pin')
        while frozen.gc_step(self.con):
            pass
        self.assertGreater(self.con.execute('SELECT COUNT(*) FROM join_row_before_images').fetchone()[0],0)
        frozen.release(self.con,'pin1')
        while frozen.gc_step(self.con):
            pass
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_row_before_images').fetchone()[0],0)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM stateful_state_sizes WHERE kind='inner_join_history'").fetchone()[0],0)

    def test_capture_rollback_and_copy_chunk_failure_preserve_exact_cursor(self):
        frozen.pin(self.con,'pin','state',0)
        def fail(*_):
            raise RuntimeError('injected crash')
        with self.assertRaisesRegex(RuntimeError,'crash'):
            join_state.apply_transaction(self.con,'state',1,
                [('left',dict(id=0,customer_id=17,amount=100,_sync_op=0))],fault_after_rows=fail)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_row_before_images').fetchone()[0],0)
        self.assertEqual(join_state.read_pairs(self.con,'state'),self.expected)
        self.copy()
        before=join_state.state_info(self.con,'copy')
        put=join_state._put_row_locked
        calls=[]
        def put_then_fail(*args):
            result=put(*args)
            calls.append(1)
            if len(calls)==2:
                fail()
            return result
        with patch.object(join_state,'_put_row_locked',side_effect=put_then_fail):
            with self.assertRaisesRegex(RuntimeError,'crash'):
                self.copy()
        self.assertEqual(join_state.state_info(self.con,'copy'),before)
        self.finish()
        self.assertEqual(join_state.read_pairs(self.con,'copy'),self.expected)

    def test_wal_writer_between_copy_reads_and_chunks_projection_and_byte_budget(self):
        frozen.pin(self.con,'pin','state',0)
        other=j4.open_state(self.path)
        self.addCleanup(other.close)
        other.execute('PRAGMA busy_timeout=1')
        project=join_state._source_row
        calls=[]
        def project_with_writer(*args):
            with j4.state_transaction(other):
                j4.meta_set(other,'independent_writer',len(calls))
            calls.append(1)
            return project(*args)
        target=dict(self.spec,projections=self.spec['projections'][:1])
        with patch.object(join_state,'_source_row',side_effect=project_with_writer):
            # Only read callbacks can interleave writers; write callbacks are
            # deliberately outside this patch below.
            original=join_state.apply_bootstrap_chunk
            def write(*args,**kw):
                with patch.object(join_state,'_source_row',side_effect=project):
                    return original(*args,**kw)
            with patch.object(join_state,'apply_bootstrap_chunk',side_effect=write):
                self.finish(spec=target,byte_limit=200)
        self.assertGreater(len(calls),30)
        expected=[dict(pair_id=x['pair_id'],row={'customer_name':x['row']['customer_name']}) for x in self.expected]
        self.assertEqual(join_state.read_pairs(self.con,'copy'),expected)
        frozen.pin(self.con,'small','state',0)
        with self.assertRaisesRegex(ValueError,'max_row_bytes'):
            self.copy(owner='small',target='reject',max_row_bytes=1)
        self.assertFalse(join_state.state_info(self.con,'reject')['bootstrap_complete'])

    def test_gc_index_skips_protected_history_with_bounded_work(self):
        frozen.pin(self.con,'pin','state',0)
        pk,payload=self.con.execute("SELECT pk_blob,row_payload FROM join_rows WHERE state_id='state' AND side='left' LIMIT 1").fetchone()
        with join_state.transaction(self.con):
            self.con.executemany('''INSERT INTO join_row_before_images
                VALUES('state','left',?,?,NULL,?)''',[(pk,seq,payload) for seq in range(1,5001)])
            self.con.execute("UPDATE join_states SET watermark=5000 WHERE state_id='state'")
        callbacks=[]
        def progress():
            callbacks.append(1)
            return int(len(callbacks)>50)
        self.con.set_progress_handler(progress,100)
        try:
            self.assertEqual(frozen.gc_step(self.con,limit=5),0)
        finally:
            self.con.set_progress_handler(None,0)
        self.assertLess(len(callbacks),50)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_row_before_images').fetchone()[0],5000)
        frozen.release(self.con,'pin')
        self.assertEqual(frozen.gc_step(self.con,limit=5),5)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_row_before_images').fetchone()[0],4995)


class FollowerTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/'state.sqlite3')
        self.con=j4.init_state(self.path)
        self.addCleanup(lambda:self.con.close())
        self.ir=runtime.plan()
        source_state.register_relation(self.con,'db.orders','source-join',runtime.left_schema(),['id'],schema_epoch=1)
        source_state.register_relation(self.con,'db.customers','source-join',runtime.right_schema(),['id'],schema_epoch=1)
        join_state.begin_bootstrap(self.con,'leader',join_ir.state_spec(self.ir),0)
        join_state.apply_bootstrap_chunk(self.con,'leader',0,'left',
            [dict(id=i,customer_id=10,amount=i) for i in range(40)],None,True)
        join_state.apply_bootstrap_chunk(self.con,'leader',0,'right',[dict(id=10,name='old')],None,True)
        self.expected=join_state.read_pairs(self.con,'leader')
        self.leader=join_task_catalog.register_task(self.con,'owner','starrocks.owner',1,
            self.ir,'owner','leader','owner-consumer',runtime.target_schema())
        task_generation.import_existing_multi(self.con,self.leader['sink_key'],1,self.leader['source_relations'],'ready')
        join_log_consumer.ensure_consumer(self.con,'owner-consumer',1,self.ir,'leader',0,
                                         generation_id=self.leader['generation_id'])
        self.leader=join_task_catalog.set_status(self.con,'owner','active')
        stateful_physical_registry.sync_ready(self.con,'inner_join',self.leader,0)
        self.task=join_task_catalog.register_task(self.con,'follower','starrocks.follower',2,
            self.ir,'follower','private','follower-consumer',runtime.target_schema())

    def step(self):
        with patch.object(shared.join_job_bridge,'stage_pending',side_effect=runtime.make_visible):
            return join_task_runner.step(self.con,'follower',{},mapping=runtime.mapping(self.task),bootstrap_limit=3)

    def finish(self):
        for _ in range(300):
            result=self.step()
            if result['phase']=='ready':
                return result
        self.fail('shared follower not ready')

    def test_unpublished_cut_survives_leader_move_restart_and_gc(self):
        first=self.step()
        self.assertEqual(first['phase'],'bootstrap')
        self.assertEqual(join_outbox.pending_commits(self.con,'follower-consumer'),[])
        self.assertEqual(source_state.consumer_info(self.con,'follower-consumer')['watermark'],0)
        self.assertEqual(runtime.source_commit(self.con,None,[dict(id=10,name='new',_sync_op=0)],100),1)
        join_log_consumer.process_next(self.con,'owner-consumer',self.ir)
        frozen.gc_step(self.con)
        self.con.close()
        self.con=j4.init_state(self.path)
        self.finish()
        actual=join_outbox.commit_rows(self.con,'follower-consumer',0)
        self.assertEqual([dict(pair_id=x['pair_id'],row=x['row']) for x in actual],self.expected)
        self.assertEqual(join_outbox.commit_info(self.con,'follower-consumer',1)['digest'],
                         join_outbox.commit_info(self.con,'owner-consumer',1)['digest'])
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_rows WHERE state_id LIKE 'join-follower-snapshot:%'").fetchone()[0],0)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_frozen_pins').fetchone()[0],0)

    def test_cancel_partial_output_cleanup_crash_and_retirement_resume(self):
        while True:
            self.step()
            if self.con.execute("SELECT 1 FROM join_shared_builds WHERE phase='output'").fetchone():
                break
        stateful_catalog_runtime.stage_retirement(self.con,'inner_join',self.task,0)
        discard=frozen.discard_step
        def crash(*args,**kw):
            discard(*args,**kw)
            raise RuntimeError('cleanup crash')
        with patch.object(frozen,'discard_step',side_effect=crash):
            with self.assertRaisesRegex(RuntimeError,'cleanup crash'):
                stateful_catalog_runtime.retire_task(self.con,{},'inner_join',self.task)
        self.assertIsNotNone(shared.maybe_binding(self.con,'follower'))
        self.assertEqual(self.con.execute("SELECT phase FROM join_shared_builds").fetchone()[0],'abandoned')
        self.con.close()
        self.con=j4.init_state(self.path)
        stateful_catalog_runtime.retire_task(self.con,{},'inner_join',self.task)
        shared.gc_retired_followers(self.con)
        self.assertIsNone(shared.maybe_binding(self.con,'follower'))
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_frozen_pins').fetchone()[0],0)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_rows WHERE state_id LIKE 'join-follower-snapshot:%'").fetchone()[0],0)

    def test_live_cancel_discards_unpublished_build_without_visible_wait(self):
        self.step()
        stateful_catalog_runtime.stage_retirement(self.con,'inner_join',self.task,0)
        runtime_state=dict(plan_lock=threading.Lock(),stateful_active_task_ids={'follower'},
                           stateful_retire_frontiers={'follower':0},stateful_retire_items={})
        with patch.object(j4,'runtime_mark_sink_retiring'):
            self.assertTrue(j4.stateful_finish_retirement(self.con,'inner_join',self.task,
                runtime.mapping(self.task),{},runtime_state,0))
        self.assertEqual(runtime_state['stateful_active_task_ids'],set())
        self.assertEqual(join_task_catalog.task_info(self.con,'follower')['status'],'retired')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_frozen_pins').fetchone()[0],0)

    def test_concurrent_worker_waits_for_handoff_then_resolves_private_state(self):
        self.finish()
        started=threading.Event()
        results=[]
        errors=[]
        def worker():
            con=j4.open_state(self.path)
            try:
                started.set()
                results.append(join_task_runner.step(con,'follower',{},mapping=runtime.mapping(self.task)))
            except BaseException as exc:
                errors.append(exc)
            finally:
                con.close()
        original=frozen.copy_step
        threads=[]
        def copying(*args,**kw):
            if not threads:
                thread=threading.Thread(target=worker)
                threads.append(thread)
                thread.start()
                self.assertTrue(started.wait(2))
            return original(*args,**kw)
        with patch.object(frozen,'copy_step',side_effect=copying),patch.object(shared.join_job_bridge,'stage_pending',side_effect=runtime.make_visible):
            shared.promote_followers(self.con,self.leader)
            threads[0].join(5)
        self.assertFalse(threads[0].is_alive())
        self.assertEqual(errors,[])
        self.assertEqual(results[0]['phase'],'ready')
        self.assertEqual(join_state.read_pairs(self.con,'private'),self.expected)

    def test_stale_promotion_selection_after_follower_drop_cannot_retain_or_revive(self):
        self.finish()
        stale=join_task_catalog.task_info(self.con,'follower')
        binding=shared.binding_info(self.con,'follower')
        stateful_catalog_runtime.stage_retirement(self.con,'inner_join',stale,0)
        with patch.object(shared.join_job_bridge,'stage_pending',side_effect=runtime.make_visible):
            stateful_catalog_runtime.retire_task(self.con,{},'inner_join',stale)
        # Selection precedes acquiring the task lock; a follower may finish
        # its durable retirement before promotion obtains that lock.
        self.assertIsNone(shared._promote_one(self.con,stale,self.leader,binding))
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_frozen_pins').fetchone()[0],0)
        self.assertEqual(join_task_catalog.task_info(self.con,'follower')['status'],'retired')
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM source_consumers WHERE consumer_id='follower-consumer'").fetchone()[0],0)

    def test_promotion_mid_copy_restart_and_independent_writer_progress(self):
        self.finish()
        other=j4.open_state(self.path)
        self.addCleanup(other.close)
        other.execute('PRAGMA busy_timeout=1')
        copy=frozen.copy_step
        calls=[]
        def crash_after_chunk(*args,**kw):
            result=copy(*args,limit=3)
            with j4.state_transaction(other):
                j4.meta_set(other,'promotion_writer',len(calls))
            calls.append(1)
            if len(calls)==4:
                raise RuntimeError('promotion crash')
            return result
        with patch.object(frozen,'copy_step',side_effect=crash_after_chunk):
            with self.assertRaisesRegex(RuntimeError,'promotion crash'):
                shared.promote_followers(self.con,self.leader)
        self.assertIsNotNone(shared.maybe_binding(self.con,'follower'))
        self.assertFalse(join_state.state_info(self.con,'private')['bootstrap_complete'])
        self.con.close()
        self.con=j4.init_state(self.path)
        self.assertTrue(self.step()['waiting_shared_leader'])
        promoted=shared.promote_followers(self.con,self.leader)
        self.assertEqual(promoted[0]['frontier'],0)
        self.assertIsNone(shared.maybe_binding(self.con,'follower'))
        self.assertEqual(join_state.read_pairs(self.con,'private'),self.expected)
        self.assertEqual(source_state.consumer_info(self.con,'follower-consumer')['metadata']['kind'],'inner_join_v1')
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_frozen_pins').fetchone()[0],0)

    def test_drop_after_interrupted_promotion_discards_partial_private_bytes_and_pin(self):
        self.finish()
        copy=frozen.copy_step
        def crash(*args,**kw):
            copy(*args,limit=3)
            raise RuntimeError('promotion interrupted')
        with patch.object(frozen,'copy_step',side_effect=crash):
            with self.assertRaisesRegex(RuntimeError,'interrupted'):
                shared.promote_followers(self.con,self.leader)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_rows WHERE state_id='private'").fetchone()[0],3)
        active=join_task_catalog.task_info(self.con,'follower')
        stateful_catalog_runtime.stage_retirement(self.con,'inner_join',active,0)
        with patch.object(shared.join_job_bridge,'stage_pending',side_effect=runtime.make_visible):
            stateful_catalog_runtime.retire_task(self.con,{},'inner_join',active)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_states WHERE state_id='private'").fetchone()[0],0)
        self.assertEqual(self.con.execute('SELECT COUNT(*) FROM join_frozen_pins').fetchone()[0],0)
        self.assertEqual(shared._locks,{})


if __name__=='__main__':
    unittest.main()
