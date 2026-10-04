#!/usr/bin/env python3
from pathlib import Path
from decimal import Decimal
import sys
import json
import tempfile
import threading
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import j4
import cold_build_admission as admission
import aggregate_task_catalog
import aggregate_task_runner
import task_generation
from tools import aggregate_runtime_test as fixture
from tools import join_runtime_test as join_fixture


class AdmissionTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=str(Path(self.temp.name)/'state.db')
        self.con=j4.init_state(self.path)
        fixture.source_state.register_relation(
            self.con,'db.orders','source',fixture.source_schema(),['id'])
        fixture.source_state.stage_snapshot_batch(
            self.con,'db.orders',fixture.source_table([
                (i,'a' if i%2 else 'b',Decimal('10'),1) for i in range(1,9)
            ]),cursor=(8,),is_last=True)
        self.first=self.register('first',1)
        self.second=self.register('second',2)
        self.cfg=dict(fixture.cfg(self.path),cold_build_admission=True,
                      cold_build_rows=2,stateful_share_mode='off')
        self.runtime={}

    def tearDown(self):
        self.con.close()
        self.temp.cleanup()

    def register(self,name,version):
        return aggregate_task_catalog.register_task(
            self.con,name,name,version,fixture.ir(),name,
            'state-'+name,'consumer-'+name,fixture.target_schema())

    def run_step(self,task,con=None,runtime=None):
        con=self.con if con is None else con
        runtime=self.runtime if runtime is None else runtime
        return admission.step(con,self.cfg,runtime,'aggregate',task['task_id'],
            lambda limit:aggregate_task_runner.step(con,task['task_id'],self.cfg,
                mapping=fixture.mapping(task),bootstrap_limit=limit),1000)

    def test_durable_order_restart_drop_and_read_only_denial(self):
        queries=[];self.con.set_trace_callback(queries.append)
        result=self.run_step(self.second)
        self.assertTrue(result['cold_build_paused'])
        self.assertFalse(any(q.startswith('BEGIN') for q in queries))
        self.con.set_trace_callback(None)
        self.assertEqual(admission.eligible(self.con,self.cfg,{}),['first','second'])
        self.con.close();self.con=j4.open_state(self.path);self.runtime={}
        self.assertTrue(self.run_step(self.second)['cold_build_paused'])
        aggregate_task_catalog.set_status(self.con,'first','failed')
        self.assertEqual(admission.eligible(self.con,self.cfg,{}),['second'])
        self.assertEqual(self.run_step(self.second)['phase'],'bootstrap')

    def test_actual_bootstrap_cap_history_release_and_visible_ack(self):
        first=self.run_step(self.first)
        self.assertEqual(first['phase'],'bootstrap')
        current=fixture.aggregate_state.state_info(self.con,self.first['state_id'])
        self.assertEqual(json.loads(current['bootstrap_cursor'])[0][1],'2')
        for _ in range(30):
            self.run_step(self.first)
            generation=task_generation.info(self.con,'first',1)
            if generation['status']=='history_staged':
                break
        self.assertEqual(generation['status'],'history_staged')
        self.assertEqual(aggregate_task_catalog.task_info(self.con,'first')['status'],'candidate')
        self.assertEqual(admission.eligible(self.con,self.cfg,{}),['second'])
        self.assertNotIn('cold_build_paused',self.run_step(self.second))
        fixture.ack_all(self.con)
        for _ in range(10):
            self.run_step(self.first)
            fixture.ack_all(self.con)
            if aggregate_task_catalog.task_info(self.con,'first')['status']=='active':
                break
        self.assertEqual(aggregate_task_catalog.task_info(self.con,'first')['status'],'active')
        self.assertEqual(task_generation.info(self.con,'first',1)['status'],'ready')

    def test_active_does_not_take_cold_guard(self):
        # Status is a scheduling probe here; runtime still independently
        # validates that an active generation cannot regress.
        self.con.execute("UPDATE aggregate_task_descriptors SET status='active' WHERE task_id='second'")
        lock=threading.Lock();lock.acquire();self.runtime['cold_build_lock']=lock
        seen=[]
        result=admission.step(self.con,self.cfg,self.runtime,'aggregate','second',
                              lambda limit:seen.append(limit) or {'ready':True},1000)
        self.assertTrue(result['ready']);self.assertEqual(seen,[1000]);lock.release()

    def test_preferred_candidate_leader_does_not_deadlock(self):
        self.cfg['stateful_share_mode']='compatible'
        self.con.execute("""INSERT INTO stateful_share_preferences
            (task_id,kind,preferred_leader_task_id,reuse_mode,reason,created,updated)
            VALUES('first','aggregate','second','exact','test',1,1)""")
        self.assertEqual(admission.eligible(self.con,self.cfg,{}),['second'])
        self.assertTrue(self.run_step(self.first)['cold_build_paused'])
        self.assertEqual(self.run_step(self.second)['phase'],'bootstrap')
        self.cfg['stateful_share_mode']='off'
        self.assertEqual(admission.eligible(self.con,self.cfg,{}),['first','second'])

    def test_retiring_or_incomplete_source_does_not_monopolize(self):
        self.runtime['stateful_active_task_ids']={'second'}
        self.assertEqual(admission.eligible(self.con,self.cfg,self.runtime),['second'])
        self.con.execute("UPDATE source_relations SET complete_seq=NULL WHERE table_name='db.orders'")
        self.assertEqual(admission.eligible(self.con,self.cfg,self.runtime),[])

    def test_exception_releases_guard(self):
        def fail(limit):
            raise RuntimeError('synthetic failure')
        with self.assertRaisesRegex(RuntimeError,'synthetic failure'):
            admission.step(self.con,self.cfg,self.runtime,'aggregate','first',fail,1000)
        self.assertTrue(self.runtime['cold_build_lock'].acquire(blocking=False))
        self.runtime['cold_build_lock'].release()
        self.assertEqual(self.run_step(self.first)['phase'],'bootstrap')

    def test_concurrent_cold_deferred_ready_continues(self):
        entered=threading.Event();release=threading.Event();errors=[]
        def worker():
            con=j4.open_state(self.path)
            try:
                def callback(limit):
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError('test timeout')
                    return aggregate_task_runner.step(con,'first',self.cfg,
                        mapping=fixture.mapping(self.first),bootstrap_limit=limit)
                admission.step(con,self.cfg,self.runtime,'aggregate','first',callback,1000)
            except Exception as exc:
                errors.append(exc)
            finally:
                con.close()
        thread=threading.Thread(target=worker);thread.start()
        try:
            self.assertTrue(entered.wait(3))
            self.assertTrue(self.run_step(self.second)['cold_build_paused'])
        finally:
            release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])

    def test_actual_join_uses_same_guard_and_row_budget(self):
        path=str(Path(self.temp.name)/'join.db')
        con=j4.init_state(path)
        try:
            for relation,schema,rows in [
                ('db.orders',join_fixture.left_schema(),[
                    dict(id=i,customer_id=1,amount=i) for i in range(1,9)]),
                ('db.customers',join_fixture.right_schema(),[dict(id=1,name='a')])]:
                fixture.source_state.register_relation(con,relation,'source',schema,['id'])
                fixture.source_state.stage_snapshot_batch(con,relation,
                    join_fixture.snapshot_table(schema,rows),cursor=(rows[-1]['id'],),is_last=True)
            task=join_fixture.register_task(con,join_fixture.plan())
            cfg=dict(join_fixture.cfg(path),cold_build_admission=True,
                     cold_build_rows=2,stateful_share_mode='off')
            lock=threading.Lock();lock.acquire();runtime={'cold_build_lock':lock}
            callback=lambda limit:join_fixture.join_task_runner.step(
                con,task['task_id'],cfg,mapping=join_fixture.wire_mapping(task),
                bootstrap_limit=limit)
            result=admission.step(con,cfg,runtime,'inner_join',task['task_id'],callback,1000)
            self.assertTrue(result['cold_build_paused'])
            self.assertEqual(con.execute('SELECT COUNT(*) FROM join_states').fetchone()[0],0)
            lock.release()
            result=admission.step(con,cfg,runtime,'inner_join',task['task_id'],callback,1000)
            self.assertEqual(result['phase'],'bootstrap')
            self.assertLessEqual(con.execute('SELECT COUNT(*) FROM join_rows').fetchone()[0],2)
        finally:
            con.close()

    def test_default_off_no_probe_no_limit(self):
        queries=[];self.con.set_trace_callback(queries.append)
        result=admission.step(self.con,{}, {}, 'aggregate','missing',lambda limit:limit,1000)
        self.assertEqual(result,1000);self.assertEqual(queries,[])


if __name__=='__main__':
    unittest.main()
