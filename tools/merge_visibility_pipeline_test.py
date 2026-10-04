#!/usr/bin/env python3
"""Durable independent-lane progress while known remote transactions wait."""
from contextlib import ExitStack
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch,MagicMock

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'tools'))
import j4
import merge_accepted_contention_test as accepted_test


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.path=str(Path(self.temporary.name)/'state.sqlite3')
        self.con=j4.init_state(self.path);self.addCleanup(lambda:self.con.close())
        self.mapping=dict(src_table='events',sr_table='events',_output_columns=['id'],_target_sequence=False)
        self.cfg=dict(merge_visibility_pipeline=True,merge_visibility_per_sink=2,
                      batch_rows=10,batch_bytes=1000,key_partitions=4,
                      max_inflight_deliveries=8,max_prepared_bytes=100000,max_row_bytes=1000)
        self.runtime=dict(stop=threading.Event(),control_lock=threading.Lock(),
                          plan_lock=threading.Lock(),active_plan_version=1,plans={},
                          version_recovery={},load_events={},lane_locks={('events',i):threading.Lock() for i in range(4)})

    def job(self,lane):
        return self.con.execute("INSERT INTO jobs(table_name,lane,kind,payload,nrows,logical_bytes,created) "
             "VALUES('events',?,'cdc',X'00',1,10,0)",(lane,)).lastrowid

    def delivery(self,name,lane,txn):
        job=self.job(lane)
        self.con.execute("INSERT INTO deliveries(id,table_name,lane,prepared) VALUES(?,'events',?,1)",(name,lane))
        j4.assign_jobs(self.con,name,[job])
        self.con.execute("INSERT INTO load_parts(delivery_id,part,label,payload,nrows,txn_id) "
                         "VALUES(?,0,?,X'00',1,?)",(name,'local-'+name,txn))
        return job

    def process_context(self):
        stack=ExitStack();stack.enter_context(patch.object(j4,'runtime_mapping',return_value=self.mapping))
        stack.enter_context(patch.object(j4,'plan_engine',return_value=None))
        stack.enter_context(patch.object(j4,'curl_request',side_effect=AssertionError('known transaction re-uploaded')))
        return stack

    def test_independent_lane_finishes_waiting_lane_and_fifo_survive_restart(self):
        first=self.delivery('waiting',0,99);self.delivery('independent',1,100)
        later=self.job(0)
        with self.process_context(),patch.object(j4,'wait_visible',side_effect=lambda cfg,txn,stop,once=False:
                ('pending',{}) if txn==99 else ('visible',{})) as poll:
            self.assertFalse(j4.process_merge_lane(self.con,{},None,'events',0,self.cfg,self.runtime))
            self.assertTrue(j4.process_merge_lane(self.con,{},None,'events',1,self.cfg,self.runtime))
            self.assertEqual([call.kwargs for call in poll.call_args_list],[{'once':True},{'once':True}])
            self.assertEqual(j4.lane_blocking_delivery(self.con,'events',0),'waiting')
            self.assertEqual(j4.claim_cdc_bundle(self.con,'events',0,self.cfg,self.runtime),'waiting')
            self.assertEqual(self.con.execute('SELECT job_id FROM job_assignments').fetchall(),[(first,)])
            self.assertGreater(j4.prepared_budget_used(self.con),0)
        self.con.close();self.con=j4.open_state(self.path)
        self.runtime.pop('visibility_poll_after');self.runtime.pop('visibility_delivery_started')
        with self.process_context(),patch.object(j4,'wait_visible',return_value=('visible',{})):
            self.assertTrue(j4.process_merge_lane(self.con,{},None,'events',0,self.cfg,self.runtime))
        self.assertEqual(self.con.execute('SELECT id FROM active_jobs').fetchall(),[(later,)])
        self.assertEqual(self.con.execute('SELECT count(*) FROM job_assignments').fetchone()[0],0)
        self.assertEqual(self.runtime['visibility_delivery_started'],{})

    def test_per_sink_admission_and_global_payload_budget_still_apply(self):
        self.delivery('one',0,99);self.delivery('two',1,100);self.job(2)
        self.assertIsNone(j4.claim_cdc_bundle(self.con,'events',2,self.cfg,self.runtime))
        self.assertEqual(self.con.execute('SELECT count(*) FROM deliveries').fetchone()[0],2)
        cfg=dict(self.cfg,merge_visibility_per_sink=3,max_inflight_deliveries=2)
        self.assertIsNone(j4.claim_cdc_bundle(self.con,'events',2,cfg,self.runtime))
        cfg=dict(self.cfg,merge_visibility_per_sink=3,max_prepared_bytes=1)
        self.assertIsNone(j4.claim_cdc_bundle(self.con,'events',2,cfg,self.runtime))
        self.assertEqual(j4.claim_cdc_bundle(self.con,'events',0,self.cfg,self.runtime),'one')

    def test_single_poll_pending_never_sleeps_and_closes_connection(self):
        remote=MagicMock();cursor=remote.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value=('PREPARE',);cursor.description=[('TransactionStatus',)]
        with patch.object(j4,'mysql_connect',return_value=remote),patch.object(self.runtime['stop'],'wait',side_effect=AssertionError('poll slept')):
            self.assertEqual(j4.wait_visible({'sr':{'database':'synthetic'}},99,self.runtime['stop'],once=True),('pending',{'TransactionStatus':'PREPARE'}))
        remote.close.assert_called_once()

    def test_all_shared_transaction_parts_remain_pending_until_exact_visibility(self):
        self.delivery('shared',0,99)
        self.con.execute("INSERT INTO load_parts(delivery_id,part,label,payload,nrows,txn_id) VALUES('shared',1,'local-second',X'00',1,99)")
        with patch.object(j4,'curl_request',side_effect=AssertionError('HTTP replay')),patch.object(j4,'wait_visible',return_value=('pending',{})) as poll:
            self.assertIsNone(j4.merge_async_delivery(None,self.con,self.mapping,'shared',self.cfg,self.runtime))
            self.assertEqual(poll.call_count,1)
            self.assertEqual(self.con.execute('SELECT SUM(visible) FROM load_parts').fetchone()[0],0)
        with patch.object(j4,'curl_request',side_effect=AssertionError('HTTP replay')),patch.object(j4,'wait_visible',return_value=('visible',{})) as poll:
            self.assertEqual(len(j4.merge_async_delivery(None,self.con,self.mapping,'shared',self.cfg,self.runtime)),2)
            self.assertEqual(poll.call_count,1)
        self.assertEqual(self.con.execute('SELECT SUM(visible) FROM load_parts').fetchone()[0],2)

    def test_confirmed_data_quality_abort_is_fatal_and_not_acknowledged(self):
        self.delivery('aborted',0,99)
        with patch.object(j4,'wait_visible',return_value=('aborted',{'Message':'data quality error'})),self.process_context():
            with self.assertRaises(ValueError):j4.process_merge_lane(self.con,{},None,'events',0,self.cfg,self.runtime)
        self.assertEqual(j4.lane_blocking_delivery(self.con,'events',0),'aborted')
        self.assertEqual(self.con.execute('SELECT visible FROM load_parts').fetchone()[0],0)

    def test_unknown_response_stays_quarantined_with_no_visibility_poll(self):
        case=accepted_test.MergeAcceptedContentionTest();case.setUp()
        try:
            cfg=dict(case.cfg,merge_visibility_pipeline=True)
            with patch.object(j4,'curl_request',side_effect=j4.pycurl.error(52,'empty reply')) as send, \
                 patch.object(j4,'wait_visible',side_effect=AssertionError('unknown result polled')):
                with self.assertRaises(RuntimeError) as caught:
                    j4.merge_async_delivery(None,case.con,case.mapping,'delivery',cfg,case.runtime)
                self.assertTrue(j4.quarantine_merge_table(case.con,'events',case.runtime,caught.exception))
                self.assertFalse(j4.process_merge_lane(case.con,{},None,'events',0,cfg,case.runtime))
                case.runtime['quarantined_tables'].clear()
                self.assertEqual(j4.quarantine_pending_merges(case.con,case.runtime),1)
                self.assertFalse(j4.process_merge_lane(case.con,{},None,'events',0,cfg,case.runtime))
                self.assertEqual(send.call_count,1)
            self.assertEqual(case.con.execute('SELECT visible,txn_id FROM load_parts').fetchall(),[(0,None)])
            self.assertEqual(len(case.marker()),1)
        finally:case.doCleanups()


if __name__=='__main__':unittest.main()
