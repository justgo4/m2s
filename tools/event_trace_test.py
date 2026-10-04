#!/usr/bin/env python3
"""Real durable boundaries, wire uncertainty and bounded correlated observations."""
import contextlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import pyarrow as pa
import cdc_event_trace as trace
import j4
import source_state
import event_trace_report as report
import merge_accepted_contention_test as wire


class EventTraceTest(unittest.TestCase):
    def setUp(self):
        trace.configure('private-source-epoch',enabled=True,every=1,limit=256)
        self.addCleanup(trace.configure,'disabled')

    def events(self):
        return trace.snapshot()['events']

    def test_disabled_has_zero_queries_and_no_metadata_validation(self):
        trace.configure('disabled')
        class NoReads:
            def execute(self,*args):raise AssertionError('disabled observer queried DB')
        self.assertIsNone(trace.select_delivery(NoReads(),'private-target','private-delivery'))
        trace.record('unknown',private_payload='private')
        self.assertEqual(trace.snapshot(),{'enabled':False})

    def test_bounded_ring_sampling_and_no_raw_identity_payload(self):
        trace.configure('private-source-epoch',enabled=True,every=2,limit=3)
        for seq in range(20):trace.record('source_durable',seq=seq)
        saved=trace.snapshot()
        self.assertEqual([event['seq'] for event in saved['events']],[14,16,18])
        self.assertEqual(saved['dropped'],7)
        self.assertNotIn('private-source-epoch',json.dumps(saved))
        with self.assertRaises(ValueError):trace.record('source_durable',seq=20,payload='private')
        with self.assertRaises(ValueError):trace.record('source_durable',seq=20,duration_seconds=float('nan'))

    def test_concurrent_ids_and_restart_instances_cannot_merge_clocks(self):
        trace.configure('epoch',enabled=True,every=1,limit=1000)
        threads=[threading.Thread(target=lambda:[trace.record('source_durable',seq=i) for i in range(100)]) for _ in range(4)]
        for thread in threads:thread.start()
        for thread in threads:thread.join(5)
        old=trace.snapshot()
        self.assertEqual(len({event['id'] for event in old['events']}),400)
        trace.configure('epoch',enabled=True,every=1)
        trace.record('base_applied',seq=1)
        new=trace.snapshot()
        self.assertNotEqual(old['instance'],new['instance'])
        self.assertEqual(report.analyze([old,new])['instances'],2)

    def test_source_hooks_emit_only_after_real_commit_and_apply(self):
        with tempfile.TemporaryDirectory() as directory:
            con=j4.init_state(str(Path(directory)/'state.sqlite3'))
            try:
                source_state.register_relation(con,'private-source','private-source-epoch',
                    pa.schema([pa.field('id',pa.int64())]),['id'])
                batch=pa.table({'id':[1],'_sync_op':pa.array([0],type=pa.int8()),
                                '_sync_order':pa.array([0],type=pa.int64())})
                part=source_state.prepare_part('private-source',batch)
                j4.meta_set(con,'read_position',('private-binlog.000001',4))
                def commit():
                    with tempfile.TemporaryFile() as spool:
                        return j4.commit_spool(con,spool,('private-binlog.000001',100),123,{},
                            source_parts=[part],source_epoch='private-source-epoch')
                con.set_authorizer(lambda action,name,*args:
                    sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_INSERT and name=='source_commits'
                    else sqlite3.SQLITE_OK)
                with self.assertRaises(sqlite3.DatabaseError):commit()
                con.set_authorizer(None)
                self.assertEqual(self.events(),[])
                commit()
                self.assertFalse(con.in_transaction)
                self.assertEqual(self.events()[0]['stage'],'source_durable')
                self.assertEqual(source_state.log_durable_seq(con),1)
                con.set_authorizer(lambda action,name,*args:
                    sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_UPDATE and name=='source_pipeline_stats'
                    else sqlite3.SQLITE_OK)
                with self.assertRaises(sqlite3.DatabaseError):source_state.apply_one(con,1)
                con.set_authorizer(None)
                self.assertEqual([e['stage'] for e in self.events()],['source_durable'])
                self.assertEqual(source_state.base_applied_seq(con),0)
                self.assertTrue(source_state.apply_one(con,1))
                self.assertFalse(source_state.apply_one(con,1))
                commit() # validated exact replay must not invent another durable observation
                self.assertEqual([e['stage'] for e in self.events()],['source_durable','base_applied'])
            finally:con.close()

    def test_real_membership_survives_ack_deletion_and_context_is_thread_local(self):
        temporary=tempfile.TemporaryDirectory()
        con=j4.init_state(str(Path(temporary.name)/'state.sqlite3'))
        cfg=dict(key_partitions=2,batch_rows=1000,batch_bytes=1000,
                 max_inflight_deliveries=4,max_prepared_bytes=1024*1024,max_row_bytes=1000)
        runtime=dict(control_lock=threading.Lock(),active_writers={'sink':1})
        try:
            for lane,seq in [(0,1),(1,2)]:
                con.execute("INSERT INTO jobs(table_name,lane,kind,payload,nrows,logical_bytes,source_seq,created) VALUES('sink',?,'cdc',X'00',1,10,?,0)",(lane,seq))
            delivery=j4.claim_cdc_bundle(con,'sink',0,cfg,runtime)
            token=trace.select_delivery(con,'private-target',delivery)
            thread=threading.Thread(target=lambda:trace.record('http_begin',part=0))
            thread.start();thread.join(5)
            self.assertEqual(len(self.events()),1)
            con.execute('UPDATE deliveries SET prepared=1 WHERE id=?',(delivery,))
            trace.record('ack_begin');j4.acknowledge_delivery(con,delivery);trace.record('ack_end')
            trace.finish_delivery(token)
            self.assertEqual(con.execute('SELECT count(*) FROM active_jobs').fetchone()[0],0)
            self.assertEqual(self.events()[0]['sequences'],[1,2])
            saved=trace.snapshot()
            saved['events'][0]['sequences'].append(999)
            self.assertEqual(self.events()[0]['sequences'],[1,2])
            self.assertNotIn('private-target',json.dumps(trace.snapshot()))
            self.assertIsNone(trace.CURRENT.get())
            result=report.analyze([trace.snapshot(),trace.snapshot()])
            self.assertEqual(result['unique_events'],3)
            self.assertEqual(result['complete_delivery_observations'],1)
            self.assertFalse(result['certification'])
        finally:con.close();temporary.cleanup()

    def test_metadata_error_never_mutates_or_requires_writer(self):
        con=sqlite3.connect(':memory:',isolation_level=None)
        try:
            before=con.total_changes
            self.assertIsNone(trace.select_delivery(con,'target','delivery'))
            self.assertFalse(con.in_transaction)
            self.assertEqual(con.total_changes,before)
            self.assertEqual(trace.snapshot()['metadata_errors'],1)
        finally:con.close()

    def test_actual_http_success_persistence_and_unknown_never_become_visible(self):
        case=wire.MergeAcceptedContentionTest();case.setUp()
        token=trace.CURRENT.set(dict(instance=trace.snapshot()['instance'],target=trace.identity('target'),delivery=trace.identity('delivery'),
                                    sequences=[1],sequences_truncated=False))
        try:
            with patch.object(j4,'curl_request',return_value=(200,case.result)) as send,contextlib.redirect_stdout(io.StringIO()):
                case.submit()
                self.assertEqual(send.call_count,1)
            self.assertEqual([e['stage'] for e in self.events()],['http_begin','http_accepted','acceptance_saved'])
            self.assertEqual(case.con.execute('SELECT txn_id FROM load_parts').fetchone()[0],99)
            trace.configure('epoch',enabled=True,every=1)
            trace.CURRENT.set(dict(instance=trace.snapshot()['instance'],target=trace.identity('target'),delivery=trace.identity('delivery'),
                                   sequences=[1],sequences_truncated=False))
            case.con.execute('UPDATE load_parts SET txn_id=NULL')
            with patch.object(j4,'curl_request',side_effect=j4.pycurl.error(52,'empty reply')) as send:
                with self.assertRaises(RuntimeError):case.submit()
                self.assertEqual(send.call_count,1)
            self.assertEqual([e['stage'] for e in self.events()],['http_begin'])
            self.assertTrue(case.marker())
            self.assertEqual(report.analyze([trace.snapshot()])['complete_delivery_observations'],0)
        finally:trace.CURRENT.reset(token);case.doCleanups()

    def test_frontier_dedup_is_bounded_and_generation_specific(self):
        for _ in range(10):trace.task_frontier('target','generation-1',2,.01)
        trace.task_frontier('target','generation-2',2,.01)
        self.assertEqual(len(self.events()),2)
        self.assertNotEqual(self.events()[0]['generation'],self.events()[1]['generation'])
        for number in range(200):trace.task_frontier('target'+str(number),'generation',2,.01)
        self.assertEqual(len(trace.STATE['frontiers']),128)

    def test_report_does_not_add_unrelated_quantiles_or_negative_clock_observations(self):
        events=[dict(id=1,stage='source_durable',seq=1,mono=10),
                dict(id=2,stage='base_applied',seq=1,mono=9),
                dict(id=3,stage='delivery_selected',target='t',delivery='d',sequences=[1],mono=12),
                dict(id=4,stage='prepare_begin',target='t',delivery='d',mono=12),
                dict(id=5,stage='prepare_end',target='t',delivery='d',mono=13),
                dict(id=6,stage='ack_end',target='t',delivery='d',mono=15)]
        snapshot=dict(enabled=True,epoch='e',instance='i',dropped=7,events=events)
        result=report.analyze([snapshot,snapshot])
        self.assertEqual(result['stages']['source_to_ack']['p95'],5)
        self.assertEqual(result['stages']['prepare']['p95'],1)
        self.assertEqual(result['clock_order_conflicts'],1)
        self.assertEqual(result['ring_evictions'],7)
        self.assertEqual(report.analyze([snapshot],max_events=2)['analysis_dropped'],4)
        wrong=dict(snapshot,events=[dict(events[0],mono=11)])
        with self.assertRaises(ValueError):report.analyze([snapshot,wrong])


if __name__=='__main__':
    unittest.main()
