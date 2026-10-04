#!/usr/bin/env python3
"""Real WAL resource denials avoid writer acquisition; recovery still proceeds."""
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'tools'))
import j4
import merge_visibility_pipeline_test as pipeline


class AdmissionReadTest(pipeline.PipelineTest):
    def test_full_cap_under_competing_wal_writer_has_no_write_attempt(self):
        self.delivery('one',0,99);self.delivery('two',1,100);self.job(2)
        blocker=j4.open_state(self.path);self.addCleanup(blocker.close)
        blocker.execute('BEGIN IMMEDIATE');self.addCleanup(blocker.rollback)
        self.con.execute('PRAGMA busy_timeout=1')
        writes=[];self.con.set_trace_callback(lambda sql:writes.append(sql) if sql.upper().startswith('BEGIN') else None)
        for _ in range(100):
            self.assertIsNone(j4.claim_cdc_bundle(self.con,'events',2,self.cfg,self.runtime))
            self.assertIsNone(j4.claim_snapshot_bundle(self.con,'events',2,self.cfg,self.runtime))
        self.assertEqual(writes,[])
        self.assertFalse(self.con.in_transaction)
        self.assertEqual(self.con.execute('SELECT count(*) FROM job_assignments').fetchone()[0],2)

    def test_global_and_payload_limits_are_read_only_and_recovery_bypasses_cap(self):
        self.delivery('one',0,99);self.job(2)
        for cfg in (dict(self.cfg,max_inflight_deliveries=1),dict(self.cfg,max_prepared_bytes=1)):
            writes=[];self.con.set_trace_callback(lambda sql:writes.append(sql) if sql.upper().startswith('BEGIN') else None)
            self.assertIsNone(j4.claim_cdc_bundle(self.con,'events',2,cfg,self.runtime))
            self.assertIsNone(j4.claim_snapshot_bundle(self.con,'events',2,cfg,self.runtime))
            self.assertEqual(writes,[])
            self.assertEqual(j4.claim_cdc_bundle(self.con,'events',0,cfg,self.runtime),'one')
            self.assertEqual(len(writes),1)

    def test_default_off_preflight_has_no_reads(self):
        class NoReads:
            def execute(self,*args):raise AssertionError('disabled preflight queried')
        self.assertFalse(j4.merge_visibility_claim_deferred(NoReads(),'events',2,{}))

    def test_atomic_recheck_catches_resource_fill_after_advisory_read(self):
        self.job(2)
        self.assertFalse(j4.merge_visibility_claim_deferred(self.con,'events',2,self.cfg))
        self.delivery('one',0,99);self.delivery('two',1,100)
        from unittest.mock import patch
        with patch.object(j4,'merge_visibility_claim_deferred',return_value=False):
            self.assertIsNone(j4.claim_cdc_bundle(self.con,'events',2,self.cfg,self.runtime))
            self.assertIsNone(j4.claim_snapshot_bundle(self.con,'events',2,self.cfg,self.runtime))
        self.assertEqual(self.con.execute('SELECT count(*) FROM deliveries').fetchone()[0],2)


if __name__=='__main__':unittest.main()
