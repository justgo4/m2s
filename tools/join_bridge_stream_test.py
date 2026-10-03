#!/usr/bin/env python3
"""Bounded Arrow batches, atomic spool rollback and whole-commit visibility."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))
import join_job_bridge_test as fixture
import j4
import join_job_bridge
import join_outbox
import join_state


class JoinBridgeStreamTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=str(Path(self.tmp.name)/"state.sqlite3")
        self.con=j4.init_state(self.path)
        self.addCleanup(lambda:self.con.close())
        join_state.install(self.con)
        join_state.create_state(self.con,"join-state",fixture.spec(),watermark=0)
        join_outbox.ensure_stream(self.con,"join-consumer","join-state",11,"generation",0)
        join_outbox.seed_bootstrap(self.con,"join-consumer","join-state",11,"generation",0)
        self.mapping=fixture.mapping()
        self.cfg=fixture.cfg(self.path)
        self.cfg["batch_rows"]=3
        self.deltas=[dict(pair_id=("pair-%03d"%i).encode(),op=i%2,
                          row=dict(customer_name="same",amount=7)) for i in range(17)]
        fixture.stage_delta(self.con,1,self.deltas)
        join_outbox.mark_visible(self.con,"join-consumer",0)

    def stage(self):
        return join_job_bridge.stage_commit(self.con,"join-consumer",getattr(self,"seq",1),self.mapping,self.cfg)

    def assert_payloads(self,result):
        actual=[]
        logical=0
        for job_id in result["job_ids"]:
            payload,nbytes=self.con.execute("SELECT payload,logical_bytes FROM jobs WHERE id=?",(job_id,)).fetchone()
            logical+=nbytes
            table=j4.arrow_job_table(self.mapping,payload)
            for row in table.to_pylist():
                actual.append((row[join_job_bridge.PAIR_COLUMN],row["_sync_op"],row["customer_name"],row["amount"]))
        expected=[(join_outbox.target_id(x["pair_id"]),x["op"],x["row"]["customer_name"],x["row"]["amount"])
                  for x in self.deltas]
        self.assertEqual(sorted(actual),sorted(expected))
        self.assertEqual(j4.meta_get(self.con,"pending_bytes",0),logical)

    def test_batches_preserve_bag_retractions_restart_and_final_ack_frontier(self):
        sizes=[]
        original=j4.raw_arrow
        def raw(mapping,rows):
            sizes.append(len(rows))
            self.assertLessEqual(len(rows),3)
            return original(mapping,rows)
        with patch.object(j4,"raw_arrow",side_effect=raw):
            result=self.stage()
        self.assertEqual(sizes,[3,3,3,3,3,2])
        self.assert_payloads(result)
        self.con.close()
        self.con=j4.open_state(self.path)
        self.assertEqual(self.stage()["job_ids"],result["job_ids"])
        for job_id in result["job_ids"][:-1]:
            j4.acknowledge_delivery(self.con,fixture.create_delivery(self.con,job_id))
            self.assertEqual(join_outbox.visible_frontier(self.con,"join-consumer"),0)
        j4.acknowledge_delivery(self.con,fixture.create_delivery(self.con,result["job_ids"][-1]))
        self.assertEqual(join_outbox.visible_frontier(self.con,"join-consumer"),1)
        self.assertEqual(j4.meta_get(self.con,"pending_bytes",0),0)

    def test_configured_rows_above_old_cap_keep_exact_payload_and_last_ack(self):
        self.cfg["batch_rows"]=5000
        self.cfg["batch_bytes"]=16*1024*1024
        self.cfg["max_row_bytes"]=64*1024*1024
        self.deltas=[dict(pair_id=("large-%05d"%i).encode(),op=i%2,
                          row=dict(customer_name="same",amount=7)) for i in range(5001)]
        join_outbox.mark_visible(self.con,"join-consumer",1)
        self.seq=2
        fixture.stage_delta(self.con,2,self.deltas)
        sizes=[]
        original=j4.raw_arrow
        def raw(mapping,rows):
            sizes.append(len(rows))
            return original(mapping,rows)
        with patch.object(j4,"raw_arrow",side_effect=raw):
            result=self.stage()
        self.assertEqual(sizes,[5000,1])
        self.assert_payloads(result)
        self.con.close()
        self.con=j4.open_state(self.path)
        self.assertEqual(self.stage()["job_ids"],result["job_ids"])
        for job in result["job_ids"][:-1]:
            j4.acknowledge_delivery(self.con,fixture.create_delivery(self.con,job))
            self.assertEqual(join_outbox.visible_frontier(self.con,"join-consumer"),1)
        j4.acknowledge_delivery(self.con,fixture.create_delivery(self.con,result["job_ids"][-1]))
        self.assertEqual(join_outbox.visible_frontier(self.con,"join-consumer"),2)

    def test_payload_byte_budget_and_singleton_oversize(self):
        self.cfg["batch_rows"]=4096
        self.cfg["batch_bytes"]=256
        self.con.execute("UPDATE join_output_rows SET row_payload=? WHERE consumer_id='join-consumer' AND pair_id=?",
                         (join_outbox.pickle.dumps(dict(customer_name="x"*2048,amount=7),protocol=5),self.deltas[0]["pair_id"]))
        self.deltas[0]["row"]["customer_name"]="x"*2048
        batches=list(join_job_bridge._mutation_batches(self.con,"join-consumer",1,self.mapping,self.cfg))
        self.assertEqual(len(batches[0]),1)
        self.assertTrue(all(len(x)<=2 for x in batches))
        self.assertEqual(sum(map(len,batches)),17)
        self.assert_payloads(self.stage())

    def test_real_truncated_spool_rolls_back_jobs_links_and_pending_bytes_then_restarts(self):
        original=j4.read_spool_record
        calls=[]
        def truncated(spool):
            if not calls:
                spool.seek(0,2)
                spool.truncate(spool.tell()-1)
                spool.seek(0)
            calls.append(1)
            return original(spool)
        with patch.object(j4,"read_spool_record",side_effect=truncated):
            with self.assertRaisesRegex(RuntimeError,"truncated transaction spool"):
                self.stage()
        self.assertGreater(len(calls),1)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],0)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_job_links").fetchone()[0],0)
        self.assertEqual(j4.meta_get(self.con,"pending_bytes",0),0)
        self.assertFalse(join_outbox.commit_info(self.con,"join-consumer",1)["visible"])
        self.con.close()
        self.con=j4.open_state(self.path)
        self.assert_payloads(self.stage())

    def test_inconsistent_commit_row_count_never_publishes_jobs_or_empty_visibility(self):
        for count in (0,18):
            self.con.execute("UPDATE join_output_commits SET nrows=? WHERE consumer_id='join-consumer' AND source_seq=1",(count,))
            with self.assertRaises(RuntimeError):
                self.stage()
            self.assertEqual(self.con.execute("SELECT COUNT(*) FROM jobs").fetchone()[0],0)
            self.assertEqual(join_outbox.visible_frontier(self.con,"join-consumer"),0)


if __name__=="__main__":
    unittest.main()
