#!/usr/bin/env python3
"""Real WAL publication between related follower SELECTs stays coherent."""
import contextlib
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import j4
import source_state
import task_generation
import stateful_physical_registry
import aggregate_ir
import aggregate_state
import aggregate_log_consumer
import aggregate_task_catalog
import aggregate_shared_runtime
import aggregate_shared_runtime_test as af
import join_ir
import join_state
import join_log_consumer
import join_task_catalog
import join_shared_runtime
import join_shared_runtime_test as jf


class SharedLeaderSnapshotTest(unittest.TestCase):
    @contextlib.contextmanager
    def fixture(self,kind):
        with tempfile.TemporaryDirectory() as td:
            path=str(Path(td)/"state.sqlite3")
            con=j4.init_state(path)
            if kind=="aggregate":
                fixture,runtime,states,catalog,consumer=af,aggregate_shared_runtime,aggregate_state,aggregate_task_catalog,aggregate_log_consumer
                ir=af.ir()
                source_state.register_relation(con,"db.orders","source-a",af.source_schema(),["id"],schema_epoch=1)
                states.begin_bootstrap(con,"leader-state",aggregate_ir.state_spec(ir),0)
                states.bind_input_semantics(con,"leader-state",aggregate_ir.semantic_id(ir))
                states.apply_bootstrap_chunk(con,"leader-state",0,[dict(category="a",amount=Decimal("5.00"),_sync_op=0)],None,True)
            else:
                fixture,runtime,states,catalog,consumer=jf,join_shared_runtime,join_state,join_task_catalog,join_log_consumer
                ir=jf.plan()
                source_state.register_relation(con,"db.orders","source-join",jf.left_schema(),["id"],schema_epoch=1)
                source_state.register_relation(con,"db.customers","source-join",jf.right_schema(),["id"],schema_epoch=1)
                states.begin_bootstrap(con,"leader-state",join_ir.state_spec(ir),0)
                states.apply_bootstrap_chunk(con,"leader-state",0,"left",[dict(id=1,customer_id=10,amount=5)],None,True)
                states.apply_bootstrap_chunk(con,"leader-state",0,"right",[dict(id=10,name="alice")],None,True)
            leader=catalog.register_task(con,"leader","leader_sink",1,ir,"leader_sink","leader-state","leader-consumer",fixture.target_schema())
            if kind=="aggregate":
                task_generation.import_existing(con,leader["sink_key"],1,leader["source_relation"],"ready")
                consumer.ensure_consumer(con,leader["consumer_id"],leader["source_relation"],1,ir,leader["state_id"],0,generation_id=leader["generation_id"])
            else:
                task_generation.import_existing_multi(con,leader["sink_key"],1,leader["source_relations"],"ready")
                consumer.ensure_consumer(con,leader["consumer_id"],1,ir,leader["state_id"],0,generation_id=leader["generation_id"])
            leader=catalog.set_status(con,leader["task_id"],"active")
            stateful_physical_registry.sync_ready(con,"aggregate" if kind=="aggregate" else "inner_join",leader,0)
            follower=catalog.register_task(con,"follower","follower_sink",2,ir,"follower_sink","follower-state","follower-consumer",fixture.target_schema())
            self.assertIsNotNone(runtime.try_bind(con,follower))
            bridge=runtime.aggregate_job_bridge if kind=="aggregate" else runtime.join_job_bridge
            with patch.object(bridge,"stage_pending",side_effect=fixture.make_visible):
                runtime.step(con,follower,fixture.mapping(follower),{})
            writer=j4.open_state(path)
            try:
                yield con,writer,leader,follower,ir,runtime,states,consumer,fixture,bridge
            finally:
                writer.close()
                con.close()

    def race(self,kind):
        with self.fixture(kind) as f:
            con,writer,leader,follower,ir,runtime,states,consumer,fixture,bridge=f
            if kind=="aggregate":
                fixture.source_commit(writer,[(2,"a",Decimal("7.00"),0)],100)
            else:
                fixture.source_commit(writer,None,[dict(id=10,name="alice",_sync_op=1),dict(id=10,name="alicia",_sync_op=0)],100)
            original=source_state.consumer_info
            commits=[]
            def read_then_publish(connection,consumer_id):
                result=original(connection,consumer_id)
                if connection is con and consumer_id==leader["consumer_id"] and not commits:
                    self.assertEqual(result["watermark"],0)
                    consumer.process_next(writer,leader["consumer_id"],ir)
                    self.assertEqual(states.state_info(writer,leader["state_id"])["watermark"],1)
                    self.assertEqual(original(writer,leader["consumer_id"])["watermark"],1)
                    commits.append(1)
                return result
            with patch.object(source_state,"consumer_info",side_effect=read_then_publish),patch.object(bridge,"stage_pending",side_effect=fixture.make_visible):
                result=runtime.step(con,follower,fixture.mapping(follower),{})
            self.assertEqual(commits,[1])
            self.assertEqual(result["consumer"]["watermark"],0)
            self.assertFalse(con.in_transaction)
            with patch.object(bridge,"stage_pending",side_effect=fixture.make_visible):
                result=runtime.step(con,follower,fixture.mapping(follower),{})
            self.assertEqual(result["consumer"]["watermark"],1)
            self.assertFalse(con.in_transaction)
            outbox=runtime.aggregate_outbox if kind=="aggregate" else runtime.join_outbox
            self.assertEqual(outbox.commit_info(con,follower["consumer_id"],1)["digest"],outbox.commit_info(con,leader["consumer_id"],1)["digest"])

    def test_aggregate_atomic_publish_between_consumer_and_state_reads(self):
        self.race("aggregate")

    def test_join_atomic_publish_between_consumer_and_state_reads(self):
        self.race("join")

    def test_actual_persisted_divergence_remains_fatal(self):
        for kind in ["aggregate","join"]:
            with self.subTest(kind=kind),self.fixture(kind) as f:
                con,writer,leader,follower,_,runtime,_,_,fixture,_=f
                table="aggregate_states" if kind=="aggregate" else "join_states"
                writer.execute("UPDATE "+table+" SET watermark=1 WHERE state_id=?",(leader["state_id"],))
                with self.assertRaisesRegex(RuntimeError,"watermarks diverged"):
                    runtime.step(con,follower,fixture.mapping(follower),{})
                self.assertFalse(con.in_transaction)
                self.assertEqual(source_state.consumer_info(con,follower["consumer_id"])["watermark"],0)

    def test_snapshot_cleanup_and_caller_transaction_ownership(self):
        with tempfile.TemporaryDirectory() as td:
            con=j4.init_state(str(Path(td)/"state.sqlite3"))
            try:
                with self.assertRaisesRegex(ValueError,"injected"):
                    with source_state.read_snapshot(con):
                        con.execute("SELECT 1").fetchone()
                        raise ValueError("injected")
                self.assertFalse(con.in_transaction)
                con.execute("BEGIN IMMEDIATE")
                with source_state.read_snapshot(con):
                    con.execute("SELECT 1").fetchone()
                self.assertTrue(con.in_transaction)
                con.execute("ROLLBACK")
            finally:
                con.close()


if __name__=="__main__":
    unittest.main()
