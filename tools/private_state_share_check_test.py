#!/usr/bin/env python3
"""Existing private owners need no writer lock during sharing checks."""
import contextlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import j4
import aggregate_ir
import aggregate_state
import aggregate_task_catalog
import aggregate_shared_runtime
import aggregate_shared_runtime_test
import join_ir
import join_state
import join_task_catalog
import join_shared_runtime
import join_shared_runtime_test


class PrivateStateShareCheckTest(unittest.TestCase):
    def fixtures(self):
        return [
            ("aggregate",aggregate_shared_runtime,aggregate_state,
             aggregate_task_catalog,aggregate_shared_runtime_test,
             aggregate_shared_runtime_test.ir,aggregate_ir.state_spec),
            ("join",join_shared_runtime,join_state,join_task_catalog,
             join_shared_runtime_test,join_shared_runtime_test.plan,
             join_ir.state_spec),
        ]

    @contextlib.contextmanager
    def fixture(self,parts,complete=None):
        name,runtime,states,catalog,fixture,ir_fn,spec_fn=parts
        with tempfile.TemporaryDirectory() as td:
            path=str(Path(td)/"state.sqlite3")
            con=j4.init_state(path)
            ir=ir_fn()
            task=catalog.register_task(
                con,"private","sink",1,ir,"sink","private_state",
                "consumer",fixture.target_schema())
            if complete is not None:
                if complete:
                    states.create_state(con,task["state_id"],spec_fn(ir),0)
                else:
                    states.begin_bootstrap(con,task["state_id"],spec_fn(ir),0)
            try:
                yield con,path,task,spec_fn(ir),runtime,states
            finally:
                con.close()

    def test_complete_and_building_private_states_read_under_competing_writer(self):
        for parts in self.fixtures():
            for complete in [False,True]:
                with self.subTest(kind=parts[0],complete=complete),self.fixture(parts,complete) as f:
                    con,path,task,_,runtime,_=f
                    writer=j4.open_state(path)
                    try:
                        writer.execute("BEGIN IMMEDIATE")
                        con.execute("PRAGMA busy_timeout=0")
                        statements=[]
                        changes=con.total_changes
                        con.set_trace_callback(statements.append)
                        for _ in range(20):
                            self.assertIsNone(runtime.try_bind(con,task))
                        self.assertFalse(con.in_transaction)
                        self.assertFalse(any(s.lstrip().upper().startswith("BEGIN") for s in statements))
                        self.assertEqual(con.total_changes,changes)
                    finally:
                        con.set_trace_callback(None)
                        writer.execute("ROLLBACK")
                        writer.close()

    def test_private_creation_between_read_and_lock_is_rechecked(self):
        for parts in self.fixtures():
            with self.subTest(kind=parts[0]),self.fixture(parts) as f:
                con,path,task,spec,runtime,states=f
                original=states.transaction
                @contextlib.contextmanager
                def boundary(connection):
                    with patch.object(states,"transaction",original):
                        writer=j4.open_state(path)
                        try:
                            states.begin_bootstrap(writer,task["state_id"],spec,0)
                        finally:
                            writer.close()
                    with original(connection):
                        yield
                with patch.object(states,"transaction",boundary),patch.object(
                    runtime,"_leader_candidates",side_effect=AssertionError("must recheck private state")):
                    self.assertIsNone(runtime.try_bind(con,task))
                self.assertFalse(con.in_transaction)
                self.assertIsNone(runtime.maybe_binding(con,task["task_id"]))

    def test_corrupt_private_state_still_fails_closed(self):
        for parts in self.fixtures():
            with self.subTest(kind=parts[0]),self.fixture(parts,True) as f:
                con,_,task,_,runtime,_=f
                table="aggregate_states" if parts[0]=="aggregate" else "join_states"
                con.execute("UPDATE "+table+" SET spec_hash='corrupt' WHERE state_id=?",(task["state_id"],))
                with self.assertRaises(RuntimeError):
                    runtime.try_bind(con,task)
                self.assertIsNone(runtime.maybe_binding(con,task["task_id"]))
                self.assertFalse(con.in_transaction)


if __name__=="__main__":
    unittest.main()
