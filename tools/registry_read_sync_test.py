#!/usr/bin/env python3
"""Registry no-ops must not acquire WAL writer; lifecycle checks stay coherent."""
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import physical_state_catalog
import stateful_physical_registry as registry
import shared_leader_snapshot_test as snapshots


class RegistryReadSyncTest(unittest.TestCase):
    def fixture(self,kind):
        return snapshots.SharedLeaderSnapshotTest().fixture(kind)

    def result(self,con,kind,follower):
        return registry.sync_runtime_result(
            con,dict(kind=kind,task=follower),dict(
                task=follower,shared_physical=True,
                shared_state_id="leader-state",visible_frontier=0))

    def test_existing_owner_ref_does_not_wait_for_writer(self):
        for kind in ("aggregate","join"):
            with self.subTest(kind=kind),self.fixture(kind) as f:
                con,writer,leader,_,_,_,_,_,_,_=f
                physical_kind="aggregate" if kind=="aggregate" else "inner_join"
                identity=registry.instance_id(physical_kind,leader)
                con.execute("PRAGMA busy_timeout=20")
                writer.execute("BEGIN IMMEDIATE")
                try:
                    physical_state_catalog.retain_state(
                        con,identity,leader["task_id"],"owner")
                    self.assertFalse(con.in_transaction)
                    with self.assertRaises(sqlite3.OperationalError):
                        physical_state_catalog.retain_state(
                            con,identity,"new-owner","owner")
                    self.assertFalse(con.in_transaction)
                finally:
                    writer.rollback()
                physical_state_catalog.retain_state(
                    con,identity,"new-owner","owner")
                self.assertTrue(any(r["owner_id"]=="new-owner" for r in
                                    physical_state_catalog.state_refs(con,identity)))

    def test_shared_sync_does_not_wait_for_writer(self):
        for kind in ("aggregate","join"):
            with self.subTest(kind=kind),self.fixture(kind) as f:
                con,writer,_,follower,_,_,_,_,_,_=f
                physical_kind="aggregate" if kind=="aggregate" else "inner_join"
                con.execute("PRAGMA busy_timeout=20")
                writer.execute("BEGIN IMMEDIATE")
                try:
                    state=self.result(con,physical_kind,follower)
                    self.assertEqual(state["metadata"][kind+"_state_id"],"leader-state")
                    self.assertFalse(con.in_transaction)
                finally:
                    writer.rollback()

    def test_promotion_between_binding_and_stream_reads_is_coherent(self):
        for kind in ("aggregate","join"):
            with self.subTest(kind=kind),self.fixture(kind) as f:
                con,writer,leader,follower,_,runtime,_,_,_,_=f
                physical_kind="aggregate" if kind=="aggregate" else "inner_join"
                original=registry._shared_binding
                published=[]
                def read_then_promote(connection,task_kind,task_id):
                    binding=original(connection,task_kind,task_id)
                    if connection is con and not published:
                        writer.execute("PRAGMA busy_timeout=20")
                        runtime.promote_followers(writer,leader)
                        self.assertIsNone(original(writer,task_kind,task_id))
                        published.append(True)
                    return binding
                with patch.object(registry,"_shared_binding",side_effect=read_then_promote):
                    old=self.result(con,physical_kind,follower)
                self.assertEqual(published,[True])
                self.assertEqual(old["metadata"][kind+"_state_id"],"leader-state")
                self.assertFalse(con.in_transaction)
                new=self.result(con,physical_kind,follower)
                self.assertEqual(new["metadata"][kind+"_state_id"],follower["state_id"])

    def test_missing_ref_and_stream_mismatch_still_fail_closed(self):
        for kind in ("aggregate","join"):
            with self.subTest(kind=kind),self.fixture(kind) as f:
                con,writer,leader,follower,_,_,_,_,_,_=f
                physical_kind="aggregate" if kind=="aggregate" else "inner_join"
                identity=registry.instance_id(physical_kind,leader)
                physical_state_catalog.release_state(
                    writer,identity,follower["task_id"],"dependency")
                with self.assertRaisesRegex(RuntimeError,"dependency ref is missing"):
                    self.result(con,physical_kind,follower)
                self.assertFalse(con.in_transaction)
                physical_state_catalog.retain_state(
                    writer,identity,follower["task_id"],"dependency")
                writer.execute("UPDATE "+kind+"_output_streams SET state_id=? WHERE consumer_id=?",
                               ("wrong-state",follower["consumer_id"]))
                with self.assertRaisesRegex(RuntimeError,"stream state disagree"):
                    self.result(con,physical_kind,follower)
                self.assertFalse(con.in_transaction)


if __name__=="__main__":
    unittest.main()
