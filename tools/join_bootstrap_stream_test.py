#!/usr/bin/env python3
"""Independent full-pair oracle and real WAL atomic streamed bootstrap tests."""
from pathlib import Path
import contextlib
import pickle
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/"tools")]
import join_job_bridge_test as fixture
import join_outbox
import join_state


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA foreign_keys=ON")
    join_state.install(con)
    join_outbox.install(con)
    return con


def setup_state(con,left=8,right=9):
    spec=fixture.spec()
    spec["sources"]["right"]["join_key"]=["bucket"]
    join_state.begin_bootstrap(con,"state",spec,0)
    join_state.apply_bootstrap_chunk(con,"state",0,"left",
        [dict(id=i,customer_id=42,amount=7) for i in range(left)]
        +[dict(id=left,customer_id=None,amount=8)],None,True)
    join_state.apply_bootstrap_chunk(con,"state",0,"right",
        [dict(id=i,bucket=42,name="same") for i in range(right)]
        +[dict(id=right,bucket=None,name="null")],None,True)
    return spec


class JoinStreamTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path=str(Path(self.temporary.name)/"join.sqlite3")
        self.con=open_db(self.path)
        self.addCleanup(self.con.close)
        self.spec=setup_state(self.con)

    def seed(self):
        return join_outbox.seed_bootstrap(self.con,"consumer","state",1,"generation",0)

    def expected(self):
        # Preserve the old full-relation enumerator as the independent oracle.
        return [(item["pair_id"],0,pickle.dumps(item["row"],protocol=5))
                for item in join_state.read_pairs(self.con,"state")]

    def test_pairs_digest_bag_null_and_exact_retry(self):
        expected=self.expected()
        self.assertEqual(len(expected),72)
        with patch.object(join_state,"read_pairs",side_effect=AssertionError("full cache called")):
            actual=list(join_state.iter_pairs(self.con,"state"))
            commit=self.seed()
            retry=self.seed()
        self.assertEqual(sorted((x["pair_id"],x["row"]) for x in actual),
                         sorted((pair,pickle.loads(payload)) for pair,_,payload in expected))
        self.assertEqual(commit["nrows"],72)
        self.assertEqual(commit["digest"],join_outbox._digest("bootstrap",expected))
        self.assertEqual(retry,commit)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_output_identities").fetchone()[0],72)
        self.assertEqual(len(join_outbox.commit_rows(self.con,"consumer",0)),72)
        # Changed bytes at the same cut must fail exact retry, not be accepted
        # based on a matching watermark and row count.
        self.con.execute("UPDATE join_rows SET row_payload=? WHERE state_id='state' AND side='right' AND join_blob IS NOT NULL",
                         (pickle.dumps(dict(id=0,bucket=42,name="changed"),protocol=5),))
        with self.assertRaisesRegex(RuntimeError,"different payload"):
            self.seed()
        self.assertEqual(join_outbox.commit_info(self.con,"consumer",0),commit)

    def test_projection_seed_matches_full_state_oracle(self):
        target=dict(self.spec,projections=[self.spec["projections"][0]])
        expected=[(pair,op,pickle.dumps({"customer_name":pickle.loads(payload)["customer_name"]},protocol=5))
                  for pair,op,payload in self.expected()]
        commit=join_outbox.seed_bootstrap_projected(self.con,"projection","state",2,"projected",0,target)
        self.assertEqual(commit["digest"],join_outbox._digest("bootstrap",expected))
        self.assertEqual(commit["nrows"],72)
        self.assertTrue(all(row["row"]=={"customer_name":"same"} for row in join_outbox.commit_rows(self.con,"projection",0)))

    def test_mid_stream_exception_rolls_back_header_rows_and_identities(self):
        iterator=join_state.iter_pairs
        other=sqlite3.connect(self.path,isolation_level=None)
        self.addCleanup(other.close)
        def fail(con,state_id):
            for index,item in enumerate(iterator(con,state_id)):
                if index==10:
                    # Another WAL reader cannot see a partially seeded commit.
                    self.assertEqual(other.execute("SELECT COUNT(*) FROM join_output_commits").fetchone()[0],0)
                    raise RuntimeError("synthetic interrupted seed")
                yield item
        with patch.object(join_state,"iter_pairs",side_effect=fail):
            with self.assertRaisesRegex(RuntimeError,"interrupted"):
                self.seed()
        self.assertFalse(self.con.in_transaction)
        for table in ["join_output_commits","join_output_rows","join_output_identities"]:
            self.assertEqual(self.con.execute("SELECT COUNT(*) FROM "+table).fetchone()[0],0)
        self.con.close()
        self.con=open_db(self.path)
        self.assertEqual(self.seed()["nrows"],72)

    def test_identity_collision_rolls_back_streamed_seed(self):
        with patch.object(join_outbox,"target_id",return_value="collision"):
            with self.assertRaisesRegex(RuntimeError,"collision"):
                self.seed()
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_output_commits").fetchone()[0],0)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_output_identities").fetchone()[0],0)
        self.assertEqual(self.seed()["nrows"],72)

    def test_fixed_w_revalidated_after_write_lock_acquisition(self):
        join_outbox.ensure_stream(self.con,"consumer","state",1,"generation",0)
        original=join_outbox.transaction
        @contextlib.contextmanager
        def advance(con):
            con.execute("UPDATE join_states SET watermark=1 WHERE state_id='state'")
            with original(con):
                yield
        with patch.object(join_outbox,"transaction",side_effect=advance):
            with self.assertRaisesRegex(RuntimeError,"changed before"):
                self.seed()
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_output_commits").fetchone()[0],0)


if __name__=="__main__":
    unittest.main()
