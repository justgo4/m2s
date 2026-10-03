#!/usr/bin/env python3
"""Bounded bootstrap buffers preserve full pairs, identities and rollback."""
import hashlib
from pathlib import Path
import pickle
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
import join_bootstrap_stream_test as fixture
import join_outbox
import join_state
import source_state


class JoinBootstrapBulkTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.con=fixture.open_db(str(Path(self.temp.name)/"state.sqlite3"))
        self.con.execute("PRAGMA temp_store=FILE")
        self.addCleanup(self.con.close)
        fixture.setup_state(self.con,34,37)

    def seed(self):
        return join_outbox.seed_bootstrap(self.con,"consumer","state",1,"generation",0)

    def expected(self):
        return [(item["pair_id"],0,pickle.dumps(item["row"],protocol=5))
                for item in join_state.read_pairs(self.con,"state")]

    def assert_empty(self):
        for table in ["join_output_commits","join_output_rows","join_output_identities"]:
            self.assertEqual(self.con.execute("SELECT COUNT(*) FROM "+table).fetchone()[0],0)

    def test_full_oracle_hash_identities_and_exact_retry_across_batches(self):
        expected=self.expected()
        result=self.seed()
        self.assertEqual(result["nrows"],1258)
        self.assertEqual(result["digest"],join_outbox._digest("bootstrap",expected))
        actual=self.con.execute("SELECT pair_id,op,row_payload FROM join_output_rows ORDER BY pair_id").fetchall()
        self.assertEqual(actual,expected)
        identities=self.con.execute("SELECT pair_id,target_id FROM join_output_identities ORDER BY pair_id").fetchall()
        self.assertEqual(identities,[(pair,hashlib.sha256(pair).hexdigest()) for pair,_,_ in expected])
        self.assertEqual(self.seed(),result)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_bootstrap_identity_stage").fetchone()[0],0)

    def test_default_temp_mode_can_still_be_configured_for_source_apply(self):
        self.con.execute("PRAGMA temp_store=DEFAULT")
        self.seed()
        self.assertIsNone(self.con.execute("SELECT name FROM sqlite_temp_master LIMIT 1").fetchone())
        self.assertEqual(source_state.require_file_temp_store(self.con)["mode"],1)

    def test_unconfigured_existing_temp_objects_are_not_destroyed(self):
        self.con.execute("PRAGMA temp_store=DEFAULT")
        self.con.execute("CREATE TEMP TABLE caller_owned(v)")
        self.con.execute("INSERT INTO caller_owned VALUES(99)")
        self.assertEqual(self.seed()["nrows"],1258)
        self.assertEqual(self.con.execute("SELECT v FROM caller_owned").fetchall(),[(99,)])
        self.assertIsNone(self.con.execute("SELECT name FROM sqlite_temp_master WHERE name='join_bootstrap_identity_stage'").fetchone())

    def test_buffer_row_and_byte_limits_and_single_oversized_row(self):
        original=join_outbox._insert_bootstrap_batch_locked
        batches=[]
        def record(con,consumer,cut,identity,rows):
            batches.append((len(rows),sum(len(pair)+len(payload)+80 for pair,_,payload in rows)))
            return original(con,consumer,cut,identity,rows)
        with patch.object(join_outbox,"_insert_bootstrap_batch_locked",side_effect=record):
            self.seed()
        self.assertEqual([count for count,_ in batches],[1000,258])
        self.assertTrue(all(size<=1024*1024 for _,size in batches))
        expected=self.expected()
        wide=[(pair,op,b"x"*(2*1024*1024 if i==0 else 20000))
              for i,(pair,op,_) in enumerate(expected[:100])]
        join_outbox.ensure_stream(self.con,"wide","state",2,"wide_generation",0)
        batches.clear()
        with patch.object(join_outbox,"_insert_bootstrap_batch_locked",side_effect=record):
            result=join_outbox._seed_bootstrap_stream(self.con,"wide",0,"state",join_state.state_info(self.con,"state")["spec_hash"],(row for row in wide))
        self.assertEqual(result["digest"],join_outbox._digest("bootstrap",sorted(wide)))
        self.assertGreater(len(batches),2)
        self.assertEqual(batches[0][0],1)
        self.assertTrue(all(count==1 or size<=1024*1024 for count,size in batches))

    def test_collision_within_and_across_batches_rolls_back_whole_seed(self):
        pairs=[item["pair_id"] for item in join_state.iter_pairs(self.con,"state")]
        original=join_outbox.target_id
        for second in [1,1001]:
            with self.subTest(second=second):
                collide={pairs[0],pairs[second]}
                def identity(pair,format=join_outbox.DEFAULT_IDENTITY_FORMAT):
                    return "forced-collision" if bytes(pair) in collide else original(pair,format)
                with patch.object(join_outbox,"target_id",side_effect=identity):
                    with self.assertRaisesRegex(RuntimeError,"identity collision"):
                        self.seed()
                self.assert_empty()
        self.assertEqual(self.seed()["nrows"],1258)

    def test_sql_failure_after_partial_batch_rolls_back_and_exact_retry_succeeds(self):
        self.con.execute("""
            CREATE TRIGGER injected_bootstrap_failure BEFORE INSERT ON join_output_rows
            WHEN (SELECT COUNT(*) FROM join_output_rows)>=1100
            BEGIN SELECT RAISE(ABORT,'injected SQL batch failure'); END
        """)
        with self.assertRaisesRegex(sqlite3.IntegrityError,"injected SQL batch failure"):
            self.seed()
        self.assert_empty()
        self.con.execute("DROP TRIGGER injected_bootstrap_failure")
        result=self.seed()
        self.assertEqual(result["digest"],join_outbox._digest("bootstrap",self.expected()))

    def test_legacy_format_and_preexisting_collision_remain_exact(self):
        join_outbox.ensure_stream(self.con,"consumer","state",1,"generation",0)
        self.con.execute("UPDATE join_output_streams SET identity_format=?",(join_outbox.IDENTITY_FORMAT_LEGACY,))
        result=self.seed()
        self.assertEqual(result["digest"],join_outbox._digest("bootstrap",self.expected()))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_output_identities").fetchone()[0],0)
        join_outbox.ensure_stream(self.con,"collision","state",2,"collision_generation",0)
        pair=self.expected()[0][0]
        self.con.execute("INSERT INTO join_output_identities VALUES(?,?,?,?)",("collision",hashlib.sha256(pair).hexdigest(),b"foreign-pair",123))
        with self.assertRaisesRegex(RuntimeError,"identity collision"):
            join_outbox.seed_bootstrap(self.con,"collision","state",2,"collision_generation",0)
        self.assertEqual(self.con.execute("SELECT pair_id,created FROM join_output_identities WHERE consumer_id='collision'").fetchall(),[(b"foreign-pair",123.0)])
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM join_output_commits WHERE consumer_id='collision'").fetchone()[0],0)


if __name__=="__main__":
    unittest.main()
