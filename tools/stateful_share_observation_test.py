#!/usr/bin/env python3
"""Real WAL idle-observation writes, durable maxima, heartbeat and reopen."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import stateful_share_policy as policy


def open_db(path):
    con=sqlite3.connect(path,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=FULL")
    con.execute("PRAGMA busy_timeout=1")
    # Observation contract alone does not need compute-state triggers.
    con.execute("""CREATE TABLE IF NOT EXISTS stateful_share_observations(
        task_id TEXT PRIMARY KEY,samples INTEGER NOT NULL,
        max_leader_lag INTEGER NOT NULL,max_source_lag INTEGER NOT NULL,
        max_visible_lag INTEGER NOT NULL,copied_sequences INTEGER NOT NULL,
        created REAL NOT NULL,updated REAL NOT NULL)""")
    return con


class ObservationTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path=str(Path(self.directory.name)/"state.sqlite3")
        self.con=open_db(self.path)
        self.addCleanup(self.con.close)

    def observe(self,now,leader=10,follower=10,source=10,visible=10,copied=0,interval=1):
        with patch.object(policy.time,"time",return_value=now):
            return policy.observe(self.con,"follower",leader,follower,source,visible,
                copied_sequences=copied,unchanged_interval=interval)

    def test_unchanged_reads_do_not_write_and_need_no_writer_lock(self):
        first=self.observe(100)
        changes=self.con.total_changes
        other=open_db(self.path)
        try:
            other.execute("BEGIN IMMEDIATE")
            for index in range(19):
                self.assertEqual(self.observe(100+.05*(index+1)),first)
            self.assertEqual(self.con.total_changes,changes)
            # A changed peak still requires a real write; no unsafe omission.
            with self.assertRaises(sqlite3.OperationalError):
                self.observe(100.5,source=11)
            self.assertEqual(policy.observation_info(self.con,"follower"),first)
        finally:
            other.execute("ROLLBACK")
            other.close()
        second=self.observe(101)
        self.assertEqual(second["samples"],2)
        self.assertEqual(second["updated"],101)

    def test_every_peak_and_copy_is_durable_immediately_across_reopen(self):
        self.observe(100)
        self.assertEqual(self.observe(100.01,leader=13)["max_leader_lag"],3)
        self.assertEqual(self.observe(100.02,source=15)["max_source_lag"],5)
        self.assertEqual(self.observe(100.03,visible=8)["max_visible_lag"],2)
        self.assertEqual(self.observe(100.04,copied=2)["copied_sequences"],2)
        other=open_db(self.path)
        try:
            with patch.object(policy.time,"time",return_value=100.05):
                result=policy.observe(other,"follower",10,10,10,10,unchanged_interval=1)
            self.assertEqual(result["samples"],5)
            self.assertEqual(result["copied_sequences"],2)
            self.assertEqual(result["max_visible_lag"],2)
        finally:
            other.close()
        self.assertEqual(self.observe(100.06,copied=3)["copied_sequences"],5)

    def test_default_preserves_call_samples_clock_rollback_and_validation(self):
        self.observe(100,interval=0)
        self.assertEqual(self.observe(100,interval=0)["samples"],2)
        self.assertEqual(self.observe(99)["samples"],3)
        for value in [-1,float("nan"),float("inf")]:
            with self.assertRaises(ValueError):
                self.observe(100,interval=value)


if __name__=="__main__":
    unittest.main()
