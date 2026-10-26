#!/usr/bin/env python3
"""Safeguards for fixed-revision configuration A/B/B/A experiments."""
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tools import config_abba as abba


class ConfigAbbaTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/"repo";self.root.mkdir()
        self.git("init","-q")
        self.git("config","user.email","synthetic@example.invalid")
        self.git("config","user.name","Synthetic")
        (self.root/"data").write_text("A")
        self.git("add","data");self.git("commit","-qm","A")
        self.sha=self.git("rev-parse","HEAD").strip()
        self.directory=Path(self.temp.name)/"results"

    def git(self,*args):
        return subprocess.check_output(
            ["git",*args],cwd=self.root,text=True,stderr=subprocess.DEVNULL)

    def test_exact_same_revision_abba(self):
        plan=abba.experiment_plan(
            self.sha,"CDC_BATCH_MS","1000","200",self.directory,
            root=self.root)
        self.assertEqual([x["revision"] for x in plan["order"]],[self.sha]*4)
        self.assertEqual([x["value"] for x in plan["order"]],
                         ["1000","200","200","1000"])
        self.assertEqual(plan["variable"],"CDC_BATCH_MS")
        self.assertFalse(plan["certification"])
        self.assertFalse(self.directory.exists())

    def test_environment_changes_only_selected_variable(self):
        a=abba.trial_environment("CDC_BATCH_MS","1000",1)
        b=abba.trial_environment("CDC_BATCH_MS","200",1)
        keys={
            "CDC_RESOURCE_CPU_CORES","CDC_SQLITE_WRITE_TIMING","CDC_EVENT_TRACE",
            "CDC_EVENT_TRACE_EVERY","CDC_EVENT_TRACE_LIMIT",
            "CDC_COLD_BUILD_ADMISSION","CDC_COLD_BUILD_ROWS",
            "CDC_MERGE_VISIBILITY_PIPELINE","CDC_MERGE_VISIBILITY_PER_SINK",
            "CDC_BATCH_MS","CDC_MERGE_COMMIT_INTERVAL_MS"}
        changed={key for key in keys if a[key]!=b[key]}
        self.assertEqual(changed,{"CDC_BATCH_MS"})
        self.assertEqual(a["CDC_MERGE_COMMIT_INTERVAL_MS"],"1000")
        self.assertEqual(b["CDC_MERGE_COMMIT_INTERVAL_MS"],"1000")

    def test_merge_interval_experiment_keeps_batch_fixed(self):
        a=abba.trial_environment("CDC_MERGE_COMMIT_INTERVAL_MS","1000",1)
        b=abba.trial_environment("CDC_MERGE_COMMIT_INTERVAL_MS","500",1)
        self.assertEqual(a["CDC_BATCH_MS"],"1000")
        self.assertEqual(b["CDC_BATCH_MS"],"1000")
        self.assertNotEqual(
            a["CDC_MERGE_COMMIT_INTERVAL_MS"],
            b["CDC_MERGE_COMMIT_INTERVAL_MS"])

    def test_rejects_mutable_sha_bad_variable_and_old_directory(self):
        with self.assertRaises((ValueError,subprocess.CalledProcessError)):
            abba.experiment_plan(
                "HEAD","CDC_BATCH_MS","1000","200",self.directory,root=self.root)
        with self.assertRaises(ValueError):
            abba.experiment_plan(
                self.sha,"CDC_UNKNOWN","1","2",self.directory,root=self.root)
        self.directory.mkdir()
        with self.assertRaises(ValueError):
            abba.experiment_plan(
                self.sha,"CDC_BATCH_MS","1000","200",self.directory,root=self.root)

    def test_dirty_driver_checkout_is_rejected(self):
        (self.root/"data").write_text("dirty")
        with self.assertRaises(RuntimeError):
            abba.experiment_plan(
                self.sha,"CDC_BATCH_MS","1000","200",self.directory,root=self.root)


if __name__=="__main__":
    unittest.main()
