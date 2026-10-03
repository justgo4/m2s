#!/usr/bin/env python3
"""Real SQLite publication/GC races; no missing-plan fallback is permitted."""
import contextlib
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import cdc_catalog as catalog


class CatalogPlanGcTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.path=os.path.join(self.directory.name,"catalog.sqlite3")
        self.open=catalog.catalog_open

    def tearDown(self):
        self.directory.cleanup()

    def publish(self,version,busy_timeout=30000):
        con=self.open(self.path)
        try:
            con.execute("PRAGMA busy_timeout=%d"%busy_timeout)
            catalog._transaction(con)
            result=catalog._publish_persist(con,dict(
                is_new=True,version=version,revision=version,
                plan_hash="synthetic-plan-%d"%version,changed=True,
                mappings=[dict(name="sink_%d"%version)],
                stateful_tasks=[dict(task_id="join_%d"%version)],
                macros=[],udfs=[]))
            catalog._commit(con)
            return result
        finally:
            con.close()

    def versions(self):
        con=self.open(self.path)
        try:
            return [int(row[0]) for row in con.execute(
                "SELECT version FROM plans ORDER BY version")]
        finally:
            con.close()

    def test_publisher_cannot_enter_between_keep_snapshot_and_delete(self):
        self.publish(1)
        self.publish(2)
        paused=threading.Event()
        release=threading.Event()
        errors=[]
        original_open=self.open

        class PauseDelete:
            def __init__(self,con):
                self.con=con
            def execute(self,sql,*args):
                if sql.startswith("DELETE FROM plans"):
                    paused.set()
                    if not release.wait(5):
                        raise RuntimeError("test did not release GC")
                return self.con.execute(sql,*args)
            def __getattr__(self,name):
                return getattr(self.con,name)
            def __enter__(self):
                self.con.__enter__()
                return self
            def __exit__(self,*args):
                return self.con.__exit__(*args)

        def collect():
            try:
                catalog.prune_plans(self.path,keep_recent=32)
            except BaseException as exc:
                errors.append(exc)

        with mock.patch.object(catalog,"catalog_open",lambda path: PauseDelete(original_open(path))):
            worker=threading.Thread(target=collect)
            worker.start()
            try:
                self.assertTrue(paused.wait(5))
                with self.assertRaises(sqlite3.OperationalError) as caught:
                    self.publish(3,busy_timeout=10)
                self.assertEqual(caught.exception.sqlite_errorcode,sqlite3.SQLITE_BUSY)
                self.assertEqual(self.versions(),[1,2])
            finally:
                release.set()
                worker.join(5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(errors,[])
        self.publish(3)
        self.assertEqual(catalog.load_plan(self.path)["version"],3)
        self.assertEqual(self.versions(),[1,2,3])

    def test_published_lookup_survives_concurrent_publish_and_prune(self):
        self.publish(1)
        original_get=catalog._meta_get
        fired=False

        def read_then_replace(con,key,*args):
            nonlocal fired
            value=original_get(con,key,*args)
            if key=="published_version" and not fired:
                fired=True
                self.publish(2)
                catalog.prune_plans(self.path,keep_recent=1)
                self.assertEqual(self.versions(),[2])
            return value

        with mock.patch.object(catalog,"_meta_get",read_then_replace):
            plan=catalog.load_plan(self.path)
        self.assertTrue(fired)
        self.assertEqual(plan["version"],1)
        self.assertEqual(plan["stateful_tasks"],[dict(task_id="join_1")])
        self.assertEqual(catalog.load_plan(self.path)["version"],2)

    def test_gc_commit_fault_rolls_back_delete(self):
        self.publish(1)
        self.publish(2)
        with mock.patch.object(catalog,"_commit",side_effect=RuntimeError("synthetic GC commit fault")):
            with self.assertRaisesRegex(RuntimeError,"GC commit fault"):
                catalog.prune_plans(self.path,keep_recent=1)
        self.assertEqual(self.versions(),[1,2])
        self.assertEqual(catalog.load_plan(self.path)["version"],2)
        self.assertEqual(catalog.prune_plans(self.path,keep_versions=[1],keep_recent=1),0)
        self.assertEqual(catalog.prune_plans(self.path,keep_recent=1),1)
        self.assertEqual(self.versions(),[2])

    def test_missing_published_plan_still_fails_closed(self):
        self.publish(1)
        con=self.open(self.path)
        try:
            catalog._meta_set(con,"published_version",3)
        finally:
            con.close()
        with self.assertRaisesRegex(RuntimeError,"plan version 3 is missing"):
            catalog.load_plan(self.path)
        self.assertEqual(self.versions(),[1])

    def test_empty_catalog_gc_releases_write_transaction(self):
        self.assertEqual(catalog.prune_plans(self.path),0)
        self.publish(1,busy_timeout=10)
        self.assertEqual(catalog.load_plan_version(self.path,1)["version"],1)


if __name__=="__main__":
    unittest.main()
