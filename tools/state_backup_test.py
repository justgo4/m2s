#!/usr/bin/env python3
"""Exercise real SQLite WAL, interrupted backups and immutable evidence checks."""
from pathlib import Path
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import state_backup


class BackupTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root/"source.sqlite3"
        self.destination = self.root/"backup"
        self.source = sqlite3.connect(self.state)
        self.addCleanup(self.source.close)
        self.source.execute("PRAGMA journal_mode=WAL")
        self.source.execute("CREATE TABLE pairs(id PRIMARY KEY, a, b, padding)")
        self.source.executemany("INSERT INTO pairs VALUES(?,1,1,?)",
                                [(i, "x"*16384) for i in range(200)])
        self.source.commit()

    def test_wal_writer_overlaps_backup_with_one_consistent_snapshot(self):
        begin = threading.Event()
        committed = threading.Event()
        errors = []

        def writer():
            try:
                if not begin.wait(5):
                    raise RuntimeError("backup never started")
                con = sqlite3.connect(self.state)
                try:
                    con.execute("UPDATE pairs SET a=2,b=2")
                    con.commit()
                finally:
                    con.close()
            except BaseException as exc:
                errors.append(exc)
            finally:
                committed.set()

        class ConcurrentConnection(sqlite3.Connection):
            def backup(self, target, **kwargs):
                progress = kwargs["progress"]

                def overlapping(status, remaining, total):
                    if remaining > 0 and not begin.is_set():
                        begin.set()
                        if not committed.wait(5):
                            raise RuntimeError("writer blocked by backup")
                    progress(status, remaining, total)

                kwargs["progress"] = overlapping
                return super().backup(target, **kwargs)

        def open_source(path, timeout=1):
            return sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True,
                                   timeout=timeout, factory=ConcurrentConnection)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            with patch.object(state_backup, "readonly", side_effect=open_source):
                result = state_backup.create(self.state, self.destination)
        finally:
            begin.set()
            thread.join(6)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors, errors)
        self.assertTrue(committed.is_set())
        self.assertEqual(state_backup.verify(self.destination), result)
        con = sqlite3.connect(self.destination/"state.sqlite3")
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM pairs").fetchone()[0], 200)
            self.assertEqual(con.execute("SELECT DISTINCT a,b FROM pairs").fetchall(), [(2, 2)])
            self.assertEqual(con.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        finally:
            con.close()
        self.assertEqual(os.stat(self.destination).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(self.destination/"state.sqlite3").st_mode & 0o777, 0o600)
        self.assertFalse(result["restore_authorized"])

    def test_existing_directory_or_missing_source_never_reinitializes(self):
        state_backup.create(self.state, self.destination)
        before = (self.destination/"manifest.json").read_bytes()
        with self.assertRaises(FileExistsError):
            state_backup.create(self.state, self.destination)
        self.assertEqual(before, (self.destination/"manifest.json").read_bytes())
        with self.assertRaises(FileNotFoundError):
            state_backup.create(self.root/"missing.sqlite3", self.root/"missing-backup")
        self.assertFalse((self.root/"missing-backup").exists())

    def test_tampered_bytes_sidecar_and_incomplete_records_are_rejected(self):
        state_backup.create(self.state, self.destination)
        snapshot = self.destination/"state.sqlite3"
        with snapshot.open("r+b") as handle:
            handle.seek(-1, os.SEEK_END)
            byte = handle.read(1)
            handle.seek(-1, os.SEEK_END)
            handle.write(bytes([byte[0] ^ 1]))
        with self.assertRaises(ValueError):
            state_backup.verify(self.destination)
        other = self.root/"other"
        state_backup.create(self.state, other)
        Path(str(other/"state.sqlite3") + "-wal").write_bytes(b"")
        with self.assertRaises(ValueError):
            state_backup.verify(other)
        Path(str(other/"state.sqlite3") + "-wal").unlink()
        record = json.loads((other/"status.json").read_text())
        record["phase"] = "incomplete"
        (other/"status.json").write_text(json.dumps(record))
        with self.assertRaises(ValueError):
            state_backup.verify(other)

    def test_locked_source_times_out_and_retains_incomplete_evidence(self):
        self.source.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.source.execute("PRAGMA journal_mode=DELETE")
        self.source.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                state_backup.create(self.state, self.destination, 0.1)
        finally:
            self.source.rollback()
        self.assertLess(time.monotonic() - started, 2)
        record = json.loads((self.destination/"status.json").read_text())
        self.assertEqual(record["phase"], "incomplete")
        self.assertFalse((self.destination/"manifest.json").exists())
        self.assertEqual(self.source.execute("SELECT DISTINCT a,b FROM pairs").fetchall(), [(1, 1)])

    def test_cancelled_or_corrupt_source_never_becomes_ready(self):
        with patch.object(state_backup, "integrity", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                state_backup.create(self.state, self.destination)
        self.assertEqual(json.loads((self.destination/"status.json").read_text())["phase"], "incomplete")
        self.assertFalse((self.destination/"manifest.json").exists())
        corrupt = self.root/"corrupt.sqlite3"
        corrupt.write_bytes(b"this is not a SQLite file")
        with self.assertRaises(sqlite3.DatabaseError):
            state_backup.create(corrupt, self.root/"corrupt-backup")
        self.assertEqual(json.loads((self.root/"corrupt-backup/status.json").read_text())["phase"], "incomplete")
        for timeout in (0, -1, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                state_backup.create(self.state, self.root/"invalid", timeout)


if __name__ == "__main__":
    unittest.main()
