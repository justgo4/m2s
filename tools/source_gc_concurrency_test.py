#!/usr/bin/env python3
"""SQLite retention/admission races, including bounded GC's logical frontier."""
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import source_state


class RetentionConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        path = str(Path(self.directory.name) / "source.sqlite3")
        self.writer = sqlite3.connect(path, timeout=0, isolation_level=None)
        self.writer.execute("PRAGMA journal_mode=WAL")
        source_state.install(self.writer)
        self.reader = sqlite3.connect(path, timeout=0, isolation_level=None)
        with source_state.transaction(self.writer):
            source_state._meta_set_int(self.writer, "base_applied_seq", 10)
            source_state._meta_set_int(self.writer, "log_durable_seq", 10)
            for seq in range(1, 11):
                self.writer.execute("""
                    INSERT INTO source_commits(
                        seq,source_epoch,source_file,source_pos,created,base_applied)
                    VALUES(?, 'epoch', 'binlog.000001', ?, 0, 1)
                """, (seq, seq))
            for key in (b"a", b"b", b"c"):
                self.writer.execute("""
                    INSERT INTO source_versions VALUES('db.t', ?, 0, 10, 0, ?, 1)
                """, (key, b"old"))
                self.writer.execute("""
                    INSERT INTO source_versions VALUES('db.t', ?, 10, NULL, 0, ?, 1)
                """, (key, b"new"))

    def tearDown(self):
        self.reader.close()
        self.writer.close()
        self.directory.cleanup()

    def old_versions(self):
        return self.reader.execute(
            "SELECT COUNT(*) FROM source_versions WHERE valid_from=0"
        ).fetchone()[0]

    def test_gc_reader_registration_interleaving(self):
        original_floor = source_state.retention_floor
        attempts = []

        def admit_reader_after_floor(con, watermarks=()):
            floor = original_floor(con, watermarks)
            # Precisely the old gap: floor is known but no rows deleted yet.
            # A second connection must not durably register a lower reader here.
            try:
                source_state.register_consumer(self.reader, "late-reader", 5)
            except sqlite3.OperationalError as exc:
                self.assertIn("locked", str(exc).lower())
                attempts.append("blocked")
            else:
                attempts.append("registered")
            return floor

        with patch.object(source_state, "retention_floor", admit_reader_after_floor):
            result = source_state.gc(self.writer, version_limit=1, commit_limit=1)
        self.assertEqual(attempts, ["blocked"])
        self.assertEqual(result["floor"], 10)
        self.assertEqual(self.old_versions(), 2)  # Bounded GC leaves physical rows.
        with self.assertRaisesRegex(ValueError, "retained source history"):
            source_state.register_consumer(self.reader, "late-reader", 5)
        self.assertEqual(self.reader.execute(
            "SELECT COUNT(*) FROM source_consumers"
        ).fetchone()[0], 0)
        source_state.register_consumer(self.reader, "current-reader", 10)

    def test_registered_reader_preserves_versions_and_replay(self):
        source_state.register_consumer(self.reader, "existing-reader", 5)
        result = source_state.gc(self.writer, version_limit=1, commit_limit=1)
        self.assertEqual(result["floor"], 5)
        self.assertEqual(self.old_versions(), 3)
        self.assertEqual([row[0] for row in self.reader.execute(
            "SELECT seq FROM source_commits WHERE seq>5 ORDER BY seq"
        )], list(range(6, 11)))
        source_state.advance_consumer(self.reader, "existing-reader", 10)
        source_state.gc(self.writer)
        self.assertEqual(self.old_versions(), 0)
        self.assertEqual(source_state.min_readable_seq(self.reader), 10)
        with self.assertRaisesRegex(ValueError, "retained source history"):
            source_state.register_consumer(self.reader, "obsolete-reader", 5)

    def test_registration_validates_history_under_write_lock(self):
        original_applied = source_state.base_applied_seq
        attempts = []

        def gc_before_admission(con):
            applied = original_applied(con)
            # A GC cannot change readability between admission validation and
            # durable insertion, even when its retained floor is already known.
            try:
                with source_state.transaction(self.writer):
                    source_state._meta_set_int(self.writer, "min_readable_seq", 10)
            except sqlite3.OperationalError as exc:
                self.assertIn("locked", str(exc).lower())
                attempts.append("blocked")
            else:
                attempts.append("advanced")
            return applied

        with patch.object(source_state, "base_applied_seq", gc_before_admission):
            source_state.register_consumer(self.reader, "new-reader", 5)
        self.assertEqual(attempts, ["blocked"])
        self.assertEqual(source_state.gc(self.writer)["floor"], 5)
        self.assertEqual(self.old_versions(), 3)


if __name__ == "__main__":
    unittest.main()
