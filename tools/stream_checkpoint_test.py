#!/usr/bin/env python3
"""Barrier, crash, duplicate checkpoint, and savepoint."""
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_checkpoint


def test_crash_and_duplicate():
    for kind, leaf in (("sqlite", "state.sqlite3"), ("rocksdb", "rocks")):
        _one_backend(kind, leaf)


def _one_backend(kind, leaf):
    with tempfile.TemporaryDirectory(prefix="m2s-ckpt-") as directory:
        coord = stream_checkpoint.open_checkpoint(kind, Path(directory) / leaf)
        try:
            coord.backend.put(b"a", b"1")
            assert coord.emit(1, {"id": 1}) is True
            prepared = coord.start_barrier(1, 10)
            assert prepared["duplicate"] is False
            again = coord.start_barrier(1, 10)
            assert again["duplicate"] is True
            done = coord.commit_barrier(1)
            assert done["duplicate"] is False
            assert coord.commit_barrier(1)["duplicate"] is True
            assert coord.emit(1, {"id": 1}) is False
            coord.backend.put(b"a", b"2")
            coord.backend.put(b"b", b"9")
            assert coord.emit(2, {"id": 2}) is True
            coord.start_barrier(2, 20)
            restored = coord.abort_pending()
            assert restored["barrier_id"] == 1
            loaded = coord.restore()
            assert loaded["source_offset"] == 10
            assert coord.backend.get(b"a") == b"1"
            assert coord.backend.get(b"b") is None
            assert coord.output_epoch == 1
            coord.backend.put(b"a", b"3")
            assert coord.emit(2, {"id": 2}) is True
            coord.start_barrier(3, 30)
            coord.commit_barrier(3)
            name = coord.savepoint("noon")
            coord.backend.put(b"a", b"4")
            coord.restore_savepoint(name)
            assert coord.backend.get(b"a") == b"3"
            try:
                coord.start_barrier(4, 5)
                raise AssertionError("offset moved backwards")
            except ValueError:
                pass
            blob = coord.export_bytes()
            assert b'"output_epoch": 2' in blob
        finally:
            coord.backend.close()


def main():
    test_crash_and_duplicate()
    print("stream_checkpoint_test ok", flush=True)


if __name__ == "__main__":
    main()
