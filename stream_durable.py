#!/usr/bin/env python3
"""Durable checkpoint. A kill before commit does not publish the pending cut."""
import json
import os
from pathlib import Path
import signal
import sys

import state_backend


def _atomic(path, payload):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _dump(barrier_id, source_offset, output_epoch, backend):
    items = [
        [key.hex(), value.hex()]
        for key, value in backend.items()
    ]
    return json.dumps(dict(
        barrier_id=int(barrier_id),
        source_offset=int(source_offset),
        output_epoch=int(output_epoch),
        items=items,
    ), sort_keys=True).encode("utf-8")


def _load(path):
    data = json.loads(Path(path).read_text())
    items = [(bytes.fromhex(key), bytes.fromhex(value)) for key, value in data["items"]]
    data["items"] = items
    return data


class DurableCheckpoint:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.backend = state_backend.open_state_backend(
            "sqlite", self.directory / "state.sqlite3")
        self.output_epoch = 0
        self.latest = None
        latest = self.directory / "latest.json"
        if latest.exists():
            self.restore()

    def close(self):
        self.backend.close()

    def emit(self, epoch, row):
        epoch = int(epoch)
        if epoch <= self.output_epoch:
            return False
        if epoch != self.output_epoch + 1:
            raise ValueError("output epoch skipped")
        self.output_epoch = epoch
        self.backend.put(("out-%d" % epoch).encode("utf-8"), json.dumps(row).encode("utf-8"))
        return True

    def commit(self, barrier_id, source_offset):
        payload = _dump(barrier_id, source_offset, self.output_epoch, self.backend)
        _atomic(self.directory / "latest.json", payload)
        pending = self.directory / "pending.json"
        if pending.exists():
            pending.unlink()
        self.latest = _load(self.directory / "latest.json")
        return dict(barrier_id=int(barrier_id), source_offset=int(source_offset))

    def prepare(self, barrier_id, source_offset):
        payload = _dump(barrier_id, source_offset, self.output_epoch, self.backend)
        _atomic(self.directory / "pending.json", payload)
        return int(barrier_id)

    def restore(self):
        latest = self.directory / "latest.json"
        if not latest.exists():
            raise ValueError("no committed checkpoint")
        data = _load(latest)
        for key, _value in list(self.backend.items()):
            self.backend.delete(key)
        for key, value in data["items"]:
            self.backend.put(key, value)
        self.output_epoch = int(data["output_epoch"])
        self.latest = data
        return data


def crash_before_commit(directory):
    cut = DurableCheckpoint(directory)
    cut.backend.put(b"a", b"1")
    if not cut.emit(1, {"id": 1}):
        raise RuntimeError("first output was not applied")
    cut.commit(1, 10)
    cut.backend.put(b"a", b"2")
    if not cut.emit(2, {"id": 2}):
        raise RuntimeError("second output was not applied")
    cut.prepare(2, 20)
    (Path(directory) / "prepared").write_text("pending\n")
    cut.backend.close()
    os.kill(os.getpid(), signal.SIGKILL)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "crash":
        crash_before_commit(sys.argv[2])
        return
    raise SystemExit("usage: stream_durable.py crash <directory>")


if __name__ == "__main__":
    main()
