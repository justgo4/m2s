#!/usr/bin/env python3
"""A new process restores state from MinIO and nothing else."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import state_backend
import stream_remote


def _store():
    return stream_remote.MinioStore(
        os.environ["M2S_MINIO_ENDPOINT"],
        os.environ["M2S_MINIO_ACCESS"],
        os.environ["M2S_MINIO_SECRET"],
        os.environ.get("M2S_MINIO_BUCKET", "m2s-restart"))


def restore_only():
    started = time.perf_counter()
    store = _store()
    with tempfile.TemporaryDirectory(prefix="m2s-minio-child-") as directory:
        backend = state_backend.open_state_backend(
            "sqlite", Path(directory) / "state.sqlite3")
        try:
            state = stream_remote.IncrementalCheckpoint(backend, store).restore(backend)
        finally:
            backend.close()
    objects = store.list("ckpt/")
    total = 0
    for key in objects:
        blob = store.get(key)
        total += len(blob or b"")
    print(json.dumps(dict(
        items=sorted((key.hex(), value.hex()) for key, value in state.items()),
        objects=len(objects),
        bytes=total,
        seconds=round(time.perf_counter() - started, 6),
    )), flush=True)


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "restore":
        restore_only()
        return
    endpoint = os.environ.get("M2S_MINIO_ENDPOINT", "")
    if not endpoint:
        raise SystemExit("M2S_MINIO_ENDPOINT is required")
    with tempfile.TemporaryDirectory(prefix="m2s-minio-parent-") as directory:
        backend = state_backend.open_state_backend(
            "sqlite", Path(directory) / "state.sqlite3")
        store = _store()
        try:
            checkpoint = stream_remote.IncrementalCheckpoint(backend, store)
            backend.put(b"a", b"1")
            backend.put(b"b", b"2")
            checkpoint.commit()
            backend.put(b"b", b"9")
            backend.delete(b"a")
            checkpoint.commit()
        finally:
            backend.close()
    env = dict(os.environ)
    proc = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "restore"],
        capture_output=True, text=True, env=env, check=False)
    if proc.returncode != 0:
        raise AssertionError(proc.stderr)
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    if payload["items"] != [["62", "39"]]:
        raise AssertionError(payload)
    if payload["objects"] < 2 or payload["bytes"] <= 0:
        raise AssertionError(payload)
    print(
        "minio_restart objects=%d bytes=%d seconds=%.6f"
        % (payload["objects"], payload["bytes"], payload["seconds"]),
        flush=True)
    print("stream_minio_restart_test ok", flush=True)


if __name__ == "__main__":
    main()
