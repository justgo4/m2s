#!/usr/bin/env python3
"""Create and verify a private SQLite snapshot; never restore or mutate live state."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from urllib.parse import quote
import uuid


def atomic_json(path, value):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as handle:
            json.dump(value, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def digest(path, check=None):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024*1024), b""):
            if check is not None:
                check()
            value.update(chunk)
    return value.hexdigest()


def readonly(path, timeout=1):
    return sqlite3.connect("file:" + quote(str(path.resolve()), safe="/") + "?mode=ro",
                           uri=True, timeout=timeout)


def integrity(con):
    if con.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
        raise ValueError("SQLite integrity check failed")


def create(state, directory, timeout_seconds=60):
    state, directory = Path(state).resolve(), Path(directory).absolute()
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("backup timeout must be finite and positive")
    if not state.is_file():
        raise FileNotFoundError("source state does not exist")
    # Existing backups and partial evidence are never overwritten.
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    record = dict(format_version=1, kind="m2s_sqlite_backup", phase="copying",
                  source=str(state), created_at_unix=time.time(), sqlite_version=sqlite3.sqlite_version)
    status_path = directory/"status.json"
    snapshot = directory/"state.sqlite3"
    source = target = None
    deadline = time.monotonic() + timeout_seconds

    def remaining():
        if time.monotonic() >= deadline:
            raise TimeoutError("backup deadline exceeded")

    def progress(_status, _remaining, _total):
        remaining()

    try:
        atomic_json(status_path, record)
        with os.fdopen(os.open(snapshot, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"):
            pass
        source = readonly(state, timeout=min(1, timeout_seconds))
        target = sqlite3.connect(snapshot, timeout=min(1, timeout_seconds))
        target.execute("PRAGMA journal_mode=DELETE")
        target.execute("PRAGMA synchronous=FULL")
        source.backup(target, pages=256, progress=progress, sleep=0.05)
        remaining()
        # backup copies the source's WAL-mode header too; switch only after
        # copying so the completed artifact is self-contained without sidecars.
        if target.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
            raise ValueError("backup could not become a single-file snapshot")
        # Check/hash have the same deadline as copying. Never publish a partial
        # or unchecked snapshot as ready, including during continuous WAL churn.
        target.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        integrity(target)
        target.close()
        target = None
        source.close()
        source = None
        with snapshot.open("rb") as handle:
            os.fsync(handle.fileno())
        sha = digest(snapshot, remaining)
        remaining()
        manifest = dict(record, phase="ready", snapshot="state.sqlite3",
                        snapshot_bytes=snapshot.stat().st_size, snapshot_sha256=sha,
                        completed_at_unix=time.time(), restore_authorized=False)
        atomic_json(directory/"manifest.json", manifest)
        atomic_json(status_path, manifest)
        return manifest
    except BaseException as exc:
        # Preserve incomplete files for diagnosis; verification requires both
        # ready records. Exception type is sufficient and avoids private SQL.
        record.update(phase="incomplete", error_type=type(exc).__name__, updated_at_unix=time.time())
        atomic_json(status_path, record)
        raise
    finally:
        if target is not None:
            target.close()
        if source is not None:
            source.close()


def verify(directory):
    directory = Path(directory).absolute()
    manifest = json.loads((directory/"manifest.json").read_text())
    status = json.loads((directory/"status.json").read_text())
    if (manifest != status or manifest.get("kind") != "m2s_sqlite_backup"
            or manifest.get("format_version") != 1 or manifest.get("phase") != "ready"
            or manifest.get("snapshot") != "state.sqlite3"):
        raise ValueError("backup is incomplete or has unsupported identity")
    snapshot = directory/"state.sqlite3"
    if snapshot.is_symlink() or any(Path(str(snapshot) + suffix).exists() for suffix in ("-wal", "-journal")):
        raise ValueError("backup is not an immutable single SQLite snapshot")
    if (snapshot.stat().st_size != manifest.get("snapshot_bytes")
            or digest(snapshot) != manifest.get("snapshot_sha256")):
        raise ValueError("backup bytes differ from manifest")
    con = readonly(snapshot)
    try:
        integrity(con)
    finally:
        con.close()
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    new = sub.add_parser("create")
    new.add_argument("--state", type=Path, required=True)
    new.add_argument("--backup-directory", type=Path, required=True)
    new.add_argument("--timeout-seconds", type=float, default=60)
    old = sub.add_parser("verify")
    old.add_argument("--backup-directory", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = (create(args.state, args.backup_directory, args.timeout_seconds)
                  if args.command == "create" else verify(args.backup_directory))
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(json.dumps(dict(ok=False, error_type=type(exc).__name__)), flush=True)
        return 1
    print(json.dumps(dict(ok=True, phase=result["phase"], snapshot_sha256=result["snapshot_sha256"],
                          snapshot_bytes=result["snapshot_bytes"])), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
