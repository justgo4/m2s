#!/usr/bin/env python3
"""Append to Iceberg on local MinIO, replay a delivery, and restart MinIO."""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import iceberg_sink
from minio import Minio


def _root_key():
    return os.environ.get("MINIO_ROOT_PASSWORD", "m2s-local-only")


def _root_user():
    return os.environ.get("MINIO_ROOT_USER", "m2sadmin")


def _endpoint():
    return "http://127.0.0.1:9100"


def _wait_ready():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            urlopen(_endpoint() + "/minio/health/ready", timeout=1).read()
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("minio did not become ready")


def _start(binary):
    data = Path("/tmp/m2s-minio")
    data.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["MINIO_ROOT_USER"] = _root_user()
    env["MINIO_ROOT_PASSWORD"] = _root_key()
    log = open("/tmp/m2s-minio.log", "ab")
    proc = subprocess.Popen(
        [binary, "server", str(data), "--address", "127.0.0.1:9100"],
        stdout=log, stderr=subprocess.STDOUT, env=env)
    _wait_ready()
    return proc


def _bucket():
    client = Minio(
        "127.0.0.1:9100", access_key=_root_user(), secret_key=_root_key(),
        secure=False)
    name = "m2s-iceberg"
    if not client.bucket_exists(name):
        client.make_bucket(name)


def main():
    binary = os.environ.get("MINIO_BIN", "/tmp/go-bin/minio")
    if not Path(binary).is_file():
        raise SystemExit("minio binary is missing: " + binary)
    catalog = Path("/tmp/m2s-iceberg")
    shutil.rmtree(catalog, ignore_errors=True)
    shutil.rmtree("/tmp/m2s-minio", ignore_errors=True)
    catalog.mkdir(parents=True)
    proc = _start(binary)
    try:
        _bucket()
        sink = iceberg_sink.IcebergSink(access_key=_root_user(), secret_key=_root_key())
        sink.ensure()
        rows = [dict(id=index, v=index * 10) for index in range(1, 4)]
        if not sink.write("d1", rows):
            raise AssertionError("first delivery was skipped")
        if sink.write("d1", rows):
            raise AssertionError("duplicate delivery was applied")
        if sink.count() != 3:
            raise AssertionError(sink.count())
        if not sink.write("d2", [dict(id=4, v=40)]):
            raise AssertionError("second delivery was skipped")
        if sink.count() != 4:
            raise AssertionError(sink.count())
        snapshots = sink.snapshots()
        proc.send_signal(signal.SIGTERM)
        proc.wait(20)
        proc = _start(binary)
        restored = iceberg_sink.IcebergSink(
            access_key=_root_user(), secret_key=_root_key())
        if restored.count() != 4:
            raise AssertionError("restart lost rows: %s" % restored.count())
        if restored.write("d1", rows):
            raise AssertionError("delivery survived as reapplicable")
        if restored.count() != 4 or restored.snapshots() != snapshots:
            raise AssertionError((restored.count(), restored.snapshots(), snapshots))
        print(
            "iceberg_sink rows=4 deliveries=2 replayed=0 restarted=1 snapshots=%d"
            % snapshots,
            flush=True)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            proc.wait(20)
    print("iceberg_sink_test ok", flush=True)


if __name__ == "__main__":
    main()
