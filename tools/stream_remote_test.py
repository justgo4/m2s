#!/usr/bin/env python3
"""Incremental checkpoint restore matches the local backend, including MinIO."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import state_backend
import stream_remote


class _Handler(BaseHTTPRequestHandler):
    store = {}

    def log_message(self, fmt, *args):
        return

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        path = urllib.parse.urlparse(self.path).path
        key = path.split("/", 2)[-1]
        self.store[urllib.parse.unquote(key)] = body
        self.send_response(201)
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if "list-type=2" in parsed.query:
            prefix = urllib.parse.parse_qs(parsed.query).get("prefix", [""])[0]
            prefix = urllib.parse.unquote(prefix)
            keys = sorted(key for key in self.store if key.startswith(prefix))
            payload = json.dumps({"keys": keys}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        key = urllib.parse.unquote(parsed.path.split("/", 2)[-1])
        if key not in self.store:
            self.send_response(404)
            self.end_headers()
            return
        body = self.store[key]
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _fill(backend):
    backend.put(b"a", b"1")
    backend.put(b"b", b"2")


def _check(store, label):
    with tempfile.TemporaryDirectory(prefix="m2s-remote-") as directory:
        root = Path(directory)
        source = state_backend.open_state_backend("sqlite", root / "src.sqlite3")
        restored = state_backend.open_state_backend("sqlite", root / "dst.sqlite3")
        try:
            checkpoint = stream_remote.IncrementalCheckpoint(source, store)
            _fill(source)
            first = checkpoint.commit()
            source.put(b"b", b"9")
            source.delete(b"a")
            source.put(b"c", b"3")
            second = checkpoint.commit()
            if not first["puts"] or "a" not in "".join(second["deletes"]):
                # deletes are hex, just ensure the second manifest records a delete
                if not second["deletes"]:
                    raise AssertionError(label + " missed a delete")
            state = checkpoint.restore(restored)
            if state != {b"b": b"9", b"c": b"3"}:
                raise AssertionError(label + " " + repr(state))
            if restored.items() != [(b"b", b"9"), (b"c", b"3")]:
                raise AssertionError(label + " backend " + repr(restored.items()))
            cache_local = state_backend.open_state_backend(
                "sqlite", root / "cache.sqlite3")
            cache = stream_remote.TieredCache(cache_local, store)
            try:
                cache.put(b"z", b"cold")
                cache.evict_local(b"z")
                if cache_local.get(b"z") is not None:
                    raise AssertionError("local cache still held the cold key")
                if cache.get(b"z") != b"cold":
                    raise AssertionError("cold key did not reload")
                if cache_local.get(b"z") != b"cold":
                    raise AssertionError("reloaded key was not cached")
            finally:
                cache_local.close()
        finally:
            source.close()
            restored.close()


def test_directory_and_http():
    with tempfile.TemporaryDirectory(prefix="m2s-objects-") as directory:
        _check(stream_remote.DirectoryStore(Path(directory) / "objects"), "directory")
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = "http://127.0.0.1:%d" % server.server_address[1]
        _check(stream_remote.HttpStore(base, "bucket"), "http")
    finally:
        server.shutdown()


def test_minio():
    import os
    endpoint = os.environ.get("M2S_MINIO_ENDPOINT", "")
    if not endpoint:
        print("minio_skipped no endpoint", flush=True)
        return
    store = stream_remote.MinioStore(
        endpoint,
        os.environ["M2S_MINIO_ACCESS"],
        os.environ["M2S_MINIO_SECRET"],
        os.environ.get("M2S_MINIO_BUCKET", "m2s-ckpt"))
    _check(store, "minio")
    print("minio_ok", flush=True)


def main():
    test_directory_and_http()
    test_minio()
    print("stream_remote_test ok", flush=True)


if __name__ == "__main__":
    main()
