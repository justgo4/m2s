#!/usr/bin/env python3
"""Incremental checkpoints on an object store, with a hot local cache.

The store speaks a small S3-style path API: PUT/GET/LIST. A directory store
is the local stand-in. HttpStore talks to any server that accepts those
paths, including MinIO when the official binary is available. Object bytes
stay outside the git repository.
"""
import json
import os
import urllib.error
import urllib.request


def _quote(key):
    from urllib.parse import quote
    return quote(str(key), safe="/")


class DirectoryStore:
    def __init__(self, root):
        self.root = str(root)
        os.makedirs(self.root, exist_ok=True)

    def put(self, key, data):
        path = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as handle:
            handle.write(bytes(data))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)

    def get(self, key):
        path = os.path.join(self.root, key)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as handle:
            return handle.read()

    def list(self, prefix):
        found = []
        base = os.path.join(self.root, prefix)
        if not os.path.isdir(os.path.dirname(base)) and not os.path.isdir(base):
            return found
        root_prefix = os.path.join(self.root, "")
        for dirpath, _dirs, files in os.walk(self.root):
            for name in files:
                full = os.path.join(dirpath, name)
                rel = full[len(root_prefix):]
                if rel.startswith(prefix) and not rel.endswith(".tmp"):
                    found.append(rel)
        return sorted(found)


class MinioStore:
    """Real MinIO. Credentials come from the caller and never from the repo."""

    def __init__(self, endpoint, access_key, secret_key, bucket, secure=False):
        from minio import Minio
        self.client = Minio(
            str(endpoint),
            access_key=str(access_key),
            secret_key=str(secret_key),
            secure=bool(secure))
        self.bucket = str(bucket)
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def put(self, key, data):
        import io
        raw = bytes(data)
        self.client.put_object(
            self.bucket, str(key), io.BytesIO(raw), len(raw))

    def get(self, key):
        from minio.error import S3Error
        try:
            response = self.client.get_object(self.bucket, str(key))
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                return None
            raise
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list(self, prefix):
        return sorted(
            item.object_name
            for item in self.client.list_objects(
                self.bucket, prefix=str(prefix), recursive=True))


class HttpStore:
    def __init__(self, base_url, bucket):
        self.base = str(base_url).rstrip("/")
        self.bucket = str(bucket)

    def _url(self, key):
        return self.base + "/" + self.bucket + "/" + _quote(key)

    def put(self, key, data):
        request = urllib.request.Request(
            self._url(key), data=bytes(data), method="PUT")
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status not in (200, 201, 204):
                raise RuntimeError("object put failed: " + str(response.status))

    def get(self, key):
        try:
            with urllib.request.urlopen(self._url(key), timeout=10) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise

    def list(self, prefix):
        url = (self.base + "/" + self.bucket
               + "?list-type=2&prefix=" + _quote(prefix))
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return list(payload["keys"])


class IncrementalCheckpoint:
    def __init__(self, backend, store):
        self.backend = backend
        self.store = store
        self.epoch = 0
        self.known = {}

    def commit(self):
        current = {bytes(key): bytes(value) for key, value in self.backend.items()}
        puts = []
        deletes = []
        for key, value in current.items():
            if self.known.get(key) != value:
                name = "ckpt/%d/put/%s" % (self.epoch, key.hex())
                self.store.put(name, value)
                puts.append(key.hex())
        for key in self.known:
            if key not in current:
                deletes.append(key.hex())
        manifest = dict(epoch=self.epoch, puts=puts, deletes=deletes)
        self.store.put(
            "ckpt/%d/manifest.json" % self.epoch,
            json.dumps(manifest, sort_keys=True).encode("utf-8"))
        self.known = current
        self.epoch += 1
        return manifest

    def restore(self, backend):
        epochs = []
        for key in self.store.list("ckpt/"):
            if key.endswith("/manifest.json"):
                epochs.append(key)
        state = {}
        for key in sorted(epochs):
            manifest = json.loads(self.store.get(key).decode("utf-8"))
            epoch = int(manifest["epoch"])
            for encoded in manifest["deletes"]:
                state.pop(bytes.fromhex(encoded), None)
            for encoded in manifest["puts"]:
                blob = self.store.get("ckpt/%d/put/%s" % (epoch, encoded))
                state[bytes.fromhex(encoded)] = blob
        for key, _value in list(backend.items()):
            backend.delete(key)
        for key, value in state.items():
            backend.put(key, value)
        return state


class TieredCache:
    def __init__(self, local, remote):
        self.local = local
        self.remote = remote

    def put(self, key, value):
        self.local.put(key, value)
        self.remote.put(_remote_name(key), value)

    def get(self, key):
        found = self.local.get(key)
        if found is not None:
            return found
        found = self.remote.get(_remote_name(key))
        if found is not None:
            self.local.put(key, found)
        return found

    def evict_local(self, key):
        self.local.delete(key)


def _remote_name(key):
    return "hot/" + bytes(key).hex()
