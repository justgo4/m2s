#!/usr/bin/env python3
"""Keyed state backends. SQLite is the default. RocksDB is the second.

The CDC hot path keeps its own SQLite engines. These backends are for the
stream reference state and later checkpoints. They are not wired into j4.
"""
import os
import sqlite3


class StateBackend:
    def put(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


def _bytes(value, label):
    if isinstance(value, str):
        value = value.encode("utf-8")
    if not isinstance(value, (bytes, bytearray)):
        raise TypeError(label + " must be bytes")
    return bytes(value)


class SqliteStateBackend(StateBackend):
    def __init__(self, path):
        self.path = str(path)
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.con = sqlite3.connect(self.path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS kv("
            "k BLOB PRIMARY KEY, v BLOB NOT NULL)")

    def put(self, key, value):
        key, value = _bytes(key, "key"), _bytes(value, "value")
        self.con.execute(
            "INSERT INTO kv(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value))
        self.con.commit()

    def get(self, key):
        row = self.con.execute(
            "SELECT v FROM kv WHERE k=?", (_bytes(key, "key"),)).fetchone()
        if row is None:
            return None
        return bytes(row[0])

    def delete(self, key):
        self.con.execute(
            "DELETE FROM kv WHERE k=?", (_bytes(key, "key"),))
        self.con.commit()

    def items(self):
        return [
            (bytes(key), bytes(value))
            for key, value in self.con.execute(
                "SELECT k,v FROM kv ORDER BY k")
        ]

    def close(self):
        self.con.close()


class RocksStateBackend(StateBackend):
    def __init__(self, path):
        import rocksdict
        self.path = str(path)
        os.makedirs(self.path, exist_ok=True)
        self.db = rocksdict.Rdict(self.path)

    def put(self, key, value):
        self.db[_bytes(key, "key")] = _bytes(value, "value")

    def get(self, key):
        value = self.db.get(_bytes(key, "key"))
        if value is None:
            return None
        return bytes(value)

    def delete(self, key):
        key = _bytes(key, "key")
        if self.db.get(key) is not None:
            del self.db[key]

    def items(self):
        return [(bytes(key), bytes(value)) for key, value in self.db.items()]

    def close(self):
        self.db.close()


def open_state_backend(kind, path):
    name = str(kind or "sqlite").strip().lower()
    if name == "sqlite":
        return SqliteStateBackend(path)
    if name == "rocksdb":
        return RocksStateBackend(path)
    raise ValueError("unknown state backend: " + name)
