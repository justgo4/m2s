#!/usr/bin/env python3
"""Idempotent inserts into a local ClickHouse table over HTTP.

A delivery id is stored after the rows. Repeating that id does not insert
again. Rows use ReplacingMergeTree on the primary key so a crash between
the row insert and the delivery insert still collapses a replay.
"""
import json
from urllib.request import Request, urlopen


class ClickHouseSink:
    name = "clickhouse"
    direction = "sink"
    semantic = "effectively_once"

    def __init__(self, base_url="http://127.0.0.1:8123", table="m2s_rows"):
        self.base_url = str(base_url).rstrip("/") + "/"
        self.table = str(table)

    def query(self, sql):
        request = Request(
            self.base_url, data=str(sql).encode("utf-8"), method="POST")
        with urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")

    def ensure(self):
        self.query(
            "CREATE TABLE IF NOT EXISTS %s ("
            "id Int64, v Int64) ENGINE = ReplacingMergeTree ORDER BY id"
            % self.table)
        self.query(
            "CREATE TABLE IF NOT EXISTS m2s_deliveries ("
            "id String) ENGINE = MergeTree ORDER BY id")

    def write(self, delivery_id, rows):
        delivery_id = str(delivery_id).replace("'", "")
        found = self.query(
            "SELECT count() FROM m2s_deliveries WHERE id = '%s'" % delivery_id)
        if int(found.strip() or "0") > 0:
            return False
        if rows:
            payload = "\n".join(json.dumps(dict(row)) for row in rows)
            self.query(
                "INSERT INTO %s FORMAT JSONEachRow\n%s" % (self.table, payload))
        self.query(
            "INSERT INTO m2s_deliveries (id) VALUES ('%s')" % delivery_id)
        return True

    def count(self):
        text = self.query("SELECT count() FROM %s FINAL" % self.table)
        return int(text.strip() or "0")
