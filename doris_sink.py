#!/usr/bin/env python3
"""Idempotent Stream Load into a local Apache Doris database.

The load label is the delivery id. Doris keeps that label after a successful
publish, so a replay does not insert the rows again. Dropping the database
forgets labels; a process restart does not.
"""
import base64
import http.client
import json
from urllib.parse import urlparse

import pymysql


class DorisSink:
    name = "doris"
    direction = "sink"
    semantic = "effectively_once"

    def __init__(self, host="127.0.0.1", http_port=8030, query_port=9030,
                 user="root", password="", database="m2s", table="m2s_rows"):
        self.host = str(host)
        self.http_port = int(http_port)
        self.query_port = int(query_port)
        self.user = str(user)
        self.password = str(password)
        self.database = str(database)
        self.table = str(table)

    def _mysql(self):
        return pymysql.connect(
            host=self.host, port=self.query_port, user=self.user,
            password=self.password, connect_timeout=10, read_timeout=30)

    def ensure(self):
        con = self._mysql()
        try:
            cur = con.cursor()
            cur.execute("DROP DATABASE IF EXISTS " + self.database)
            cur.execute("CREATE DATABASE " + self.database)
            cur.execute(
                "CREATE TABLE %s.%s ("
                "id BIGINT, v BIGINT) UNIQUE KEY(id) "
                "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                "PROPERTIES (\"replication_num\"=\"1\")"
                % (self.database, self.table))
        finally:
            con.close()

    def _put(self, host, port, path, payload, headers):
        conn = http.client.HTTPConnection(host, int(port), timeout=60)
        try:
            conn.putrequest("PUT", path)
            for key, value in headers.items():
                conn.putheader(key, value)
            conn.putheader("Content-Length", str(len(payload)))
            conn.putheader("Expect", "100-continue")
            conn.endheaders()
            conn.send(payload)
            response = conn.getresponse()
            body = response.read()
            location = response.getheader("Location")
            return response.status, location, body
        finally:
            conn.close()

    def _load(self, delivery_id, payload):
        token = base64.b64encode(
            (self.user + ":" + self.password).encode("utf-8")).decode("ascii")
        headers = {
            "Authorization": "Basic " + token,
            "label": str(delivery_id),
            "format": "json",
            "read_json_by_line": "true",
            "max_filter_ratio": "0",
        }
        path = "/api/%s/%s/_stream_load" % (self.database, self.table)
        status, location, body = self._put(self.host, self.http_port, path, payload, headers)
        if status in (301, 302, 307, 308) and location:
            parsed = urlparse(location)
            target = parsed.path or path
            if parsed.query:
                target += "?" + parsed.query
            status, _location, body = self._put(
                parsed.hostname, parsed.port or self.http_port, target, payload, headers)
        if status < 200 or status >= 300:
            raise RuntimeError("doris stream load HTTP %s" % status)
        result = json.loads(body.decode("utf-8"))
        state = str(result.get("Status", ""))
        if state == "Success":
            loaded = int(result.get("NumberLoadedRows", 0))
            return True, loaded
        if state == "Label Already Exists":
            existing = str(result.get("ExistingJobStatus", "")).upper()
            if existing == "FINISHED":
                return False, 0
            raise RuntimeError("doris label %s is %s" % (delivery_id, existing))
        raise RuntimeError("doris stream load %s: %s" % (state, result.get("Message", "")))

    def write(self, delivery_id, rows):
        payload = "\n".join(json.dumps(dict(row)) for row in rows).encode("utf-8")
        applied, _loaded = self._load(delivery_id, payload)
        return applied

    def count(self):
        con = self._mysql()
        try:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM %s.%s" % (self.database, self.table))
            return int(cur.fetchone()[0])
        finally:
            con.close()

    def version(self):
        con = self._mysql()
        try:
            cur = con.cursor()
            cur.execute("SHOW VARIABLES LIKE 'version_comment'")
            comment = str(cur.fetchone()[1])
            return comment.split()[-1]
        finally:
            con.close()
