#!/usr/bin/env python3
"""Read PostgreSQL logical decoding and apply it to a primary-key image."""
import os
import re

import psycopg2


_FIELD = re.compile(r"(\w+)\[(\w+)\]:(null|\S+)")


def connect(dbname=None):
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "127.0.0.1"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=dbname or os.environ.get("PGDATABASE", "m2s_cdc"),
        user=os.environ.get("PGUSER", "m2s"))


def parse_change(line):
    text = str(line)
    if " INSERT: " in text:
        op = "insert"
        body = text.split(" INSERT: ", 1)[1]
    elif " UPDATE: " in text:
        op = "update"
        body = text.split(" UPDATE: ", 1)[1]
    elif " DELETE: " in text:
        op = "delete"
        body = text.split(" DELETE: ", 1)[1]
    else:
        return None
    fields = {}
    for name, kind, value in _FIELD.findall(body):
        if value == "null":
            fields[name] = None
        elif kind == "integer":
            fields[name] = int(value)
        elif len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            fields[name] = value[1:-1]
        else:
            fields[name] = value
    return op, fields


def apply_changes(state, lines):
    for line in lines:
        parsed = parse_change(line)
        if parsed is None:
            continue
        op, fields = parsed
        key = fields["id"]
        if op == "delete":
            state.pop(key, None)
        else:
            state[key] = fields
    return state


def read_slot(connection, slot):
    with connection.cursor() as cur:
        cur.execute(
            "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL)",
            (slot,))
        return [row[0] for row in cur.fetchall()]
