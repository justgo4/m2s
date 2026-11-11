#!/usr/bin/env python3
"""Connector SDK. Only connectors with a real incremental path are openable.

MySQL changelog events and a StarRocks primary-key apply are implemented.
The live StarRocks HTTP writer remains in j4. PostgreSQL, Kafka, Doris,
ClickHouse, Iceberg and Paimon are refused instead of being stubbed.
"""


SEMANTICS = ("at_least_once", "effectively_once", "downstream_dedup")
_IMPLEMENTED = {
    "mysql": "source",
    "starrocks": "sink",
}


class UnsupportedConnector(ValueError):
    pass


def open_connector(name, direction):
    key = str(name).strip().lower()
    direction = str(direction).strip().lower()
    if key not in _IMPLEMENTED or _IMPLEMENTED[key] != direction:
        raise UnsupportedConnector(
            "connector %s/%s is not implemented; refusing it" % (key, direction))
    if key == "mysql":
        return MysqlSource()
    return StarrocksSink()


class MysqlSource:
    name = "mysql"
    direction = "source"
    semantic = "effectively_once"

    def __init__(self):
        self.applied = set()

    def poll(self, events):
        fresh = []
        for event in events:
            identity = (str(event["server"]), str(event["gtid"]))
            if identity in self.applied:
                continue
            self.applied.add(identity)
            fresh.append(dict(event))
        return fresh


class StarrocksSink:
    name = "starrocks"
    direction = "sink"
    semantic = "effectively_once"

    def __init__(self):
        self.rows = {}
        self.deliveries = set()

    def write(self, delivery_id, events):
        delivery_id = str(delivery_id)
        if delivery_id in self.deliveries:
            return False
        for event in events:
            key = event["pk"]
            op = str(event["op"]).lower()
            if op == "delete":
                self.rows.pop(key, None)
            elif op in {"insert", "upsert"}:
                self.rows[key] = dict(event["values"])
            else:
                raise ValueError("unknown changelog op")
        self.deliveries.add(delivery_id)
        return True


def plan_ddl(change):
    action = str(change.get("action") or "")
    if action == "add" and bool(change.get("nullable")):
        return dict(propagate=True, rebuild=False, drop_rows=False)
    if action in {"drop", "narrow"}:
        return dict(propagate=False, rebuild=True, drop_rows=False)
    raise ValueError("DDL change is not classified")


def apply_add_nullable(rows, column):
    copied = []
    for row in rows:
        item = dict(row)
        item.setdefault(column, None)
        copied.append(item)
    return copied
