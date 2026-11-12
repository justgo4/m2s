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
    "clickhouse": "sink",
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
    if key == "clickhouse":
        from clickhouse_sink import ClickHouseSink
        return ClickHouseSink()
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
        decision = dict(propagate=True, rebuild=False, drop_rows=False, mv="propagate")
    elif action == "widen":
        decision = dict(propagate=True, rebuild=False, drop_rows=False, mv="propagate")
    elif action in {"drop", "narrow", "incompatible"}:
        decision = dict(propagate=False, rebuild=True, drop_rows=False, mv="rebuild")
    elif action == "add":
        decision = dict(propagate=False, rebuild=True, drop_rows=False, mv="rebuild")
    else:
        raise ValueError("DDL change is not classified")
    return decision


def apply_add_nullable(rows, column):
    copied = []
    for row in rows:
        item = dict(row)
        item.setdefault(column, None)
        copied.append(item)
    return copied


def apply_mv(change, rows):
    """Apply a classified DDL change to a downstream MV image.

    A propagated add puts NULL in the new column and keeps every row.
    A widen keeps the values. A rebuild keeps the current rows too:
    nothing is deleted while the MV waits to be rebuilt.
    """
    decision = plan_ddl(change)
    copied = [dict(row) for row in rows]
    if decision["mv"] == "propagate" and str(change.get("action")) == "add":
        column = str(change["column"])
        for row in copied:
            row.setdefault(column, None)
    decision = dict(decision)
    decision["retained_rows"] = len(copied)
    return decision, copied
