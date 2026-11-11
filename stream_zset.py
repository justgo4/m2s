#!/usr/bin/env python3
"""Z-set rows for incremental SQL.

A Z-set maps each row to an integer weight. +1 inserts one copy, -1 retracts
one copy, and a weight that sums to zero drops the row. Keyed MySQL changes
enter through KeyedMirror: delete is -1 of the stored before image, and an
upsert of an existing key retracts that image before inserting the new row.
"""
import datetime
import decimal
import math


RESERVED_COLUMNS = {"_sync_op", "_sync_order", "_m2s_z_weight"}


def tag(value):
    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("bool", bool(value))
    if isinstance(value, int):
        return ("int", int(value))
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            raise ValueError("non-finite z-set values are unsupported")
        return ("decimal", value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite z-set values are unsupported")
        return ("float", value.hex())
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("bytes", bytes(value))
    if isinstance(value, datetime.datetime):
        return ("datetime", value.isoformat())
    if isinstance(value, datetime.date):
        return ("date", value.isoformat())
    if isinstance(value, datetime.time):
        return ("time", value.isoformat())
    raise TypeError(
        "unsupported z-set value type: " + type(value).__name__)


def weight(value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("z-set weight must be an int")
    return int(value)


def row_key(row):
    if not isinstance(row, dict) or not row:
        raise TypeError("z-set row must be a non-empty dict")
    items = []
    for name in sorted(row):
        if not isinstance(name, str) or not name:
            raise ValueError(
                "z-set column name must be a non-empty string")
        if name in RESERVED_COLUMNS:
            raise ValueError("z-set row uses reserved column " + name)
        items.append((name, tag(row[name])))
    return tuple(items)


class ZSet:
    def __init__(self):
        self.weights = {}
        self.rows = {}

    def add(self, row, value):
        delta = weight(value)
        if delta == 0:
            return
        key = row_key(row)
        total = self.weights.get(key, 0) + delta
        if total == 0:
            self.weights.pop(key, None)
            self.rows.pop(key, None)
            return
        self.weights[key] = total
        if key not in self.rows:
            self.rows[key] = dict(row)

    def add_all(self, pairs):
        for row, value in pairs or ():
            self.add(row, value)

    def items(self):
        return [
            (dict(self.rows[key]), self.weights[key])
            for key in sorted(self.weights, key=repr)
        ]

    def copy(self):
        other = ZSet()
        other.weights = dict(self.weights)
        other.rows = {
            key: dict(value) for key, value in self.rows.items()
        }
        return other


def consolidate(pairs):
    folded = ZSet()
    folded.add_all(pairs)
    return folded.items()


class KeyedMirror:
    """Fold a primary-key delete/upsert log into Z-set deltas.

    sync_op 1 deletes the stored image. sync_op 0 inserts, or retracts the
    stored image and inserts the replacement. A delete whose columns differ
    from the stored image is rejected.
    """

    def __init__(self, key_columns):
        columns = [str(item) for item in key_columns or ()]
        if not columns or len(columns) != len(set(columns)):
            raise ValueError("primary key must be unique and non-empty")
        self.key_columns = columns
        self.rows = {}

    def apply(self, row, sync_op):
        if not isinstance(row, dict):
            raise TypeError("keyed change must be a dict")
        data = {}
        for name, value in row.items():
            if name in RESERVED_COLUMNS:
                continue
            if not isinstance(name, str) or not name:
                raise ValueError(
                    "column name must be a non-empty string")
            tag(value)
            data[name] = value
        try:
            key = tuple(data[name] for name in self.key_columns)
        except KeyError as exc:
            raise ValueError(
                "keyed change lacks primary key " + str(exc.args[0])
            ) from None
        if isinstance(sync_op, bool):
            raise ValueError("sync_op must be 0(upsert) or 1(delete)")
        try:
            op = int(sync_op)
        except (TypeError, ValueError):
            raise ValueError("sync_op must be 0(upsert) or 1(delete)")
        if op not in (0, 1):
            raise ValueError("sync_op must be 0(upsert) or 1(delete)")
        if op == 1:
            old = self.rows.get(key)
            if old is None:
                raise RuntimeError("delete of absent key")
            if set(data) != set(old) or any(
                old[name] != value for name, value in data.items()
            ):
                raise RuntimeError("delete before-image mismatch")
            self.rows.pop(key)
            return [(dict(old), -1)]
        stored = dict(data)
        old = self.rows.get(key)
        self.rows[key] = stored
        if old is None:
            return [(dict(stored), 1)]
        if old == stored:
            return []
        return [(dict(old), -1), (dict(stored), 1)]

    def snapshot(self):
        return [dict(value) for value in self.rows.values()]
