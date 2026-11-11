#!/usr/bin/env python3
"""Python fallback for the stream hot spots.

Weight merge, ordered state, a join index and row serialization live here.
A PyO3 module may replace consolidate() when it has been built. The CDC
native reader is not replaced: there is no small-profile comparison that
justifies that.
"""
import json
import struct


def consolidate(pairs):
    totals = {}
    for key, weight in pairs:
        if isinstance(weight, bool) or not isinstance(weight, int):
            raise TypeError("weight must be an int")
        name = str(key)
        totals[name] = totals.get(name, 0) + int(weight)
    return [
        (key, totals[key])
        for key in sorted(totals)
        if totals[key] != 0
    ]


def _rust_consolidate():
    try:
        import stream_hot_rs
    except ImportError:
        return None
    return stream_hot_rs.consolidate


def consolidate_weights(pairs):
    """Prefer the built Rust helper. Fall back to the Python function."""
    rust = _rust_consolidate()
    if rust is None:
        return consolidate(pairs), "python"
    return list(rust(list(pairs))), "rust"


def put_ordered(backend, key, value):
    backend.put(_bytes_key(key), _bytes_key(value))


def range_ordered(backend, start, end):
    start_b = _bytes_key(start)
    end_b = _bytes_key(end)
    return [
        (key, value)
        for key, value in backend.items()
        if start_b <= key < end_b
    ]


def _bytes_key(value):
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    raise TypeError("ordered key must be bytes or str")


def join_add(backend, key, row_id):
    current = backend.get(_bytes_key(key))
    ids = [] if current is None else current.decode("utf-8").split("\0")
    text = str(row_id)
    if text not in ids:
        ids.append(text)
    backend.put(_bytes_key(key), "\0".join(ids).encode("utf-8"))


def join_get(backend, key):
    current = backend.get(_bytes_key(key))
    if not current:
        return []
    return [item for item in current.decode("utf-8").split("\0") if item]


def dump_row(row, weight):
    if isinstance(weight, bool) or not isinstance(weight, int):
        raise TypeError("weight must be an int")
    payload = json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return struct.pack(">q", int(weight)) + payload


def load_row(blob):
    if not isinstance(blob, (bytes, bytearray)) or len(blob) < 8:
        raise ValueError("row blob is too short")
    weight = struct.unpack(">q", bytes(blob[:8]))[0]
    row = json.loads(bytes(blob[8:]).decode("utf-8"))
    return row, weight
