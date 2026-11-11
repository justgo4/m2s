#!/usr/bin/env python3
"""Synthetic MySQL to StarRocks path. Unimplemented connectors refuse."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_connector


def test_incremental_and_ddl():
    source = stream_connector.open_connector("mysql", "source")
    sink = stream_connector.open_connector("starrocks", "sink")
    if source.semantic != "effectively_once" or sink.semantic != "effectively_once":
        raise AssertionError("semantic grade missing")
    first = source.poll([
        dict(server="a", gtid="1", op="insert", pk=1, values={"id": 1, "v": 5}),
        dict(server="b", gtid="1", op="insert", pk=2, values={"id": 2, "v": 7}),
    ])
    assert sink.write("d1", first) is True
    assert sink.write("d1", first) is False
    again = source.poll([
        dict(server="a", gtid="1", op="insert", pk=1, values={"id": 1, "v": 9}),
        dict(server="a", gtid="2", op="delete", pk=1, values={"id": 1}),
    ])
    sink.write("d2", again)
    if sink.rows != {2: {"id": 2, "v": 7}}:
        raise AssertionError(sink.rows)
    for name in ("postgresql", "kafka", "doris", "clickhouse", "iceberg", "paimon"):
        try:
            stream_connector.open_connector(name, "source")
            raise AssertionError(name + " was accepted")
        except stream_connector.UnsupportedConnector:
            pass
    added = stream_connector.plan_ddl(dict(action="add", nullable=True, column="note"))
    if not added["propagate"] or added["drop_rows"]:
        raise AssertionError(added)
    rows = stream_connector.apply_add_nullable([{"id": 2}], "note")
    if rows != [{"id": 2, "note": None}]:
        raise AssertionError(rows)
    dropped = stream_connector.plan_ddl(dict(action="drop", column="v"))
    if dropped["propagate"] or not dropped["rebuild"] or dropped["drop_rows"]:
        raise AssertionError(dropped)
    if sink.rows != {2: {"id": 2, "v": 7}}:
        raise AssertionError("DDL planning dropped a row")


def main():
    test_incremental_and_ddl()
    print("stream_connector_test ok", flush=True)


if __name__ == "__main__":
    main()
