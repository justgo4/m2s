#!/usr/bin/env python3
"""TPC-DS queries against an independent DuckDB run.

DuckDB's TPC-DS extension supplies the data and the query text at runtime.
The official text is not copied into this repository. A query that falls
back to a full recompute is a failure.
"""
from pathlib import Path
import os
import sys

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_eval
import stream_sql
import stream_zset


def _columns(con):
    found = {}
    for name, in con.execute("SHOW TABLES").fetchall():
        if str(name).startswith("tpcds"):
            continue
        found[name] = [row[0] for row in con.execute("DESCRIBE " + name).fetchall()]
    return found


def _rows(con, name, columns):
    quoted = ", ".join('"%s"' % column for column in columns)
    cursor = con.execute("SELECT %s FROM %s" % (quoted, name))
    return [dict(zip(columns, record)) for record in cursor.fetchall()]


def _norm(value):
    if value is None:
        return None
    if hasattr(value, "as_tuple"):
        return format(value, "f").rstrip("0").rstrip(".") or "0"
    if isinstance(value, float):
        return "%.6f" % value
    return str(value)


def _bag(zset):
    rows = []
    for row, weight in zset.items():
        item = tuple(sorted((key, _norm(value)) for key, value in row.items()))
        rows.extend([item] * weight)
    return sorted(
        rows,
        key=lambda row: tuple(
            (key, value is None, "" if value is None else value)
            for key, value in row))


def _oracle(con, sql):
    cursor = con.execute(sql)
    names = [item[0] for item in cursor.description]
    result = stream_zset.ZSet()
    for record in cursor.fetchall():
        result.add(dict(zip(names, record)), 1)
    return result


def _mode(plan):
    for node in plan["nodes"]:
        if node["op"] in {"recompute", "theta_join"} or node.get("full_input_recompute"):
            return "recompute"
    return "incremental"


def main():
    database = os.environ.get("TPCDS_DB", "")
    if database:
        con = duckdb.connect(database, read_only=True)
        con.execute("LOAD tpcds")
    else:
        con = duckdb.connect()
        con.execute("INSTALL tpcds")
        con.execute("LOAD tpcds")
        con.execute("CALL dsdgen(sf=0.01)")
    columns = _columns(con)
    tables = {name: _rows(con, name, cols) for name, cols in columns.items()}
    queries = con.execute(
        "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr").fetchall()
    only = sys.argv[1:]
    failures = []
    modes = []
    for number, sql in queries:
        label = "%02d" % number
        if only and label not in only and str(number) not in only:
            continue
        try:
            compiled = stream_sql.compile_sql(sql, columns)
            mode = _mode(compiled["plan"])
            if mode != "incremental":
                raise AssertionError("plan fell back to full recompute")
            graph = stream_eval.StreamGraph(compiled["plan"])
            try:
                used = {
                    node["input"]
                    for node in compiled["plan"]["nodes"]
                    if node["op"] == "input"
                }
                batch = {
                    name: [(row, 1) for row in rows]
                    for name, rows in tables.items()
                    if name in used
                }
                graph.push(batch)
                got = graph.output.copy()
            finally:
                graph.close()
            expected = _oracle(con, sql)
            if _bag(got) != _bag(expected):
                raise AssertionError(
                    "row mismatch got=%d expected=%d"
                    % (sum(got.weights.values()), sum(expected.weights.values())))
            modes.append((number, mode, sum(got.weights.values())))
            print("tpcds q%02d %s rows=%d" % (number, mode, modes[-1][2]), flush=True)
        except Exception as exc:
            line = str(exc).splitlines()
            failures.append((number, type(exc).__name__, (line[0] if line else "")[:180]))
            print(
                "tpcds q%02d FAIL %s %s" % (number, failures[-1][1], failures[-1][2]),
                flush=True)
    con.close()
    for number, mode, rows in modes:
        print("tpcds_mode q%02d %s rows=%d" % (number, mode, rows), flush=True)
    if failures:
        for item in failures:
            print("tpcds_fail", item, flush=True)
        raise SystemExit(1)
    print("tpcds_diff_test ok queries=%d scale=0.01" % len(modes), flush=True)


if __name__ == "__main__":
    main()
