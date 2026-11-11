#!/usr/bin/env python3
"""Event-time, windows, OVER, top-n, recursive CTE and reserve budget."""
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import j4
import relational_ir
import stream_eval
import stream_ir
import stream_sql
import stream_zset


def assert_same(actual, expected, label):
    if actual.weights == expected.weights:
        return
    changed = []
    keys = list(set(actual.weights) | set(expected.weights))[:6]
    for key in keys:
        if actual.weights.get(key) != expected.weights.get(key):
            changed.append(dict(
                actual=actual.rows.get(key),
                actual_weight=actual.weights.get(key),
                expected=expected.rows.get(key),
                expected_weight=expected.weights.get(key),
            ))
    raise AssertionError(label + " " + repr(changed))


def duck(sql, tables):
    con = duckdb.connect(":memory:")
    try:
        for name, rows in tables.items():
            con.register(name, pa.Table.from_pylist(rows))
        cursor = con.execute(sql)
        names = [item[0] for item in cursor.description]
        result = stream_zset.ZSet()
        for record in cursor.fetchall():
            result.add(dict(zip(names, record)), 1)
        return result
    finally:
        con.close()


def run(sql, sources, batches):
    compiled = stream_sql.compile_sql(sql, sources)
    graph = stream_eval.StreamGraph(compiled["plan"])
    try:
        for batch in batches:
            graph.push(batch)
        return graph.output.copy(), compiled
    finally:
        graph.close()


def rows_of(batches, name):
    state = {}
    for batch in batches:
        for row, weight in batch.get(name, ()):
            current = state.get(row["id"], (row, 0))[1] + weight
            if current == 0:
                state.pop(row["id"], None)
            else:
                state[row["id"]] = (dict(row), current)
    expanded = []
    for row, weight in state.values():
        expanded.extend([row] * weight)
    return expanded


def test_reserve_override():
    with j4.catalog_variable_scope({}):
        auto = j4.read_resource_policy()["memory_reserve_mb"]
    with j4.catalog_variable_scope(
        {"CDC_RESOURCE_MEMORY_RESERVE_MB": "1024"}
    ):
        manual = j4.read_resource_policy()["memory_reserve_mb"]
    if auto < 2048 or manual != 1024:
        raise AssertionError(
            "reserve auto=%s manual=%s" % (auto, manual))


def test_v1_id():
    mapping = dict(
        src_table="orders",
        primary_key=["id"],
        _sql=(
            'SELECT "id", "amount" * 2 AS "double_amount" '
            'FROM arrow_batch WHERE "status" = \'paid\''
        ),
        _filter_sql='"amount" > 0',
        _schema_signature=[
            ("id", "bigint", "bigint", None, None, False),
            ("amount", "decimal", "decimal(18,2)", None, None, True),
            ("status", "varchar", "varchar(16)", None, "utf8mb4_bin", True),
        ],
    )
    rel = relational_ir.mapping_ir(mapping)
    identity = stream_ir.semantic_id(stream_ir.lower_stateless(rel))
    if identity != (
        "1283fb55eb3b4b4408aa8095e8e9c66255383bb60fa630d38d6f401b109fc8ef"
    ):
        raise AssertionError("v1 stateless plan id moved")


def test_tumble_and_late():
    sources = {"events": ["id", "k", "ts", "v"]}
    rows = [
        dict(id=1, k="a", ts=3, v=1),
        dict(id=2, k="a", ts=12, v=4),
        dict(id=3, k="b", ts=12, v=5),
    ]
    sql = (
        "SELECT k, COUNT(*) AS n FROM events "
        "GROUP BY k, m2s_tumble(ts, 10)")
    batches = [{"events": [(row, 1) for row in rows]}]
    got, compiled = run(sql, sources, batches)
    expected = duck(
        "SELECT k, (ts // 10) * 10 AS w_start, "
        "(ts // 10) * 10 + 10 AS w_end, COUNT(*) AS n "
        "FROM events GROUP BY k, 2, 3",
        {"events": rows})
    # The stream names w_start from the window, duck uses the same aliases.
    assert_same(got, expected, "tumble")
    batches.append({"events": [(rows[0], -1)]})
    got, _compiled = run(sql, sources, batches)
    expected = duck(
        "SELECT k, (ts // 10) * 10 AS w_start, "
        "(ts // 10) * 10 + 10 AS w_end, COUNT(*) AS n "
        "FROM events GROUP BY k, 2, 3",
        {"events": rows[1:]})
    assert_same(got, expected, "tumble retract")
    text = stream_ir.explain(compiled["plan"])
    if "window_agg tumble" not in text:
        raise AssertionError(text)


def test_watermark_drop():
    sources = {"events": ["id", "ts", "v"]}
    sql = (
        "SELECT id, ts FROM events "
        "WHERE m2s_watermark(ts, 5, 'drop')")
    batches = [
        {"events": [(dict(id=1, ts=0, v=1), 1), (dict(id=2, ts=20, v=1), 1)]},
        {"events": [(dict(id=3, ts=10, v=1), 1), (dict(id=4, ts=30, v=1), 1)]},
    ]
    got, compiled = run(sql, sources, batches)
    # First batch watermark is 15, so ts=0 is late. Second watermark is 25,
    # so ts=10 is late and ts=30 is on time. ts=20 stays.
    expected = stream_zset.ZSet()
    expected.add(dict(id=2, ts=20), 1)
    expected.add(dict(id=4, ts=30), 1)
    assert_same(got, expected, "late drop")
    if "watermark" not in stream_ir.explain(compiled["plan"]):
        raise AssertionError("watermark missing")


def test_over_and_topn():
    sources = {"events": ["id", "k", "ts", "v"]}
    rows = [
        dict(id=1, k="a", ts=1, v=5),
        dict(id=2, k="a", ts=2, v=1),
        dict(id=3, k="b", ts=1, v=4),
    ]
    batches = [{"events": [(row, 1) for row in rows]}]
    batches.append({"events": [(dict(id=4, k="a", ts=3, v=2), 1)]})
    got, _compiled = run(
        "SELECT id, k, SUM(v) OVER (PARTITION BY k ORDER BY id) AS s "
        "FROM events",
        sources, batches)
    live = rows + [dict(id=4, k="a", ts=3, v=2)]
    expected = duck(
        "SELECT id, k, SUM(v) OVER (PARTITION BY k ORDER BY id) AS s "
        "FROM events",
        {"events": live})
    assert_same(got, expected, "over")
    got, compiled = run(
        "SELECT id, v FROM events ORDER BY v DESC LIMIT 2",
        sources, [batches[0]])
    expected = duck(
        "SELECT id, v FROM events ORDER BY v DESC, id LIMIT 2",
        {"events": rows})
    assert_same(got, expected, "topn")
    if "topn" not in stream_ir.explain(compiled["plan"]):
        raise AssertionError("topn missing")


def test_recursive_and_cte():
    sources = {"edges": ["id", "src", "dst"], "events": ["id", "ts"]}
    edges = [
        dict(id=1, src=1, dst=2),
        dict(id=2, src=2, dst=3),
        dict(id=3, src=9, dst=8),
    ]
    sql = (
        "WITH RECURSIVE reach AS ("
        " SELECT src, dst FROM edges WHERE src = 1"
        " UNION ALL"
        " SELECT r.src, e.dst FROM reach AS r"
        " JOIN edges AS e ON r.dst = e.src"
        ") SELECT src, dst FROM reach")
    got, compiled = run(
        sql, sources, [{"edges": [(row, 1) for row in edges]}])
    expected = duck(sql, {"edges": edges})
    assert_same(got, expected, "recursive")
    if "semi_naive" not in stream_ir.explain(compiled["plan"]):
        raise AssertionError("recursive strategy missing")
    got, _compiled = run(
        "WITH recent AS (SELECT id, ts FROM events WHERE ts > 0) "
        "SELECT id FROM recent",
        sources,
        [{"events": [
            (dict(id=1, ts=0), 1),
            (dict(id=2, ts=4), 1),
        ]}])
    expected = duck(
        "SELECT id FROM events WHERE ts > 0",
        {"events": [dict(id=1, ts=0), dict(id=2, ts=4)]})
    assert_same(got, expected, "cte")


def test_nondeterministic():
    sources = {"events": ["id"]}
    try:
        stream_sql.compile_sql(
            "SELECT random() AS r FROM events", sources)
    except ValueError:
        pass
    else:
        raise AssertionError("random() was accepted")
    compiled = stream_sql.compile_sql(
        "SELECT now() AS n FROM events", sources,
        nondeterministic="timed_recompute")
    if "timed_recompute" not in stream_ir.explain(compiled["plan"]):
        raise AssertionError(stream_ir.explain(compiled["plan"]))


def test_hop_session_cumulate():
    sources = {"events": ["id", "k", "ts"]}
    rows = [
        dict(id=1, k="a", ts=3),
        dict(id=2, k="a", ts=8),
        dict(id=3, k="a", ts=20),
    ]
    got, _compiled = run(
        "SELECT k, COUNT(*) AS n FROM events "
        "GROUP BY k, m2s_hop(ts, 10, 5)",
        sources, [{"events": [(row, 1) for row in rows]}])
    counts = {}
    for row in rows:
        ts = row["ts"]
        slide = 5
        size = 10
        last = (ts // slide) * slide
        start = last - ((size - 1) // slide) * slide
        while start <= ts:
            if start <= ts < start + size:
                key = (row["k"], start, start + size)
                counts[key] = counts.get(key, 0) + 1
            start += slide
    expected = stream_zset.ZSet()
    for (key, start, end), count in counts.items():
        expected.add(dict(
            k=key, w_start=start, w_end=end, n=count), 1)
    assert_same(got, expected, "hop")
    got, _compiled = run(
        "SELECT k, COUNT(*) AS n FROM events "
        "GROUP BY k, m2s_session(ts, 5)",
        sources, [{"events": [(row, 1) for row in rows]}])
    # ts 3 and 8 are one session (gap 5), 20 starts another.
    expected = stream_zset.ZSet()
    expected.add(dict(k="a", w_start=3, w_end=8, n=2), 1)
    expected.add(dict(k="a", w_start=20, w_end=20, n=1), 1)
    assert_same(got, expected, "session")
    got, _compiled = run(
        "SELECT k, COUNT(*) AS n FROM events "
        "GROUP BY k, m2s_cumulate(ts, 10, 20)",
        sources, [{"events": [(dict(id=1, k="a", ts=3), 1)]}])
    expected = stream_zset.ZSet()
    expected.add(dict(k="a", w_start=0, w_end=10, n=1), 1)
    expected.add(dict(k="a", w_start=0, w_end=20, n=1), 1)
    assert_same(got, expected, "cumulate")


def test_interval_and_ttl():
    plan = stream_ir.validate_plan(dict(
        format_version=1,
        kind="stream_dag",
        inputs=["orders", "rates"],
        output="j",
        semantics=dict(stream_ir.SEMANTICS),
        nodes=[
            dict(id="l", op="input", input="orders"),
            dict(id="r", op="input", input="rates"),
            dict(
                id="j", op="interval_join", left="l", right="r",
                keys=[dict(left="k", right="k")],
                mode="contains_point",
                projections=[
                    dict(output="id", source="left", column="id"),
                    dict(output="rate", source="right", column="rate"),
                ],
                strategy="affected_key_recompute",
                point="ts", left_start="", left_end="",
                right_start="start_ts", right_end="end_ts",
            ),
        ],
    ))
    graph = stream_eval.StreamGraph(plan)
    try:
        graph.push({
            "orders": [(dict(id=1, k=1, ts=5), 1)],
            "rates": [(
                dict(k=1, start_ts=0, end_ts=10, rate=3), 1), (
                dict(k=1, start_ts=10, end_ts=20, rate=9), 1)],
        })
        expected = stream_zset.ZSet()
        expected.add(dict(id=1, rate=3), 1)
        assert_same(graph.output, expected, "temporal")
        graph.push({"orders": [(dict(id=1, k=1, ts=15), 1)]})
        expected.add(dict(id=1, rate=9), 1)
        assert_same(graph.output, expected, "temporal second")
    finally:
        graph.close()
    if "affected_key_recompute" not in stream_ir.explain(plan):
        raise AssertionError("interval strategy missing")


def main():
    test_reserve_override()
    test_v1_id()
    test_tumble_and_late()
    test_watermark_drop()
    test_over_and_topn()
    test_recursive_and_cte()
    test_nondeterministic()
    test_hop_session_cumulate()
    test_interval_and_ttl()
    print("stream_time_test ok", flush=True)


if __name__ == "__main__":
    main()
