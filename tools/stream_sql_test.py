#!/usr/bin/env python3
"""Differential tests for the milestone 2 SQL subset."""
from pathlib import Path
import random
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import aggregate_ir
import join_ir
import relational_ir
import stream_eval
import stream_ir
import stream_mv
import stream_sql
import stream_zset


STATELESS_ID = (
    "1283fb55eb3b4b4408aa8095e8e9c66255383bb60fa630d38d6f401b109fc8ef")
AGGREGATE_ID = (
    "8ff37078393ac336c586fa6ef3138bcedfb99c1add01c914c298b9f93fd2b9ef")
JOIN_ID = (
    "573cac45023a5f0717b085c4b5f0027196774508357723da29b7e103d0ea4990")


def expect_error(function, error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected " + error.__name__)


def assert_same(actual, expected, label):
    if actual.weights == expected.weights:
        return
    changed = []
    for key in list(set(actual.weights) | set(expected.weights))[:6]:
        if actual.weights.get(key) != expected.weights.get(key):
            changed.append(dict(
                actual_row=actual.rows.get(key),
                actual_weight=actual.weights.get(key),
                expected_row=expected.rows.get(key),
                expected_weight=expected.weights.get(key),
            ))
    raise AssertionError(label + " " + repr(changed))


def duck_zset(sql, tables):
    con = duckdb.connect(":memory:")
    try:
        for name, (rows, columns) in tables.items():
            if rows:
                table = pa.Table.from_pylist(rows)
            else:
                fields = [pa.field(column, pa.null()) for column in columns]
                table = pa.schema(fields).empty_table()
            con.register(name, table)
        cursor = con.execute(sql)
        names = [item[0] for item in cursor.description]
        result = stream_zset.ZSet()
        for record in cursor.fetchall():
            result.add(dict(zip(names, record)), 1)
        return result
    finally:
        con.close()


def run_sql(sql, sources, batches, label, duck_sql=None):
    compiled = stream_sql.compile_sql(sql, sources)
    graph = stream_eval.StreamGraph(compiled["plan"])
    try:
        for batch in batches:
            graph.push({
                name: batch[name]
                for name in compiled["plan"]["inputs"]
                if name in batch
            })
        expected = duck_zset(duck_sql or sql, {
            name: (_snapshot(batches, name), sources[name])
            for name in compiled["plan"]["inputs"]
        })
        assert_same(graph.output, expected, label)
    finally:
        graph.close()
    return compiled


def _snapshot(batches, name):
    rows = {}
    for batch in batches:
        for row, weight in batch.get(name, ()):
            key = row["id"]
            stored = rows.get(key)
            current = 0 if stored is None else stored[1]
            current += weight
            if current < 0:
                raise AssertionError("test weight became negative")
            if current == 0:
                rows.pop(key, None)
            else:
                rows[key] = (dict(row), current)
    expanded = []
    for row, weight in rows.values():
        expanded.extend([row] * weight)
    return expanded


def test_v1_plan_ids():
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
    assert stream_ir.semantic_id(
        stream_ir.lower_stateless(rel)) == STATELESS_ID
    signature = [
        ("id", "bigint", "bigint", None, None, False),
        ("category", "varchar", "varchar(16)", None, "utf8mb4_bin", True),
        ("amount", "bigint", "bigint", None, None, True),
        ("status", "varchar", "varchar(16)", None, "utf8mb4_bin", False),
    ]
    aggregate = aggregate_ir.compile_sql(
        "db.orders", signature,
        'SELECT category, COUNT(*) AS n, COUNT(amount) AS nn, '
        'SUM(amount) AS total, AVG(amount) AS mean '
        'FROM arrow_batch WHERE "status" = \'open\' '
        'GROUP BY category',
        source_filter='"amount" IS NULL OR "amount" > 0')
    assert stream_ir.semantic_id(
        stream_ir.lower_aggregate(aggregate)) == AGGREGATE_ID
    joined = join_ir.compile_sql(
        "db.lefts",
        [
            ("id", "bigint", "bigint", "NO", None, ""),
            ("k", "bigint", "bigint", "YES", None, ""),
            ("value", "bigint", "bigint", "YES", None, ""),
        ],
        "id",
        "db.rights",
        [
            ("id", "bigint", "bigint", "NO", None, ""),
            ("k", "bigint", "bigint", "YES", None, ""),
            ("label", "varchar", "varchar(16)", "YES", "utf8mb4_bin", ""),
        ],
        "id",
        "SELECT l.value AS left_value, r.label AS right_label "
        "FROM left_batch l INNER JOIN right_batch r ON l.k = r.k")
    assert stream_ir.semantic_id(stream_ir.lower_join(joined)) == JOIN_ID


def test_dialect_and_rejections():
    sources = {"orders": ["id", "status"]}
    duck = stream_sql.compile_sql(
        "SELECT id, status FROM orders", sources)
    mysql = stream_sql.compile_sql(
        "SELECT `id`, `status` FROM `orders`", sources, read="mysql")
    assert duck["read"] == "duckdb"
    assert mysql["outputs"] == ["id", "status"]
    assert stream_ir.semantic_id(duck["plan"]) == stream_ir.semantic_id(
        mysql["plan"])
    expect_error(
        lambda: stream_sql.compile_sql(
            "SELECT `id` FROM `orders`", sources),
        ValueError)
    expect_error(
        lambda: stream_sql.compile_sql(
            "SELECT STRING_AGG(status, ',') AS s FROM orders GROUP BY id",
            {"orders": ["id", "status"]}),
        ValueError)
    expect_error(
        lambda: stream_sql.compile_sql(
            "SELECT id FROM orders LIMIT 1", sources),
        ValueError)
    grouped = stream_sql.compile_sql(
        "SELECT city, GROUP_CONCAT(name ORDER BY name SEPARATOR ',') AS s "
        "FROM customers GROUP BY city",
        {"customers": ["name", "city"]},
        read="mysql")
    assert "string_agg" in stream_ir.explain(grouped["plan"])


def test_scripted_sql():
    sources = {
        "orders": ["id", "status", "amount"],
        "customers": ["id", "name", "city"],
    }
    orders = [
        dict(id=1, status="open", amount=10),
        dict(id=2, status="open", amount=20),
        dict(id=3, status="closed", amount=5),
        dict(id=4, status="open", amount=None),
    ]
    customers = [
        dict(id=1, name="a", city="x"),
        dict(id=2, name="b", city="y"),
        dict(id=9, name="z", city=None),
    ]
    first = {
        "orders": [(row, 1) for row in orders],
        "customers": [(row, 1) for row in customers],
    }
    second = {"orders": [(orders[0], -1)]}
    cases = [
        "SELECT id, status FROM orders WHERE amount > 0",
        "SELECT status, COUNT(*) AS n, SUM(amount) AS total, "
        "MIN(amount) AS lo, MAX(amount) AS hi, "
        "COUNT(DISTINCT amount) AS d FROM orders GROUP BY status",
        "SELECT status, COUNT(*) AS n FROM orders GROUP BY status "
        "HAVING n > 1",
        "SELECT COUNT(*) AS n FROM orders",
        "SELECT DISTINCT status FROM orders",
        "SELECT amount, STRING_AGG(status, ',' ORDER BY status) AS s "
        "FROM orders WHERE amount IS NOT NULL GROUP BY amount",
        "SELECT STRING_AGG(status, ',' ORDER BY amount DESC) AS s "
        "FROM orders WHERE amount IS NOT NULL",
        "SELECT status, COUNT(*) AS n FROM orders GROUP BY ROLLUP (status)",
        "SELECT city, COUNT(*) AS n FROM customers GROUP BY CUBE (city)",
        "SELECT city, COUNT(*) AS n FROM customers "
        "GROUP BY GROUPING SETS ((city), ())",
        "SELECT o.id AS oid, c.name AS name FROM orders o "
        "INNER JOIN customers c ON o.id = c.id",
        "SELECT o.id AS oid, c.name AS name FROM orders o "
        "LEFT JOIN customers c ON o.id = c.id",
        "SELECT c.name AS name, o.status AS status FROM customers c "
        "RIGHT JOIN orders o ON c.id = o.id",
        "SELECT o.id AS oid, c.name AS name FROM orders o "
        "FULL OUTER JOIN customers c ON o.id = c.id",
        "SELECT o.id AS oid FROM orders o SEMI JOIN customers c "
        "ON o.id = c.id",
        "SELECT o.id AS oid FROM orders o ANTI JOIN customers c "
        "ON o.id = c.id",
        "SELECT o.id AS oid, c.city AS city FROM orders o "
        "JOIN customers c ON o.amount > c.id",
        "SELECT o.id AS oid, c.name AS name, x.city AS city FROM orders o "
        "JOIN customers c ON o.id = c.id "
        "JOIN customers x ON c.city = x.city",
        "SELECT status FROM orders UNION SELECT city AS status "
        "FROM customers",
        "SELECT status FROM orders UNION ALL SELECT city AS status "
        "FROM customers",
        "SELECT city FROM customers INTERSECT SELECT status FROM orders",
        "SELECT status FROM orders EXCEPT ALL SELECT status FROM orders "
        "WHERE amount < 10",
        "SELECT id FROM orders o WHERE EXISTS "
        "(SELECT 1 FROM customers c WHERE c.id = o.id)",
        "SELECT id FROM orders o WHERE NOT EXISTS "
        "(SELECT 1 FROM customers c WHERE c.id = o.id AND c.city = 'x')",
        "SELECT id FROM orders WHERE id IN (SELECT id FROM customers)",
        "SELECT id FROM orders WHERE id NOT IN "
        "(SELECT id FROM customers WHERE city IS NOT NULL)",
    ]
    for sql in cases:
        compiled = run_sql(sql, sources, [first, second], sql)
        text = stream_ir.explain(compiled["plan"])
        if "NOT IN" in sql:
            assert "keyed_join" in text
        elif "FULL OUTER" in sql or " LEFT " in sql or "ANTI " in sql:
            assert "keyed_join" in text
        elif "o.amount > c.id" in sql:
            assert "full_input_recompute" in text
        elif "INNER JOIN" in sql:
            assert "equi_join" in text


def test_random_aggregate_and_join():
    rng = random.Random(20261111)
    sources = {
        "orders": ["id", "status", "amount"],
        "customers": ["id", "name"],
    }
    orders = {}
    customers = {}
    batches = []
    for step in range(40):
        batch = {}
        if rng.randrange(3) == 0:
            row_id = rng.randrange(1, 8)
            if row_id in customers and rng.randrange(3) == 0:
                batch["customers"] = [(customers.pop(row_id), -1)]
            else:
                row = dict(
                    id=row_id,
                    name=rng.choice(["a", "b", "c"]))
                if row_id in customers:
                    batch["customers"] = [
                        (customers[row_id], -1), (row, 1)]
                else:
                    batch["customers"] = [(row, 1)]
                customers[row_id] = row
        else:
            row_id = rng.randrange(1, 12)
            if row_id in orders and rng.randrange(4) == 0:
                batch["orders"] = [(orders.pop(row_id), -1)]
            else:
                row = dict(
                    id=row_id,
                    status=rng.choice(["open", "closed", "hold"]),
                    amount=None if rng.randrange(5) == 0 else rng.randrange(0, 9),
                )
                if row_id in orders:
                    batch["orders"] = [(orders[row_id], -1), (row, 1)]
                else:
                    batch["orders"] = [(row, 1)]
                orders[row_id] = row
        batches.append(batch)
    run_sql(
        "SELECT status, COUNT(*) AS n, MIN(amount) AS lo, "
        "MAX(amount) AS hi, COUNT(DISTINCT amount) AS d "
        "FROM orders GROUP BY status",
        sources, batches, "random aggregate")
    run_sql(
        "SELECT o.status AS status, c.name AS name FROM orders o "
        "LEFT JOIN customers c ON o.id = c.id",
        sources, batches, "random left join")


def test_materialized_view():
    upstream = stream_sql.compile_sql(
        "SELECT id, status, amount FROM orders WHERE amount > 0",
        {"orders": ["id", "status", "amount"]})
    downstream = stream_sql.compile_sql(
        "SELECT status, COUNT(*) AS n FROM upstream GROUP BY status",
        {"upstream": ["id", "status", "amount"]})
    view = stream_mv.MaterializedView(
        stream_eval.StreamGraph(upstream["plan"]),
        stream_eval.StreamGraph(downstream["plan"]),
        "upstream")
    try:
        rows = [
            dict(id=1, status="open", amount=10),
            dict(id=2, status="open", amount=3),
            dict(id=3, status="closed", amount=0),
        ]
        view.push({"orders": [(row, 1) for row in rows]}, 1)
        expected = duck_zset(
            "SELECT status, COUNT(*) AS n FROM "
            "(SELECT id, status, amount FROM orders WHERE amount > 0) t "
            "GROUP BY status",
            {"orders": (rows, ["id", "status", "amount"])})
        assert_same(view.downstream.output, expected, "mv chain")
        expect_error(lambda: view.push({"orders": []}, 1), RuntimeError)
        expect_error(lambda: view.push({"orders": []}, 3), RuntimeError)
        view.push({"orders": [(rows[0], -1)]}, 2)
        assert view.watermark == 2
    finally:
        view.close()


def main():
    test_v1_plan_ids()
    test_dialect_and_rejections()
    test_scripted_sql()
    test_random_aggregate_and_join()
    test_materialized_view()
    print("stream_sql_test ok", flush=True)


if __name__ == "__main__":
    main()
