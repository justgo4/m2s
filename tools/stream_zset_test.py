#!/usr/bin/env python3
"""Z-set DAG differential tests for the migrated v1 operators."""
from decimal import Decimal
from pathlib import Path
import random
import sqlite3
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import aggregate_ir
import aggregate_state
import incremental_ir
import join_ir
import join_state
import relational_ir
import stateful_task_plan
import stream_eval
import stream_ir
import stream_zset


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
    for key in list(set(actual.weights) | set(expected.weights))[:8]:
        if actual.weights.get(key) != expected.weights.get(key):
            changed.append(dict(
                actual_row=actual.rows.get(key),
                actual_weight=actual.weights.get(key),
                expected_row=expected.rows.get(key),
                expected_weight=expected.weights.get(key),
            ))
    raise AssertionError(label + " " + repr(changed))


def query_zset(sql, bindings):
    con = duckdb.connect(":memory:")
    try:
        for name, (rows, schema) in bindings.items():
            if rows:
                table = pa.Table.from_pylist(rows, schema=schema)
            else:
                table = schema.empty_table()
            con.register(name, table)
        cursor = con.execute(sql)
        names = [item[0] for item in cursor.description]
        result = stream_zset.ZSet()
        for record in cursor.fetchall():
            result.add(dict(zip(names, record)), 1)
        return result
    finally:
        con.close()


def rows_zset(rows):
    result = stream_zset.ZSet()
    for row in rows:
        result.add(row, 1)
    return result


def stateless_relation():
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
    return relational_ir.mapping_ir(mapping)


def test_zset_and_mirror():
    folded = stream_zset.ZSet()
    folded.add(dict(id=1, amount=Decimal("2.00")), 1)
    folded.add(dict(id=1, amount=Decimal("2.00")), -1)
    assert folded.weights == {}
    folded.add(dict(id=2), 2)
    assert list(folded.weights.values()) == [2]
    expect_error(
        lambda: folded.add(dict(id=3), True), TypeError)
    mirror = stream_zset.KeyedMirror(["id"])
    assert mirror.apply(dict(id=1, amount=Decimal("2.00")), 0) == [
        (dict(id=1, amount=Decimal("2.00")), 1)]
    assert mirror.apply(dict(id=1, amount=Decimal("2.00")), 0) == []
    changed = mirror.apply(dict(id=1, amount=Decimal("3.00")), 0)
    assert changed == [
        (dict(id=1, amount=Decimal("2.00")), -1),
        (dict(id=1, amount=Decimal("3.00")), 1),
    ]
    expect_error(
        lambda: mirror.apply(dict(id=1, amount=Decimal("9.00")), 1),
        RuntimeError)
    assert mirror.apply(dict(id=1, amount=Decimal("3.00")), 1) == [
        (dict(id=1, amount=Decimal("3.00")), -1)]
    expect_error(lambda: mirror.apply(dict(id=1, amount=Decimal("3.00")), 1))
    assert mirror.snapshot() == []


def test_plan_identity():
    rel = stateless_relation()
    plan = stream_ir.lower_stateless(rel)
    assert stream_ir.semantic_id(plan) == stream_ir.semantic_id(
        stream_ir.lower_stateless(rel))
    assert [node["op"] for node in plan["nodes"]] == [
        "input", "filter", "filter", "project"]
    assert plan["nodes"][1]["scope"] == "source"
    assert plan["nodes"][2]["scope"] == "query"
    delta = incremental_ir.compile_ir(rel)
    assert incremental_ir.semantic_id(delta) == incremental_ir.semantic_id(
        incremental_ir.compile_ir(rel))
    text = stream_ir.explain(plan)
    assert "filter" in text and "project" in text
    broken = dict(plan)
    broken["nodes"] = list(plan["nodes"]) + [dict(
        id="loop", op="filter", input="loop",
        predicate="TRUE", scope="predicate")]
    expect_error(lambda: stream_ir.validate_plan(broken), ValueError)
    bad_agg = dict(
        format_version=1, kind="stream_dag", inputs=["source"],
        output="agg", semantics=dict(stream_ir.SEMANTICS),
        nodes=[
            dict(id="in_source", op="input", input="source"),
            dict(
                id="agg", op="aggregate", input="in_source",
                group_keys=["id"],
                aggregates=[dict(output="m", function="median", input="id")],
            ),
        ])
    expect_error(lambda: stream_ir.validate_plan(bad_agg), ValueError)


def test_stateless_matches_duckdb():
    rel = stateless_relation()
    plan = stream_ir.lower_stateless(rel)
    graph = stream_eval.StreamGraph(plan)
    mirror = stream_zset.KeyedMirror(["id"])
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("amount", pa.decimal128(18, 2)),
        pa.field("status", pa.string()),
    ])
    script = [
        (1, Decimal("2.00"), "paid", 0),
        (1, Decimal("2.00"), "open", 0),
        (2, Decimal("-1.00"), "paid", 0),
        (3, Decimal("4.00"), "paid", 0),
        (3, Decimal("5.00"), "paid", 0),
        (4, None, "paid", 0),
        (3, Decimal("5.00"), "paid", 1),
    ]
    try:
        for row_id, amount, status, op in script:
            row = dict(id=row_id, amount=amount, status=status)
            graph.push(dict(source=mirror.apply(row, op)))
        expected = query_zset(
            'SELECT id, amount * 2 AS double_amount FROM src '
            'WHERE amount > 0 AND status = \'paid\'',
            dict(src=(mirror.snapshot(), schema)))
        assert_same(graph.output, expected, "scripted stateless")
        rng = random.Random(20261111)
        for _ in range(80):
            row_id = rng.randrange(1, 12)
            if rng.randrange(8) == 0 and any(
                item["id"] == row_id for item in mirror.snapshot()
            ):
                current = [
                    item for item in mirror.snapshot() if item["id"] == row_id
                ][0]
                delta = mirror.apply(current, 1)
            else:
                amount = (
                    None if rng.randrange(6) == 0
                    else Decimal(rng.randrange(-50, 80)).scaleb(-2))
                delta = mirror.apply(dict(
                    id=row_id, amount=amount,
                    status=rng.choice(["paid", "open", "hold"]),
                ), 0)
            graph.push(dict(source=delta))
        expected = query_zset(
            'SELECT id, amount * 2 AS double_amount FROM src '
            'WHERE amount > 0 AND status = \'paid\'',
            dict(src=(mirror.snapshot(), schema)))
        assert_same(graph.output, expected, "random stateless")
    finally:
        graph.close()


def aggregate_plan():
    signature = [
        ("id", "bigint", "bigint", None, None, False),
        ("category", "varchar", "varchar(16)", None, "utf8mb4_bin", True),
        ("amount", "bigint", "bigint", None, None, True),
        ("status", "varchar", "varchar(16)", None, "utf8mb4_bin", False),
    ]
    ir = aggregate_ir.compile_sql(
        "db.orders", signature,
        'SELECT category, COUNT(*) AS n, COUNT(amount) AS nn, '
        'SUM(amount) AS total, AVG(amount) AS mean '
        'FROM arrow_batch WHERE "status" = \'open\' '
        'GROUP BY category',
        source_filter='"amount" IS NULL OR "amount" > 0')
    plan = stream_ir.lower_aggregate(ir)
    assert [node["id"] for node in plan["nodes"]] == [
        "in_source", "filter_source", "filter_query", "aggregate"]
    assert "aggregate" in stream_ir.explain(plan)
    return ir, plan


def aggregate_changes(delta):
    changes = []
    for row, value in delta:
        if value not in (1, -1):
            raise AssertionError("keyed delta weight " + str(value))
        if row["status"] != "open":
            continue
        if not (row["amount"] is None or row["amount"] > 0):
            continue
        item = dict(row)
        item["_sync_op"] = 0 if value > 0 else 1
        changes.append(item)
    return changes


def test_aggregate_matches_engines():
    _ir, plan = aggregate_plan()
    graph = stream_eval.StreamGraph(plan)
    mirror = stream_zset.KeyedMirror(["id"])
    db = sqlite3.connect(":memory:", isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON")
    aggregate_state.install(db)
    aggregate_state.create_state(
        db, "agg", aggregate_ir.state_spec(_ir), watermark=0)
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("category", pa.string()),
        pa.field("amount", pa.int64()),
        pa.field("status", pa.string()),
    ])
    sql = (
        "SELECT category, COUNT(*) AS n, COUNT(amount) AS nn, "
        "SUM(amount) AS total, AVG(amount) AS mean FROM src "
        "WHERE status = 'open' AND (amount IS NULL OR amount > 0) "
        "GROUP BY category")
    seq = 0

    def check(label):
        expected = query_zset(
            sql, dict(src=(mirror.snapshot(), schema)))
        assert_same(graph.output, expected, label + " duckdb")
        state_rows = []
        for row in aggregate_state.read_rows(db, "agg"):
            state_rows.append(dict(
                (name, value) for name, value in row.items()
                if name != "_row_count"))
        assert_same(graph.output, rows_zset(state_rows), label + " state")

    def push(row, op):
        nonlocal seq
        delta = mirror.apply(row, op)
        if not delta:
            return
        seq += 1
        graph.push(dict(source=delta))
        assert aggregate_state.apply_transaction(
            db, "agg", seq, aggregate_changes(delta))

    try:
        before = graph.output.copy()
        expect_error(
            lambda: graph.push(dict(source=[(dict(
                id=1, category="a", amount=1, status="open"), -1)])),
            RuntimeError)
        assert graph.output.weights == before.weights
        push(dict(id=1, category="a", amount=10, status="open"), 0)
        push(dict(id=2, category="a", amount=None, status="open"), 0)
        push(dict(id=3, category="b", amount=-4, status="open"), 0)
        push(dict(id=4, category="b", amount=5, status="closed"), 0)
        push(dict(id=5, category=None, amount=7, status="open"), 0)
        check("scripted aggregate")
        push(dict(id=1, category="a", amount=10, status="open"), 1)
        push(dict(id=2, category="c", amount=3, status="open"), 0)
        check("retract aggregate")
        rng = random.Random(20261111)
        live = {row["id"]: row for row in mirror.snapshot()}
        for step in range(100):
            row_id = rng.randrange(1, 25)
            current = live.get(row_id)
            if current is not None and rng.randrange(5) == 0:
                push(current, 1)
                live.pop(row_id, None)
            else:
                amount = None if rng.randrange(5) == 0 else rng.randrange(-5, 15)
                row = dict(
                    id=row_id,
                    category=rng.choice(["a", "b", "c", None]),
                    amount=amount,
                    status=rng.choice(["open", "closed"]),
                )
                push(row, 0)
                live[row_id] = row
            if step % 20 == 19:
                check("random aggregate " + str(step))
        check("final aggregate")
        expect_error(
            lambda: graph.push(dict(other=[])), ValueError)
    finally:
        graph.close()
        db.close()


def join_plan():
    left = [
        ("id", "bigint", "bigint", "NO", None, ""),
        ("k", "bigint", "bigint", "YES", None, ""),
        ("value", "bigint", "bigint", "YES", None, ""),
    ]
    right = [
        ("id", "bigint", "bigint", "NO", None, ""),
        ("k", "bigint", "bigint", "YES", None, ""),
        ("label", "varchar", "varchar(16)", "YES", "utf8mb4_bin", ""),
    ]
    ir = join_ir.compile_sql(
        "db.lefts", left, "id",
        "db.rights", right, "id",
        "SELECT l.value AS left_value, r.label AS right_label "
        "FROM left_batch l INNER JOIN right_batch r ON l.k = r.k")
    plan = stream_ir.lower_join(ir)
    assert "equi_join" in stream_ir.explain(plan)
    assert stream_ir.semantic_id(plan) == stream_ir.semantic_id(
        stream_ir.lower_join(ir))
    return ir, plan


def test_join_matches_engines():
    ir, plan = join_plan()
    graph = stream_eval.StreamGraph(plan)
    left = stream_zset.KeyedMirror(["id"])
    right = stream_zset.KeyedMirror(["id"])
    db = sqlite3.connect(":memory:", isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON")
    join_state.install(db)
    join_state.create_state(
        db, "j", join_ir.state_spec(ir), watermark=0)
    left_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("k", pa.int64()),
        pa.field("value", pa.int64()),
    ])
    right_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("k", pa.int64()),
        pa.field("label", pa.string()),
    ])
    sql = (
        "SELECT l.value AS left_value, r.label AS right_label "
        "FROM lefts l INNER JOIN rights r ON l.k = r.k")
    seq = 0

    def check(label):
        expected = query_zset(sql, dict(
            lefts=(left.snapshot(), left_schema),
            rights=(right.snapshot(), right_schema)))
        assert_same(graph.output, expected, label + " duckdb")
        assert_same(
            graph.output,
            rows_zset(join_state.read_rows(db, "j")),
            label + " state")

    def push(side, row, op):
        nonlocal seq
        mirror = left if side == "left" else right
        delta = mirror.apply(row, op)
        if not delta:
            return
        seq += 1
        changes = []
        for item, value in delta:
            if value not in (1, -1):
                raise AssertionError("join weight " + str(value))
            stored = dict(item)
            stored["_sync_op"] = 0 if value > 0 else 1
            changes.append((side, stored))
        payload = dict(left=[], right=[])
        payload[side] = delta
        graph.push(payload)
        join_state.apply_transaction(db, "j", seq, changes)

    try:
        before = graph.output.copy()
        expect_error(
            lambda: graph.push(dict(
                left=[(dict(id=1, k=1, value=1), -1)], right=[])),
            RuntimeError)
        assert graph.output.weights == before.weights
        push("left", dict(id=1, k=1, value=10), 0)
        push("left", dict(id=2, k=1, value=10), 0)
        push("right", dict(id=1, k=1, label="x"), 0)
        push("right", dict(id=2, k=None, label="z"), 0)
        push("left", dict(id=3, k=None, value=4), 0)
        check("null and multiplicity")
        assert any(weight == 2 for weight in graph.output.weights.values())
        push("left", dict(id=2, k=1, value=10), 1)
        check("retract one left")
        push("left", dict(id=4, k=2, value=7), 0)
        push("right", dict(id=3, k=2, label="y"), 0)
        push("left", dict(id=1, k=2, value=8), 0)
        check("key move")
        rng = random.Random(20261111)
        live = dict(left={}, right={})
        mirrors = dict(left=left, right=right)
        for step in range(80):
            side = rng.choice(["left", "right"])
            row_id = rng.randrange(1, 16)
            current = live[side].get(row_id)
            if current is not None and rng.randrange(4) == 0:
                push(side, current, 1)
                live[side].pop(row_id, None)
            elif side == "left":
                row = dict(
                    id=row_id,
                    k=None if rng.randrange(7) == 0 else rng.randrange(1, 5),
                    value=rng.randrange(0, 6),
                )
                push(side, row, 0)
                live[side][row_id] = row
            else:
                row = dict(
                    id=row_id,
                    k=None if rng.randrange(7) == 0 else rng.randrange(1, 5),
                    label=rng.choice(["x", "y", "z"]),
                )
                push(side, row, 0)
                live[side][row_id] = row
            if step % 20 == 19:
                check("random join " + str(step))
        check("final join")
        del mirrors
    finally:
        graph.close()
        db.close()


def test_composed_dags():
    plan = stream_ir.validate_plan(dict(
        format_version=1,
        kind="stream_dag",
        inputs=["source"],
        output="agg",
        semantics=dict(stream_ir.SEMANTICS),
        nodes=[
            dict(id="in_source", op="input", input="source"),
            dict(
                id="filt", op="filter", input="in_source",
                predicate='"status" = \'paid\'', scope="predicate"),
            dict(
                id="proj", op="project", input="filt",
                expressions=[
                    dict(output="category", expression='"category"'),
                    dict(output="amount", expression='"amount"'),
                ]),
            dict(
                id="agg", op="aggregate", input="proj",
                group_keys=["category"],
                aggregates=[
                    dict(output="n", function="count", input="*"),
                    dict(output="total", function="sum", input="amount"),
                ]),
        ],
    ))
    rows = [
        dict(id=1, status="paid", category="a", amount=10),
        dict(id=2, status="paid", category="a", amount=10),
        dict(id=3, status="open", category="a", amount=99),
        dict(id=4, status="paid", category="b", amount=4),
    ]
    graph = stream_eval.StreamGraph(plan)
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("category", pa.string()),
        pa.field("amount", pa.int64()),
    ])
    try:
        graph.push(dict(source=[(row, 1) for row in rows[:2]]))
        graph.push(dict(source=[(row, 1) for row in rows[2:]]))
        expected = query_zset(
            "SELECT category, COUNT(*) AS n, SUM(amount) AS total "
            "FROM src WHERE status = 'paid' GROUP BY category",
            dict(src=(rows, schema)))
        assert_same(graph.output, expected, "filter project aggregate")
    finally:
        graph.close()

    joined = stream_ir.validate_plan(dict(
        format_version=1,
        kind="stream_dag",
        inputs=["left", "right"],
        output="agg",
        semantics=dict(stream_ir.SEMANTICS),
        nodes=[
            dict(id="in_left", op="input", input="left"),
            dict(id="in_right", op="input", input="right"),
            dict(
                id="join", op="equi_join", left="in_left", right="in_right",
                join_pairs=[dict(left="k", right="k")],
                projections=[
                    dict(output="name", source="right", column="name"),
                    dict(output="v", source="left", column="v"),
                ]),
            dict(
                id="agg", op="aggregate", input="join",
                group_keys=["name"],
                aggregates=[
                    dict(output="n", function="count", input="*"),
                ]),
        ],
    ))
    left_rows = [
        dict(id=1, k=1, v=3),
        dict(id=2, k=1, v=3),
        dict(id=3, k=2, v=9),
        dict(id=4, k=None, v=1),
    ]
    right_rows = [
        dict(id=1, k=1, name="x"),
        dict(id=2, k=2, name="y"),
        dict(id=3, k=None, name="z"),
    ]
    graph = stream_eval.StreamGraph(joined)
    left_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("k", pa.int64()),
        pa.field("v", pa.int64()),
    ])
    right_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("k", pa.int64()),
        pa.field("name", pa.string()),
    ])
    try:
        graph.push(dict(
            left=[(left_rows[0], 1)],
            right=[(row, 1) for row in right_rows]))
        graph.push(dict(left=[(row, 1) for row in left_rows[1:]], right=[]))
        graph.push(dict(left=[(left_rows[1], -1)], right=[]))
        kept = [left_rows[0], left_rows[2], left_rows[3]]
        expected = query_zset(
            "SELECT r.name AS name, COUNT(*) AS n "
            "FROM lefts l INNER JOIN rights r ON l.k = r.k "
            "GROUP BY r.name",
            dict(
                lefts=(kept, left_schema),
                rights=(right_rows, right_schema)))
        assert_same(graph.output, expected, "join aggregate")
    finally:
        graph.close()


def test_catalog_plan_carries_stream_id():
    metadata = {
        "orders": dict(
            schema_signature=[
                ("id", "bigint", "bigint", "NO", None, ""),
                ("category", "varchar", "varchar(32)", "NO", "utf8mb4_bin", ""),
                ("customer_id", "bigint", "bigint", "YES", None, ""),
                ("amount", "decimal", "decimal(18,2)", "YES", None, ""),
            ],
            primary_key=["id"],
        ),
        "customers": dict(
            schema_signature=[
                ("id", "bigint", "bigint", "NO", None, ""),
                ("name", "varchar", "varchar(64)", "YES", "utf8mb4_bin", ""),
            ],
            primary_key=["id"],
        ),
    }
    aggregate_manifest = dict(
        kind="aggregate", sink="starrocks.agg", target_table="agg",
        source_relations=["orders"], task_version=5,
        sql=(
            "SELECT category, COUNT(*) AS n FROM mysql.orders "
            "GROUP BY category"))
    target = [
        dict(name="category", type="VARCHAR(32)", nullable=False, key=True),
        dict(name="n", type="BIGINT", nullable=False, key=False),
    ]
    compiled = stateful_task_plan.compile_ir(
        aggregate_manifest, "db", metadata)
    assert compiled["stream_plan_id"] == stream_ir.semantic_id(
        compiled["stream_plan"])
    task = stateful_task_plan.compile_task(
        aggregate_manifest, 11, "db", metadata, target)
    again = stateful_task_plan.compile_task(
        aggregate_manifest, 11, "db", metadata, target)
    assert task["stream_plan_id"] == again["stream_plan_id"]
    assert task["task"]["descriptor_hash"] == again["task"]["descriptor_hash"]
    assert "stream_plan" not in task["task"]


def main():
    test_zset_and_mirror()
    test_plan_identity()
    test_stateless_matches_duckdb()
    test_aggregate_matches_engines()
    test_join_matches_engines()
    test_composed_dags()
    test_catalog_plan_carries_stream_id()
    print("stream_zset_test ok", flush=True)


if __name__ == "__main__":
    main()
