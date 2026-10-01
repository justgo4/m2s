#!/usr/bin/env python3
from collections import Counter
from decimal import Decimal
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import relational_ir


def mapping(sql, full_filter=""):
    return dict(
        src_table="orders",
        primary_key=["id"],
        _sql=sql,
        _filter_sql=full_filter,
        _schema_signature=[
            ("id", "bigint", "bigint", None, None, False),
            ("amount", "decimal", "decimal(18,2)", None, None, True),
            ("status", "varchar", "varchar(16)", None, "utf8mb4_bin", True),
        ],
    )


def expect_error(function):
    try:
        function()
    except (ValueError, TypeError):
        return
    raise AssertionError("expected IR validation error")


def main():
    a = relational_ir.mapping_ir(mapping(
        'SELECT "id", "amount" * 2 AS "double_amount" '
        'FROM arrow_batch WHERE "status" = \'paid\'',
        '"amount" > 0',
    ))
    b = relational_ir.mapping_ir(mapping(
        'select "id", "amount" * 2 as "double_amount" '
        'from arrow_batch where "status" = \'paid\' order by "id"',
        '"amount">0',
    ))
    assert relational_ir.semantic_id(a) == relational_ir.semantic_id(b)
    assert a["source_filter"] == '"amount" > 0'
    assert a["query_filter"] == '"status" = \'paid\''
    assert [item["output"] for item in a["projection"]] == [
        "id", "double_amount"]

    changed_filter = relational_ir.mapping_ir(mapping(
        'SELECT "id", "amount" * 2 AS "double_amount" '
        'FROM arrow_batch WHERE "status" = \'open\'',
        '"amount" > 0',
    ))
    assert relational_ir.semantic_id(a) != relational_ir.semantic_id(
        changed_filter)

    with_udf_a = relational_ir.mapping_ir(
        mapping('SELECT "id", score("amount") AS "score" FROM arrow_batch'),
        udfs=[dict(
            name="score", source_sha256="aaa", function="score",
            parameters=["DOUBLE"], return_type="DOUBLE")])
    with_udf_b = relational_ir.mapping_ir(
        mapping('SELECT "id", score("amount") AS "score" FROM arrow_batch'),
        udfs=[dict(
            name="score", source_sha256="bbb", function="score",
            parameters=["DOUBLE"], return_type="DOUBLE")])
    assert relational_ir.semantic_id(with_udf_a) != relational_ir.semantic_id(
        with_udf_b)

    with_macro_a = relational_ir.mapping_ir(
        mapping('SELECT "id", m("amount") AS "v" FROM arrow_batch'),
        macros=["CREATE MACRO m(x) AS x * 2"])
    with_macro_b = relational_ir.mapping_ir(
        mapping('SELECT "id", m("amount") AS "v" FROM arrow_batch'),
        macros=["CREATE MACRO m(x) AS x * 3"])
    assert relational_ir.semantic_id(with_macro_a) != relational_ir.semantic_id(
        with_macro_b)

    raw = pa.Table.from_pylist([
        dict(id=1, amount=Decimal("2.50"), status="paid"),
        dict(id=2, amount=None, status="paid"),
        dict(id=3, amount=Decimal("-1.00"), status="paid"),
        dict(id=4, amount=Decimal("4.00"), status="open"),
        dict(id=5, amount=Decimal("2.50"), status="paid"),
    ], schema=pa.schema([
        pa.field("id", pa.int64()),
        pa.field("amount", pa.decimal128(18,2)),
        pa.field("status", pa.string()),
    ]))
    con = duckdb.connect(":memory:")
    con.register("_sync_raw", raw)
    current_sql = (
        'WITH arrow_batch AS (SELECT * FROM _sync_raw '
        'WHERE "amount" > 0) '
        'SELECT "id", "amount" * 2 AS "double_amount" '
        'FROM arrow_batch WHERE "status" = \'paid\''
    )
    expected = con.execute(current_sql).fetchall()
    actual = con.execute(relational_ir.to_duckdb_sql(a)).fetchall()
    assert Counter(expected) == Counter(actual)
    assert len(actual) == 3
    assert all(row[1] == Decimal("5.00") for row in actual)
    con.close()

    broken = dict(a)
    broken["extra"] = 1
    expect_error(lambda: relational_ir.semantic_id(broken))
    expect_error(lambda: relational_ir.mapping_ir({"src_table": "orders"}))

    print("relational_ir_test ok", flush=True)


if __name__ == "__main__":
    main()
