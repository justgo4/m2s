#!/usr/bin/env python3
from pathlib import Path
import sys

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

    broken = dict(a)
    broken["extra"] = 1
    expect_error(lambda: relational_ir.semantic_id(broken))
    expect_error(lambda: relational_ir.mapping_ir({"src_table": "orders"}))

    print("relational_ir_test ok", flush=True)


if __name__ == "__main__":
    main()
