#!/usr/bin/env python3
"""Canonical relational IR for the currently supported stateless SQL subset.

Input SQL must already have passed j4.validate_mapping(). This module does not
expand the supported SQL surface; it gives later incremental planners a stable,
backend-neutral semantic representation.
"""
import hashlib
import json

import sqlglot
from sqlglot import exp


IR_FORMAT_VERSION = 1


def canonical_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")


def semantic_id(ir):
    validate_ir(ir)
    return hashlib.sha256(canonical_bytes(ir)).hexdigest()


def _normalized_expr(value, dialect="duckdb"):
    value = str(value or "").strip()
    if not value:
        return ""
    return sqlglot.parse_one(value, read=dialect).sql(dialect="duckdb")


def dependency_spec(macros=(), udfs=()):
    macro_items = [str(item).strip() for item in (macros or ())]
    udf_items = []
    for item in udfs or ():
        if not isinstance(item, dict):
            raise ValueError("UDF dependency must be a dict")
        name = str(item.get("name") or "").strip()
        source_sha = str(item.get("source_sha256") or "").strip()
        if not name or not source_sha:
            raise ValueError("UDF dependency needs name and source_sha256")
        udf_items.append(dict(
            name=name,
            source_sha256=source_sha,
            function=str(item.get("function") or ""),
            parameters=[str(value) for value in item.get("parameters", ())],
            return_type=str(item.get("return_type") or ""),
        ))
    return dict(macros=macro_items, udfs=udf_items)


def mapping_ir(mapping, macros=(), udfs=()):
    if not isinstance(mapping, dict):
        raise ValueError("validated mapping must be a dict")
    for name in ("src_table", "_sql", "_filter_sql", "_schema_signature"):
        if name not in mapping:
            raise ValueError("validated mapping lacks " + name)
    keys = mapping.get("primary_key")
    keys = [keys] if isinstance(keys, str) else list(keys or ())
    if not keys:
        raise ValueError("validated mapping lacks primary key")

    tree = sqlglot.parse_one(str(mapping["_sql"]), read="duckdb")
    if not isinstance(tree, exp.Select):
        raise ValueError("relational IR v1 requires one validated SELECT")
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1 or tables[0].name.lower() != "arrow_batch":
        raise ValueError("relational IR v1 requires one arrow_batch source")

    projections = [
        dict(
            output=str(item.alias_or_name),
            expression=item.sql(dialect="duckdb"),
        )
        for item in tree.expressions
    ]
    where = tree.args.get("where")
    query_filter = (
        where.this.sql(dialect="duckdb")
        if where is not None else ""
    )
    source_filter = _normalized_expr(
        mapping.get("_filter_sql") or "", dialect="duckdb")

    schema = [
        [str(value) for value in item]
        for item in mapping["_schema_signature"]
    ]

    ir = dict(
        format_version=IR_FORMAT_VERSION,
        kind="stateless_relation",
        source=dict(
            relation=str(mapping["src_table"]),
            schema=schema,
        ),
        primary_key=[str(value) for value in keys],
        source_filter=source_filter,
        query_filter=query_filter,
        projection=projections,
        dependencies=dependency_spec(macros, udfs),
        semantics=dict(
            bag=True,
            nulls="sql",
            row_order="not_part_of_relation",
        ),
    )
    validate_ir(ir)
    return ir


def validate_ir(ir):
    if not isinstance(ir, dict):
        raise ValueError("relational IR must be a dict")
    expected = {
        "format_version", "kind", "source", "primary_key",
        "source_filter", "query_filter", "projection",
        "dependencies", "semantics",
    }
    if set(ir) != expected:
        raise ValueError("relational IR fields differ from format v1")
    if int(ir["format_version"]) != IR_FORMAT_VERSION:
        raise ValueError("unsupported relational IR format version")
    if ir["kind"] != "stateless_relation":
        raise ValueError("unsupported relational IR kind")
    source = ir["source"]
    if (
        not isinstance(source, dict)
        or set(source) != {"relation", "schema"}
        or not str(source["relation"])
        or not isinstance(source["schema"], list)
    ):
        raise ValueError("invalid relational IR source")
    if not ir["primary_key"] or not all(
        isinstance(value, str) and value for value in ir["primary_key"]
    ):
        raise ValueError("invalid relational IR primary key")
    if not isinstance(ir["projection"], list) or not ir["projection"]:
        raise ValueError("relational IR projection cannot be empty")
    outputs = []
    for item in ir["projection"]:
        if not isinstance(item, dict) or set(item) != {"output", "expression"}:
            raise ValueError("invalid relational IR projection")
        if not item["output"] or not item["expression"]:
            raise ValueError("empty relational IR projection")
        outputs.append(item["output"])
    if len(outputs) != len(set(outputs)):
        raise ValueError("duplicate relational IR output")
    deps = ir["dependencies"]
    if not isinstance(deps, dict) or set(deps) != {"macros", "udfs"}:
        raise ValueError("invalid relational IR dependencies")
    if ir["semantics"] != dict(
        bag=True, nulls="sql", row_order="not_part_of_relation"
    ):
        raise ValueError("unsupported relational IR semantics")
    return ir
