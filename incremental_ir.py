#!/usr/bin/env python3
"""Incremental IR v1 for cardinality-preserving stateless relations.

The current source change model is keyed delete/upsert. UPDATE requires a before
image and is represented as delete(old) + upsert(new). Filters and projections
therefore propagate retracts without operator state.
"""
import hashlib
import json

import sqlglot
from sqlglot import exp

import relational_ir


DELTA_FORMAT_VERSION = 1


def canonical_bytes(value):
    return json.dumps(
        value,ensure_ascii=False,sort_keys=True,
        separators=(",",":")).encode("utf-8")


def semantic_id(ir):
    validate_ir(ir)
    return hashlib.sha256(canonical_bytes(ir)).hexdigest()


def _key_projected_unchanged(rel, key):
    for item in rel["projection"]:
        if item["output"] != key:
            continue
        tree = sqlglot.parse_one(
            "SELECT " + item["expression"],read="duckdb")
        expression = tree.expressions[0]
        if isinstance(expression,exp.Alias):
            expression = expression.this
        return isinstance(expression,exp.Column) and expression.name == key
    return False


def compile_ir(rel):
    relational_ir.validate_ir(rel)
    for key in rel["primary_key"]:
        if not _key_projected_unchanged(rel,key):
            raise ValueError(
                "incremental IR v1 requires primary keys projected unchanged")
    operators = []
    if rel["source_filter"]:
        operators.append(dict(
            op="filter",scope="source",
            expression=rel["source_filter"]))
    if rel["query_filter"]:
        operators.append(dict(
            op="filter",scope="query",
            expression=rel["query_filter"]))
    operators.append(dict(
        op="project",
        expressions=[dict(item) for item in rel["projection"]]))
    result = dict(
        format_version=DELTA_FORMAT_VERSION,
        kind="stateless_delta",
        relation_semantic_id=relational_ir.semantic_id(rel),
        primary_key=list(rel["primary_key"]),
        operators=operators,
        input_changes=dict(
            encoding="keyed_delete_upsert",
            delete_op=1,
            upsert_op=0,
            update_requires_before_image=True,
        ),
        output_changes=dict(
            encoding="keyed_delete_upsert",
            delete_op=1,
            upsert_op=0,
            metadata=["_sync_op","_sync_order"],
        ),
    )
    validate_ir(result)
    return result


def validate_ir(ir):
    if not isinstance(ir,dict):
        raise ValueError("incremental IR must be a dict")
    expected = {
        "format_version","kind","relation_semantic_id","primary_key",
        "operators","input_changes","output_changes",
    }
    if set(ir) != expected:
        raise ValueError("incremental IR fields differ from format v1")
    if int(ir["format_version"]) != DELTA_FORMAT_VERSION:
        raise ValueError("unsupported incremental IR format version")
    if ir["kind"] != "stateless_delta":
        raise ValueError("unsupported incremental IR kind")
    identity = str(ir["relation_semantic_id"])
    if len(identity) != 64 or any(
        char not in "0123456789abcdef" for char in identity):
        raise ValueError("invalid relation semantic id")
    if not ir["primary_key"]:
        raise ValueError("incremental IR primary key cannot be empty")
    if not isinstance(ir["operators"],list) or not ir["operators"]:
        raise ValueError("incremental IR operators cannot be empty")
    if ir["operators"][-1].get("op") != "project":
        raise ValueError("incremental IR v1 must end with project")
    input_changes = ir["input_changes"]
    if input_changes != dict(
        encoding="keyed_delete_upsert",
        delete_op=1,upsert_op=0,
        update_requires_before_image=True,
    ):
        raise ValueError("unsupported input change semantics")
    output_changes = ir["output_changes"]
    if output_changes != dict(
        encoding="keyed_delete_upsert",
        delete_op=1,upsert_op=0,
        metadata=["_sync_op","_sync_order"],
    ):
        raise ValueError("unsupported output change semantics")
    return ir


def to_duckdb_sql(delta_ir, rel, input_name="_sync_raw"):
    validate_ir(delta_ir)
    relational_ir.validate_ir(rel)
    if delta_ir["relation_semantic_id"] != relational_ir.semantic_id(rel):
        raise ValueError("incremental IR does not match relational IR")
    input_name = str(input_name or "").strip()
    if not input_name.replace("_","").isalnum():
        raise ValueError("unsafe DuckDB input relation name")
    cte = (
        "WITH arrow_batch AS (SELECT * FROM " + input_name
        + (
            " WHERE " + rel["source_filter"]
            if rel["source_filter"] else ""
        )
        + ") "
    )
    projections = ", ".join(
        item["expression"] for item in rel["projection"])
    return (
        cte + "SELECT " + projections
        + ', "_sync_op", "_sync_order" FROM arrow_batch'
        + (
            " WHERE " + rel["query_filter"]
            if rel["query_filter"] else ""
        )
    )
