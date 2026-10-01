#!/usr/bin/env python3
"""Canonical IR for the first stateful GROUP BY subset.

This compiler is deliberately not wired into cdc_catalog yet. It accepts one
arrow_batch source, plain-column grouping, deterministic row filters, and
COUNT/SUM/AVG over '*' or one plain input column as appropriate.
"""
import hashlib
import json
import re

import sqlglot
from sqlglot import exp

import aggregate_state


FORMAT_VERSION = 1


def canonical_bytes(value):
    return json.dumps(
        value,ensure_ascii=False,sort_keys=True,
        separators=(",",":")).encode("utf-8")


def semantic_id(ir):
    validate_ir(ir)
    return hashlib.sha256(canonical_bytes(ir)).hexdigest()


def _schema_columns(schema_signature):
    result=[]
    for item in schema_signature or ():
        if not isinstance(item,(list,tuple)) or not item:
            raise ValueError("invalid aggregate source schema signature")
        name=str(item[0])
        if not name or name in result:
            raise ValueError("invalid/duplicate aggregate source column")
        result.append(name)
    if not result:
        raise ValueError("aggregate source schema cannot be empty")
    return result


def _normalized_filter(value, dialect):
    value=str(value or "").strip()
    if not value:
        return ""
    tree=sqlglot.parse_one(value,read=dialect)
    if any(isinstance(node,(exp.Select,exp.Subquery,exp.AggFunc,exp.Window))
           for node in tree.walk()):
        raise ValueError("aggregate row filter cannot contain stateful/subquery SQL")
    sql=tree.sql(dialect="duckdb")
    if re.search(
        r"\b(random|rand|uuid|now|current_timestamp|current_date|"
        r"current_time|read_\w+|query|generate_series)\b",
        sql,re.I,
    ):
        raise ValueError("aggregate row filter must be deterministic")
    return sql


def _aggregate_projection(node, columns):
    output=str(node.alias_or_name or "").strip()
    target=node.this if isinstance(node,exp.Alias) else node
    if isinstance(target,exp.Column):
        if isinstance(node,exp.Alias):
            raise ValueError("GROUP BY columns cannot be renamed in aggregate IR v1")
        name=target.name
        if name not in columns:
            raise ValueError("unknown GROUP BY projection column: "+name)
        return dict(kind="group",name=name)

    function=None
    if isinstance(target,exp.Count):
        function="count"
    elif isinstance(target,exp.Sum):
        function="sum"
    elif isinstance(target,exp.Avg):
        function="avg"
    if function is None:
        raise ValueError(
            "aggregate IR v1 supports only GROUP BY columns and COUNT/SUM/AVG")
    if not isinstance(node,exp.Alias) or not output:
        raise ValueError("aggregate expressions require an explicit output alias")
    if isinstance(target.this,exp.Distinct):
        raise ValueError("DISTINCT aggregates are not supported")
    if isinstance(target.this,exp.Star):
        input_name="*"
        if function != "count":
            raise ValueError(function+"(*) is unsupported")
    elif isinstance(target.this,exp.Column):
        input_name=target.this.name
        if input_name not in columns:
            raise ValueError("unknown aggregate input column: "+input_name)
    else:
        raise ValueError(
            "aggregate IR v1 requires '*' or one plain input column")
    return dict(
        kind="aggregate",output=output,
        function=function,input=input_name)


def compile_sql(
        source_relation, schema_signature, sql, source_filter=""
):
    source_relation=str(source_relation or "").strip()
    if not source_relation:
        raise ValueError("source_relation is required")
    columns=_schema_columns(schema_signature)
    tree=sqlglot.parse_one(str(sql or ""),read="duckdb")
    if not isinstance(tree,exp.Select):
        raise ValueError("aggregate IR v1 requires one SELECT")
    tables=list(tree.find_all(exp.Table))
    if len(tables)!=1 or tables[0].name.lower()!="arrow_batch":
        raise ValueError("aggregate IR v1 requires one arrow_batch source")
    forbidden=(exp.Join,exp.Union,exp.Window,exp.Having,exp.Limit,
               exp.Offset,exp.Distinct,exp.With,exp.Unnest,exp.Explode)
    if any(isinstance(node,forbidden) for node in tree.walk()):
        raise ValueError("unsupported aggregate IR v1 SQL construct")
    if tree.args.get("order") is not None:
        raise ValueError("ORDER BY is not part of aggregate IR v1")
    group=tree.args.get("group")
    if group is None or not group.expressions:
        raise ValueError("aggregate IR v1 requires GROUP BY")

    group_keys=[]
    for expression in group.expressions:
        if not isinstance(expression,exp.Column):
            raise ValueError("GROUP BY expressions must be plain columns")
        name=expression.name
        if name not in columns:
            raise ValueError("unknown GROUP BY column: "+name)
        if name in group_keys:
            raise ValueError("duplicate GROUP BY column")
        group_keys.append(name)

    projections=[
        _aggregate_projection(node,columns)
        for node in tree.expressions
    ]
    projected_groups=[
        item["name"] for item in projections if item["kind"]=="group"
    ]
    if projected_groups != group_keys:
        raise ValueError(
            "GROUP BY columns must be projected once, unchanged, in GROUP BY order")
    aggregates=[
        dict(
            output=item["output"],
            function=item["function"],
            input=item["input"])
        for item in projections if item["kind"]=="aggregate"
    ]
    if not aggregates:
        raise ValueError("aggregate query must contain COUNT/SUM/AVG")
    aggregate_state.aggregate_spec(group_keys,aggregates)

    where=tree.args.get("where")
    query_filter=(
        "" if where is None
        else _normalized_filter(where.this.sql(dialect="duckdb"),"duckdb")
    )
    # Match j4.validate_mapping(): full_filter is parsed as MySQL once
    # during preflight, then persisted as normalized DuckDB _filter_sql.
    source_filter=_normalized_filter(source_filter,"duckdb")
    ir=dict(
        format_version=FORMAT_VERSION,
        kind="group_aggregate",
        source=dict(
            relation=source_relation,
            schema=[[str(value) for value in item] for item in schema_signature],
        ),
        source_filter=source_filter,
        query_filter=query_filter,
        group_keys=group_keys,
        aggregates=aggregates,
        semantics=dict(
            bag=True,
            nulls="sql",
            retract="before_image_delete_upsert",
            avg_output="double",
        ),
    )
    validate_ir(ir)
    return ir


def validate_ir(ir):
    if not isinstance(ir,dict):
        raise ValueError("aggregate IR must be a dict")
    expected={
        "format_version","kind","source","source_filter","query_filter",
        "group_keys","aggregates","semantics",
    }
    if set(ir)!=expected:
        raise ValueError("aggregate IR fields differ from format v1")
    if int(ir["format_version"])!=FORMAT_VERSION:
        raise ValueError("unsupported aggregate IR format")
    if ir["kind"]!="group_aggregate":
        raise ValueError("unsupported aggregate IR kind")
    source=ir["source"]
    if (
        not isinstance(source,dict)
        or set(source)!={"relation","schema"}
        or not str(source["relation"])
    ):
        raise ValueError("invalid aggregate IR source")
    columns=_schema_columns(source["schema"])
    if (
        not isinstance(ir["group_keys"],list)
        or not ir["group_keys"]
        or any(name not in columns for name in ir["group_keys"])
    ):
        raise ValueError("invalid aggregate IR group keys")
    aggregate_state.aggregate_spec(ir["group_keys"],ir["aggregates"])
    if not isinstance(ir["source_filter"],str) or not isinstance(
        ir["query_filter"],str
    ):
        raise ValueError("aggregate filters must be strings")
    if ir["semantics"]!=dict(
        bag=True,nulls="sql",
        retract="before_image_delete_upsert",
        avg_output="double",
    ):
        raise ValueError("unsupported aggregate IR semantics")
    return ir


def state_spec(ir):
    validate_ir(ir)
    return aggregate_state.aggregate_spec(
        ir["group_keys"],ir["aggregates"])


def input_columns(ir):
    validate_ir(ir)
    result=list(ir["group_keys"])
    for item in ir["aggregates"]:
        name=item["input"]
        if name!="*" and name not in result:
            result.append(name)
    return result


def to_duckdb_input_sql(ir, input_name="_sync_raw"):
    validate_ir(ir)
    input_name=str(input_name or "").strip()
    if not input_name.replace("_","").isalnum():
        raise ValueError("unsafe aggregate input relation name")
    columns=input_columns(ir)+["_sync_op","_sync_order"]
    predicates=[
        value for value in (ir["source_filter"],ir["query_filter"]) if value
    ]
    return (
        "SELECT "
        +", ".join('"' + name.replace('"','""') + '"' for name in columns)
        +" FROM "+input_name
        +(" WHERE "+" AND ".join("("+value+")" for value in predicates)
          if predicates else "")
    )
