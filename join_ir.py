#!/usr/bin/env python3
"""Canonical IR for the first correctness-first INNER JOIN subset.

This module is deliberately not exposed through cdc_catalog yet. It accepts
exactly two durable source relations, stable source primary keys, one INNER
equi-join over one or more qualified plain columns, and qualified plain-column
projections. The narrow surface gives the incremental state implementation an
unambiguous pair identity and SQL NULL/bag semantics before optimization.
"""
import hashlib
import json

import sqlglot
from sqlglot import exp


FORMAT_VERSION=1


def canonical_bytes(value):
    return json.dumps(
        value,ensure_ascii=False,sort_keys=True,
        separators=(",",":")).encode("utf-8")


def semantic_id(ir):
    validate_ir(ir)
    return hashlib.sha256(canonical_bytes(ir)).hexdigest()


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _schema(schema_signature,name):
    result=[]
    seen=set()
    for item in schema_signature or ():
        if not isinstance(item,(list,tuple)) or not item:
            raise ValueError(name+" schema signature contains an invalid column")
        column=_text(item[0],name+" column")
        if column in seen:
            raise ValueError(name+" schema contains duplicate column "+column)
        seen.add(column)
        result.append([str(value) for value in item])
    if not result:
        raise ValueError(name+" schema cannot be empty")
    return result


def _columns(schema):
    return [str(item[0]) for item in schema]


def _primary_key(value,columns,name):
    keys=[value] if isinstance(value,str) else list(value or ())
    keys=[_text(item,name+" primary key") for item in keys]
    if not keys or len(keys)!=len(set(keys)):
        raise ValueError(name+" primary key must be unique and non-empty")
    missing=[item for item in keys if item not in columns]
    if missing:
        raise ValueError(
            name+" primary key is absent from schema: "+",".join(missing))
    return keys


def _and_terms(node):
    if isinstance(node,exp.And):
        return _and_terms(node.this)+_and_terms(node.expression)
    return [node]


def _side_for_column(column,aliases):
    if not isinstance(column,exp.Column):
        raise ValueError("INNER JOIN v1 requires plain-column expressions")
    qualifier=str(column.table or "").strip()
    if not qualifier:
        raise ValueError(
            "INNER JOIN v1 requires every projected/join column to be qualified")
    side=aliases.get(qualifier.lower())
    if side is None:
        raise ValueError(
            "INNER JOIN column references an unknown source alias: "+qualifier)
    return side,str(column.name)


def _source(
        relation,schema_signature,primary_key,name
):
    relation=_text(relation,name+" relation")
    schema=_schema(schema_signature,name)
    columns=_columns(schema)
    return dict(
        relation=relation,
        schema=schema,
        primary_key=_primary_key(primary_key,columns,name),
    )


def compile_sql(
        left_relation,left_schema,left_primary_key,
        right_relation,right_schema,right_primary_key,sql
):
    left=_source(
        left_relation,left_schema,left_primary_key,"left")
    right=_source(
        right_relation,right_schema,right_primary_key,"right")
    if left["relation"]==right["relation"]:
        raise ValueError(
            "INNER JOIN v1 requires two distinct durable source relations")

    try:
        tree=sqlglot.parse_one(str(sql or ""),read="duckdb")
    except Exception as exc:
        raise ValueError("invalid INNER JOIN SQL: "+str(exc)) from exc
    if not isinstance(tree,exp.Select):
        raise ValueError("INNER JOIN v1 requires one SELECT")

    forbidden=(
        exp.Subquery,exp.Union,exp.AggFunc,exp.Window,exp.Group,
        exp.Having,exp.Limit,exp.Offset,exp.Distinct,exp.With,
        exp.Unnest,exp.Explode,
    )
    if any(isinstance(node,forbidden) for node in tree.walk()):
        raise ValueError(
            "INNER JOIN v1 supports no subquery/aggregate/window/set operation")
    if (
        tree.args.get("where") is not None
        or tree.args.get("order") is not None
        or tree.args.get("qualify") is not None
    ):
        raise ValueError(
            "INNER JOIN v1 does not yet support WHERE/ORDER BY/QUALIFY")

    tables=list(tree.find_all(exp.Table))
    joins=list(tree.find_all(exp.Join))
    if len(tables)!=2 or len(joins)!=1:
        raise ValueError(
            "INNER JOIN v1 requires exactly two tables and one JOIN")
    by_name={str(table.name).lower():table for table in tables}
    if set(by_name)!={"left_batch","right_batch"}:
        raise ValueError(
            "INNER JOIN v1 sources must be left_batch and right_batch")

    aliases={}
    for name,table in by_name.items():
        alias=str(table.alias_or_name or table.name).strip().lower()
        if not alias or alias in aliases:
            raise ValueError("INNER JOIN source aliases must be unique")
        aliases[alias]="left" if name=="left_batch" else "right"

    join=joins[0]
    side=str(join.args.get("side") or "").strip().upper()
    kind=str(join.args.get("kind") or "").strip().upper()
    if side or kind not in {"","INNER"}:
        raise ValueError("INNER JOIN v1 supports INNER JOIN only")
    on=join.args.get("on")
    if on is None:
        raise ValueError("INNER JOIN v1 requires an ON predicate")

    left_columns=set(_columns(left["schema"]))
    right_columns=set(_columns(right["schema"]))
    pairs=[]
    seen_left=set()
    seen_right=set()
    for term in _and_terms(on):
        if not isinstance(term,exp.EQ):
            raise ValueError(
                "INNER JOIN v1 ON supports equality conjunctions only")
        first_side,first_name=_side_for_column(term.this,aliases)
        second_side,second_name=_side_for_column(term.expression,aliases)
        if first_side==second_side:
            raise ValueError(
                "INNER JOIN equality must compare left and right sources")
        if first_side=="left":
            left_name,right_name=first_name,second_name
        else:
            left_name,right_name=second_name,first_name
        if left_name not in left_columns:
            raise ValueError("unknown left join column: "+left_name)
        if right_name not in right_columns:
            raise ValueError("unknown right join column: "+right_name)
        if left_name in seen_left or right_name in seen_right:
            raise ValueError(
                "INNER JOIN v1 join keys cannot repeat a source column")
        seen_left.add(left_name)
        seen_right.add(right_name)
        pairs.append(dict(left=left_name,right=right_name))
    if not pairs:
        raise ValueError("INNER JOIN v1 requires at least one equality")
    pairs.sort(key=lambda item:(item["left"],item["right"]))

    projections=[]
    outputs=set()
    for item in tree.expressions:
        target=item.this if isinstance(item,exp.Alias) else item
        if not isinstance(target,exp.Column):
            raise ValueError(
                "INNER JOIN v1 projections must be qualified plain columns")
        source,name=_side_for_column(target,aliases)
        columns=left_columns if source=="left" else right_columns
        if name not in columns:
            raise ValueError(
                "unknown "+source+" projection column: "+name)
        output=str(item.alias_or_name or "").strip()
        if not output:
            raise ValueError("INNER JOIN projection requires an output name")
        if output.startswith("_sync_"):
            raise ValueError("_sync_* names are reserved")
        if output in outputs:
            raise ValueError("duplicate INNER JOIN output column: "+output)
        outputs.add(output)
        projections.append(
            dict(output=output,source=source,column=name))
    if not projections:
        raise ValueError("INNER JOIN v1 projection cannot be empty")

    ir=dict(
        format_version=FORMAT_VERSION,
        kind="inner_join",
        sources=dict(left=left,right=right),
        join_pairs=pairs,
        projections=projections,
        semantics=dict(
            bag=True,
            nulls="sql",
            retract="source_pk_pair_identity",
        ),
    )
    validate_ir(ir)
    return ir


def validate_ir(ir):
    if not isinstance(ir,dict):
        raise ValueError("INNER JOIN IR must be a dict")
    if set(ir)!={
        "format_version","kind","sources","join_pairs",
        "projections","semantics",
    }:
        raise ValueError("INNER JOIN IR fields differ from format v1")
    if int(ir["format_version"])!=FORMAT_VERSION:
        raise ValueError("unsupported INNER JOIN IR format")
    if ir["kind"]!="inner_join":
        raise ValueError("unsupported JOIN IR kind")
    sources=ir["sources"]
    if not isinstance(sources,dict) or set(sources)!={"left","right"}:
        raise ValueError("INNER JOIN IR must contain left/right sources")

    columns={}
    for side in ("left","right"):
        source=sources[side]
        if not isinstance(source,dict) or set(source)!={
            "relation","schema","primary_key"
        }:
            raise ValueError("invalid "+side+" INNER JOIN source")
        source=_source(
            source["relation"],source["schema"],
            source["primary_key"],side)
        columns[side]=set(_columns(source["schema"]))
    if sources["left"]["relation"]==sources["right"]["relation"]:
        raise ValueError("INNER JOIN sources must be distinct relations")

    pairs=ir["join_pairs"]
    if not isinstance(pairs,list) or not pairs:
        raise ValueError("INNER JOIN IR requires join pairs")
    normalized=[]
    used_left=set()
    used_right=set()
    for item in pairs:
        if not isinstance(item,dict) or set(item)!={"left","right"}:
            raise ValueError("invalid INNER JOIN key pair")
        left_name=_text(item["left"],"left join key")
        right_name=_text(item["right"],"right join key")
        if left_name not in columns["left"] or right_name not in columns["right"]:
            raise ValueError("INNER JOIN key is absent from source schema")
        if left_name in used_left or right_name in used_right:
            raise ValueError("INNER JOIN key repeats a source column")
        used_left.add(left_name)
        used_right.add(right_name)
        normalized.append(dict(left=left_name,right=right_name))
    if pairs!=sorted(normalized,key=lambda item:(item["left"],item["right"])):
        raise ValueError("INNER JOIN key pairs are not canonical")

    projections=ir["projections"]
    if not isinstance(projections,list) or not projections:
        raise ValueError("INNER JOIN IR requires projections")
    outputs=set()
    for item in projections:
        if not isinstance(item,dict) or set(item)!={
            "output","source","column"
        }:
            raise ValueError("invalid INNER JOIN projection")
        output=_text(item["output"],"INNER JOIN output")
        source=str(item["source"])
        column=_text(item["column"],"INNER JOIN projection column")
        if source not in {"left","right"} or column not in columns[source]:
            raise ValueError("INNER JOIN projection source/column is invalid")
        if output.startswith("_sync_") or output in outputs:
            raise ValueError("INNER JOIN output name is reserved/duplicate")
        outputs.add(output)

    if ir["semantics"]!=dict(
        bag=True,nulls="sql",
        retract="source_pk_pair_identity",
    ):
        raise ValueError("unsupported INNER JOIN IR semantics")
    return ir


def state_spec(ir):
    validate_ir(ir)
    return dict(
        sources={
            side:dict(
                relation=ir["sources"][side]["relation"],
                primary_key=list(ir["sources"][side]["primary_key"]),
                join_key=[
                    item[side] for item in ir["join_pairs"]
                ],
            )
            for side in ("left","right")
        },
        projections=list(ir["projections"]),
        semantics=dict(ir["semantics"]),
    )


def output_columns(ir):
    validate_ir(ir)
    return [item["output"] for item in ir["projections"]]


def input_columns(ir,side):
    validate_ir(ir)
    side=str(side)
    if side not in {"left","right"}:
        raise ValueError("INNER JOIN side must be left or right")
    result=[]
    source=ir["sources"][side]
    for name in source["primary_key"]:
        if name not in result:
            result.append(name)
    for item in ir["join_pairs"]:
        name=item[side]
        if name not in result:
            result.append(name)
    for item in ir["projections"]:
        if item["source"]==side and item["column"] not in result:
            result.append(item["column"])
    return result


def _sql_name(name):
    return '"' + str(name).replace('"','""') + '"'


def to_duckdb_sql(ir,left_name="_left",right_name="_right"):
    validate_ir(ir)
    left_name=_text(left_name,"left input relation")
    right_name=_text(right_name,"right input relation")
    for value in (left_name,right_name):
        if not value.replace("_","").isalnum():
            raise ValueError("unsafe INNER JOIN input relation name")
    projections=[]
    for item in ir["projections"]:
        alias="l" if item["source"]=="left" else "r"
        projections.append(
            alias+"."+_sql_name(item["column"])
            +" AS "+_sql_name(item["output"]))
    predicates=[
        "l."+_sql_name(item["left"])
        +" = r."+_sql_name(item["right"])
        for item in ir["join_pairs"]
    ]
    return (
        "SELECT "+", ".join(projections)
        +" FROM "+left_name+" AS l INNER JOIN "
        +right_name+" AS r ON "+" AND ".join(predicates)
    )
