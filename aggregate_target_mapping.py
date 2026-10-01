#!/usr/bin/env python3
"""Identity writer mapping for aggregate materialized results.

Aggregate rows are already the final target relation. They do not originate
from a MySQL table, so they must not be forced through source-table preflight.
This adapter builds the minimal validated j4 mapping needed by routing,
durable jobs, winner selection and JSON Stream Load.
"""
import pyarrow as pa

import j4


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def build(sink_key,sr_table,schema,primary_key):
    sink_key=_text(sink_key,"sink_key")
    sr_table=_text(sr_table,"sr_table")
    if not isinstance(schema,pa.Schema):
        schema=pa.schema(schema)
    names=list(schema.names)
    if not names or len(names)!=len(set(names)):
        raise ValueError("aggregate target schema must have unique columns")
    if any(name.startswith("_sync_") or name=="__op" for name in names):
        raise ValueError("aggregate target schema uses a reserved column")
    keys=[primary_key] if isinstance(primary_key,str) else list(primary_key or ())
    keys=[_text(value,"primary_key") for value in keys]
    if not keys or len(keys)!=len(set(keys)):
        raise ValueError("aggregate target primary key must be unique and non-empty")
    missing=[name for name in keys if name not in names]
    if missing:
        raise ValueError(
            "aggregate target primary key is absent from schema: "
            +",".join(missing))
    fields=[(field.name,field.type) for field in schema]
    binary={
        field.name for field in schema
        if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type)
    }
    mapping=dict(
        src_table=sink_key,
        sr_table=sr_table,
        primary_key=keys[0] if len(keys)==1 else keys,
        sql="SELECT "+",".join(j4.sql_name(name) for name in names)
            +" FROM arrow_batch",
        _catalog_sink=sink_key,
        _schema=fields,
        _source_name_set=frozenset(names),
        _source_index={name:index for index,name in enumerate(names)},
        _json_columns=set(),
        _schema_signature=[
            (name,str(dtype),str(dtype),None,None,True)
            for name,dtype in fields
        ],
        _source_table_comment="",
        _source_column_comments={name:"" for name in names},
        _filter_sql="",
        _sql="SELECT "+",".join(j4.sql_name(name) for name in names)
            +" FROM arrow_batch",
        _direct_output_sources={name:name for name in names},
        _arrow_columns=list(names),
        _delta_sql=(
            "SELECT "+",".join(j4.sql_name(name) for name in names)
            +', "_sync_op", "_sync_order" FROM arrow_batch'),
        _binary_output_columns=binary,
        _target_json_columns=set(),
        _size_columns={},
        _auto_size_columns={},
        _output_columns=list(names),
        _target_constraints={},
        _target_sequence=False,
        _target_missing=False,
    )
    # Reuse ordinary projection validation so the candidate obeys the same
    # reserved-name, PK projection and determinism contracts as normal sinks.
    j4.validate_mapping(mapping)
    return mapping


def bind_target(mapping,target_columns,target_sequence=False):
    target_columns=dict(target_columns or {})
    missing=set(mapping["_output_columns"])-set(target_columns)
    if missing:
        raise ValueError(
            "aggregate target is missing columns: "+",".join(sorted(missing)))
    mapping=dict(mapping)
    mapping["_target_sequence"]=bool(target_sequence)
    mapping["_target_json_columns"]={
        name for name in mapping["_output_columns"]
        if str(target_columns[name][1]).lower().startswith("json")
    }
    mapping["_target_constraints"]=j4.compile_target_constraints(
        mapping,target_columns)
    mapping["_target_schema"]={
        name:dict(
            type=str(row[1]),
            nullable=str(row[2]).upper()=="YES",
            key=str(row[3] or ""),
            default=row[4] if len(row)>4 else None,
            extra=str(row[5] or "") if len(row)>5 else "",
        )
        for name,row in target_columns.items()
    }
    return mapping
