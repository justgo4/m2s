#!/usr/bin/env python3
"""Writer mapping and schema contract for aggregate materialized results.

Aggregate rows are already the final target relation, so they bypass MySQL
source preflight. The durable task descriptor stores the normalized StarRocks
target contract. Every restart re-reads the real target schema, verifies that
contract, then reconstructs the same identity writer mapping used by j4's
existing durable jobs/Stream Load path.
"""
import re

import pyarrow as pa

import aggregate_ir
import aggregate_task_catalog


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _type_base(type_sql):
    return str(type_sql or "").strip().upper()


def target_arrow_type(type_sql):
    value=_type_base(type_sql)
    if value=="BOOLEAN":
        return pa.bool_()
    if value=="TINYINT":
        return pa.int8()
    if value=="SMALLINT":
        return pa.int16()
    if value=="INT":
        return pa.int32()
    if value=="BIGINT":
        return pa.int64()
    if value=="LARGEINT":
        return pa.decimal128(38,0)
    if value=="FLOAT":
        return pa.float32()
    if value in {"DOUBLE","DOUBLE PRECISION"}:
        return pa.float64()
    decimal_match=re.match(
        r"^DECIMAL(?:V\d+)?\s*\((\d+)\s*,\s*(\d+)\)$",
        value)
    if decimal_match:
        precision,scale=map(int,decimal_match.groups())
        if precision>38:
            raise ValueError(
                "aggregate target decimal precision exceeds Arrow Decimal128")
        return pa.decimal128(precision,scale)
    if value=="DATE":
        return pa.date32()
    if value.startswith("DATETIME"):
        return pa.timestamp("us")
    if value.startswith(("CHAR(","VARCHAR(")) or value=="STRING":
        return pa.large_string()
    raise ValueError(
        "unsupported aggregate target type for JSON writer: "+value)


def _source_signature(ir,name):
    for item in ir["source"]["schema"]:
        if str(item[0])==str(name):
            return item
    raise ValueError("aggregate source schema lacks column "+str(name))


def _decimal_shape(column_type):
    match=re.search(r"\((\d+)\s*,\s*(\d+)\)",str(column_type))
    return tuple(map(int,match.groups())) if match else None


def validate_semantic_target(ir,target_schema):
    aggregate_ir.validate_ir(ir)
    schema=aggregate_task_catalog.normalize_target_schema(
        ir,target_schema)
    by_name={item["name"]:item for item in schema}

    for item in schema:
        target_arrow_type(item["type"])

    for aggregate in ir["aggregates"]:
        output=aggregate["output"]
        type_sql=_type_base(by_name[output]["type"])
        function=aggregate["function"]
        if function=="count":
            if type_sql!="BIGINT":
                raise ValueError(
                    "COUNT target must be BIGINT in aggregate runtime v1")
            continue
        if function=="avg":
            if type_sql!="DOUBLE":
                raise ValueError(
                    "AVG target must be DOUBLE in aggregate runtime v1")
            continue

        source=_source_signature(ir,aggregate["input"])
        data_type=str(source[1]).lower()
        column_type=str(source[2]).lower()
        if data_type in {"decimal","numeric"}:
            shape=_decimal_shape(column_type)
            scale=shape[1] if shape is not None else 0
            if not re.match(
                r"^DECIMAL(?:V\d+)?\s*\(38\s*,\s*%d\)$" % scale,
                type_sql,
            ):
                raise ValueError(
                    "SUM(DECIMAL) target must be DECIMAL(38,%d)" % scale)
        elif data_type in {
            "tinyint","smallint","mediumint","int","integer","bigint","year"
        }:
            if type_sql!="LARGEINT":
                raise ValueError(
                    "SUM(integer) target must be LARGEINT in aggregate runtime v1")
        elif data_type in {"float","double","real"}:
            if type_sql!="DOUBLE":
                raise ValueError(
                    "SUM(float) target must be DOUBLE in aggregate runtime v1")
        else:
            raise ValueError(
                "SUM input type is unsupported in aggregate runtime v1: "
                +data_type)
    return schema


def build(sink_key,sr_table,schema,primary_key,plan_version=0):
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
        raise ValueError(
            "aggregate target primary key must be unique and non-empty")
    missing=[name for name in keys if name not in names]
    if missing:
        raise ValueError(
            "aggregate target primary key is absent from schema: "
            +",".join(missing))

    import j4
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
        full_filter="",
        _catalog_sink=sink_key,
        _plan_version=int(plan_version),
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
    j4.validate_mapping(mapping)
    return mapping


def bind_target(mapping,target_columns,target_sequence=False):
    import j4
    target_columns=dict(target_columns or {})
    missing=set(mapping["_output_columns"])-set(target_columns)
    if missing:
        raise ValueError(
            "aggregate target is missing columns: "
            +",".join(sorted(missing)))
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


def mapping_from_descriptor(task):
    ir=task["ir"]
    schema=validate_semantic_target(
        ir,task["target_schema"])
    arrow_schema=pa.schema([
        pa.field(
            item["name"],target_arrow_type(item["type"]),
            nullable=bool(item["nullable"]))
        for item in schema
    ])
    mapping=build(
        task["sink_key"],task["target_table"],arrow_schema,
        ir["group_keys"],plan_version=task["plan_version"])
    target_rows={
        item["name"]:(
            item["name"],item["type"],
            "YES" if item["nullable"] else "NO",
            "PRI" if item["key"] else "",None,"")
        for item in schema
    }
    mapping=bind_target(mapping,target_rows,target_sequence=False)
    mapping["_target_ddl"]=""
    return mapping


def target_contract_from_rows(ir,ddl,rows):
    aggregate_ir.validate_ir(ir)
    key_match=re.search(
        r"PRIMARY\s+KEY\s*\(([^)]+)\)",str(ddl),re.I)
    if not key_match:
        raise ValueError("aggregate target must be a Primary Key table")
    primary=[
        value.strip().strip(chr(96))
        for value in key_match.group(1).split(",")
    ]
    if primary!=list(ir["group_keys"]):
        raise ValueError(
            "aggregate target primary key differs from GROUP BY key order")
    schema=[
        dict(
            name=str(row[0]),
            type=str(row[1]),
            nullable=str(row[2]).upper()=="YES",
            key=str(row[0]) in primary,
        )
        for row in rows
    ]
    return validate_semantic_target(ir,schema)


def load_target_mapping(cfg,task):
    import j4
    with j4.mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            if not j4.target_table_exists(
                cur,cfg,task["target_table"]
            ):
                raise RuntimeError(
                    "aggregate target table does not exist: "
                    +task["target_table"])
            cur.execute(
                "SHOW CREATE TABLE "
                +j4.sql_name(task["target_table"],True))
            ddl=str(cur.fetchone()[1])
            cur.execute(
                "SHOW COLUMNS FROM "
                +j4.sql_name(task["target_table"],True))
            rows=cur.fetchall()
    actual=target_contract_from_rows(
        task["ir"],ddl,rows)
    expected=aggregate_task_catalog.normalize_target_schema(
        task["ir"],task["target_schema"])
    if actual!=expected:
        raise RuntimeError(
            "aggregate target schema changed since task descriptor creation "
            "expected=%r actual=%r" % (expected,actual))
    mapping=mapping_from_descriptor(task)
    mapping["_target_ddl"]=ddl
    return mapping


def descriptor_schema_from_target(cfg,ir,target_table):
    import j4
    target_table=_text(target_table,"target_table")
    with j4.mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            if not j4.target_table_exists(cur,cfg,target_table):
                raise RuntimeError(
                    "aggregate target table does not exist: "+target_table)
            cur.execute(
                "SHOW CREATE TABLE "+j4.sql_name(target_table,True))
            ddl=str(cur.fetchone()[1])
            cur.execute(
                "SHOW COLUMNS FROM "+j4.sql_name(target_table,True))
            rows=cur.fetchall()
    return target_contract_from_rows(ir,ddl,rows)
