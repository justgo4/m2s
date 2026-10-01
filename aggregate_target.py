#!/usr/bin/env python3
"""Prepare restart-safe writer mappings for candidate aggregate targets.

The durable task descriptor stores the target schema contract. On every daemon
restart the real StarRocks schema is read again and must match the normalized
contract before a writer mapping is reconstructed.
"""
import re

import pyarrow as pa

import aggregate_ir
import aggregate_task_catalog


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


def mapping_from_descriptor(task):
    ir=task["ir"]
    schema=validate_semantic_target(
        ir,task["target_schema"])
    names=[item["name"] for item in schema]
    mapping=dict(
        src_table=task["sink_key"],
        sr_table=task["target_table"],
        primary_key=(
            ir["group_keys"][0]
            if len(ir["group_keys"])==1
            else list(ir["group_keys"])),
        sql="SELECT "+",".join(
            '"' + name.replace('"','""') + '"'
            for name in names)
            +" FROM arrow_batch",
        full_filter="",
        _catalog_sink=task["sink_key"],
        _plan_version=int(task["plan_version"]),
        _schema=[
            (item["name"],target_arrow_type(item["type"]))
            for item in schema
        ],
    )

    import j4
    j4.validate_mapping(mapping)
    target_rows={
        item["name"]:(
            item["name"],item["type"],
            "YES" if item["nullable"] else "NO",
            "PRI" if item["key"] else "",None,"")
        for item in schema
    }
    mapping["_binary_output_columns"]=set()
    mapping["_output_columns"]=names
    mapping["_target_schema"]={
        item["name"]:dict(
            type=item["type"],nullable=item["nullable"],
            key="PRI" if item["key"] else "",
            default=None,extra="")
        for item in schema
    }
    mapping["_target_sequence"]=False
    mapping["_target_ddl"]=""
    mapping["_size_columns"]={}
    mapping["_target_json_columns"]=set()
    mapping["_target_constraints"]=j4.compile_target_constraints(
        mapping,target_rows)
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
    return mapping_from_descriptor(task)


def descriptor_schema_from_target(cfg,ir,target_table):
    import j4
    target_table=str(target_table or "").strip()
    if not target_table:
        raise ValueError("aggregate target_table is required")
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
