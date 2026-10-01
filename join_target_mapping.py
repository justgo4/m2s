#!/usr/bin/env python3
"""Writer mapping contract for materialized INNER JOIN results.

Projected JOIN values are not a row identity under SQL bag semantics. Every
JOIN target therefore carries one internal text primary key, _j4_pair_id,
containing the exact durable pair identity encoding produced by join_job_bridge.
"""
import re

import pyarrow as pa

import aggregate_target_mapping
import join_ir


PAIR_COLUMN="_j4_pair_id"


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def build(
        sink_key,sr_table,projection_schema,plan_version=0
):
    sink_key=_text(sink_key,"sink_key")
    sr_table=_text(sr_table,"sr_table")
    if not isinstance(projection_schema,pa.Schema):
        projection_schema=pa.schema(
            projection_schema)
    if PAIR_COLUMN in projection_schema.names:
        raise ValueError(
            "JOIN projection collides with internal pair identity")
    if any(
        name.startswith("_sync_")
        for name in projection_schema.names
    ):
        raise ValueError(
            "JOIN projection uses a reserved _sync_ column")

    schema=pa.schema([
        pa.field(
            PAIR_COLUMN,pa.string(),
            nullable=False),
        *list(projection_schema),
    ])
    mapping=aggregate_target_mapping.build(
        sink_key,sr_table,schema,
        PAIR_COLUMN,
        plan_version=int(plan_version))
    mapping["_join_pair_identity"]=PAIR_COLUMN
    return mapping


def bind_target(mapping,target_columns):
    target_columns=dict(target_columns or {})
    row=target_columns.get(PAIR_COLUMN)
    if row is None:
        raise ValueError(
            "JOIN target is missing internal pair identity column")
    if str(row[2]).upper()=="YES":
        raise ValueError(
            "JOIN target pair identity must be NOT NULL")
    if not str(row[3] or "").strip():
        raise ValueError(
            "JOIN target pair identity must be a key column")
    for name in mapping.get("_output_columns",()):
        if (
            name!=PAIR_COLUMN
            and name in target_columns
            and str(target_columns[name][3] or "").strip()
        ):
            raise ValueError(
                "JOIN target cannot key projected column "+name)
    bound=aggregate_target_mapping.bind_target(
        mapping,target_columns,
        target_sequence=False)
    if list(bound.get("_output_columns",()))[
        :1
    ]!=[PAIR_COLUMN]:
        raise RuntimeError(
            "JOIN pair identity is not the first writer column")
    return bound



def mapping_from_descriptor(task):
    ir=task["ir"]
    join_ir.validate_ir(ir)
    schema=list(task["target_schema"])
    expected=[
        PAIR_COLUMN,
        *join_ir.output_columns(ir),
    ]
    if [item["name"] for item in schema]!=expected:
        raise RuntimeError(
            "JOIN descriptor target output contract changed")
    projection_schema=pa.schema([
        pa.field(
            item["name"],
            aggregate_target_mapping.target_arrow_type(
                item["type"]),
            nullable=bool(item["nullable"]),
        )
        for item in schema
        if item["name"]!=PAIR_COLUMN
    ])
    mapping=build(
        task["sink_key"],task["target_table"],
        projection_schema,
        plan_version=task["plan_version"])
    target_rows={
        item["name"]:(
            item["name"],item["type"],
            "YES" if item["nullable"] else "NO",
            "PRI" if item["key"] else "",
            None,"",
        )
        for item in schema
    }
    mapping=bind_target(
        mapping,target_rows)
    mapping["_target_ddl"]=""
    return mapping


def target_contract_from_rows(ir,ddl,rows):
    join_ir.validate_ir(ir)
    key_match=re.search(
        r"PRIMARY\s+KEY\s*\(([^)]+)\)",
        str(ddl),re.I)
    if not key_match:
        raise ValueError(
            "JOIN target must be a Primary Key table")
    primary=[
        value.strip().strip(chr(96))
        for value in key_match.group(1).split(",")
    ]
    if primary!=[PAIR_COLUMN]:
        raise ValueError(
            "JOIN target primary key must be "
            +PAIR_COLUMN)
    schema=[
        dict(
            name=str(row[0]),
            type=str(row[1]),
            nullable=str(row[2]).upper()=="YES",
            key=str(row[0]) in primary,
        )
        for row in rows
    ]
    import join_task_catalog
    return join_task_catalog.normalize_target_schema(
        ir,schema)


def load_target_mapping(cfg,task):
    import j4
    with j4.mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            if not j4.target_table_exists(
                cur,cfg,task["target_table"]
            ):
                raise RuntimeError(
                    "JOIN target table does not exist: "
                    +task["target_table"])
            cur.execute(
                "SHOW CREATE TABLE "
                +j4.sql_name(
                    task["target_table"],True))
            ddl=str(cur.fetchone()[1])
            cur.execute(
                "SHOW COLUMNS FROM "
                +j4.sql_name(
                    task["target_table"],True))
            rows=cur.fetchall()
    actual=target_contract_from_rows(
        task["ir"],ddl,rows)
    import join_task_catalog
    expected=join_task_catalog.normalize_target_schema(
        task["ir"],task["target_schema"])
    if actual!=expected:
        raise RuntimeError(
            "JOIN target schema changed since task descriptor creation "
            "expected=%r actual=%r"
            % (expected,actual))
    mapping=mapping_from_descriptor(
        task)
    mapping["_target_ddl"]=ddl
    return mapping


def descriptor_schema_from_target(
        cfg,ir,target_table
):
    import j4
    target_table=_text(
        target_table,"target_table")
    with j4.mysql_connect(cfg,target=True) as target:
        with target.cursor() as cur:
            if not j4.target_table_exists(
                cur,cfg,target_table
            ):
                raise RuntimeError(
                    "JOIN target table does not exist: "
                    +target_table)
            cur.execute(
                "SHOW CREATE TABLE "
                +j4.sql_name(target_table,True))
            ddl=str(cur.fetchone()[1])
            cur.execute(
                "SHOW COLUMNS FROM "
                +j4.sql_name(target_table,True))
            rows=cur.fetchall()
    return target_contract_from_rows(
        ir,ddl,rows)
