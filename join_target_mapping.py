#!/usr/bin/env python3
"""Writer mapping contract for materialized INNER JOIN results.

Projected JOIN values are not a row identity under SQL bag semantics. Every
JOIN target therefore carries one internal text primary key, _j4_pair_id,
containing the exact durable pair identity encoding produced by join_job_bridge.
"""
import pyarrow as pa

import aggregate_target_mapping


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
