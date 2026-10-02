#!/usr/bin/env python3
"""Reproduce and fence durable Arrow string/large_string schema drift."""
from pathlib import Path
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4


def table(label_type,label,id_type=pa.int64()):
    id_value=(
        "1"
        if pa.types.is_string(id_type)
        or pa.types.is_large_string(id_type)
        else 1
    )
    return pa.Table.from_arrays([
        pa.array([id_value],type=id_type),
        pa.array([label],type=label_type),
        pa.array([0],type=pa.int16()),
        pa.array([99],type=pa.int32()),
        pa.array(["int64:1:1"],type=pa.large_string()),
        pa.array([0],type=pa.int32()),
    ],names=[
        "id","label",
        "_sync_op","_sync_order",
        "_sync_key","_sync_lane",
    ])


def main():
    mapping=dict(
        src_table="synthetic_join",
        _schema=[
            ("id",pa.int64()),
            ("label",pa.large_string()),
        ],
    )
    first=j4.arrow_table_payload(
        table(pa.string(),"short"))
    second=j4.arrow_table_payload(
        table(pa.large_string(),"wide"))

    merged=j4.concat_job_tables(
        mapping,[first,second])
    assert merged.num_rows==2
    assert merged.schema==j4.journal_schema(mapping)
    assert pa.types.is_large_string(
        merged.schema.field("label").type)
    assert merged.column("label").to_pylist()==[
        "short","wide"]
    assert merged.column(
        "_sync_order").to_pylist()==[0,1]
    assert merged.column(
        "_sync_lane").type==pa.uint16()
    assert merged.column(
        "_sync_key").type==pa.string()

    # Physical offset-width normalization must not become a generic semantic
    # cast that silently accepts a changed business type.
    invalid=j4.arrow_table_payload(
        table(
            pa.large_string(),"wrong",
            id_type=pa.string()))
    try:
        j4.arrow_job_table(mapping,invalid)
        raise AssertionError(
            "logical business type drift was accepted")
    except RuntimeError as exc:
        assert "changed logical type" in str(exc)

    print(
        "arrow_job_schema_test ok string_width "
        "metadata_width semantic_fail_closed",
        flush=True,
    )


if __name__=="__main__":
    main()
