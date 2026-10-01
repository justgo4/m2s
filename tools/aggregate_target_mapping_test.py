#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import json
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import aggregate_target_mapping
import j4


def main():
    schema=pa.schema([
        pa.field("category",pa.string(),nullable=False),
        pa.field("n",pa.int64()),
        pa.field("total",pa.decimal128(38,2)),
        pa.field("mean",pa.float64()),
        pa.field("_row_count",pa.int64()),
    ])
    mapping=aggregate_target_mapping.build(
        "agg-sink","agg_result",schema,"category")
    assert j4.pk_columns(mapping)==["category"]
    assert mapping["_output_columns"]==schema.names
    assert mapping["_arrow_columns"]==schema.names

    target_columns={
        "category":("category","varchar(64)","NO","PRI",None,""),
        "n":("n","bigint","YES","",None,""),
        "total":("total","decimal(38,2)","YES","",None,""),
        "mean":("mean","double","YES","",None,""),
        "_row_count":("_row_count","bigint","YES","",None,""),
    }
    mapping=aggregate_target_mapping.bind_target(
        mapping,target_columns)
    assert "category" in mapping["_target_constraints"]

    raw=j4.raw_arrow(mapping,[
        (0,dict(
            category="a",n=2,total=Decimal("30.00"),
            mean=15.0,_row_count=2)),
        (1,dict(category="b")),
    ])
    engine=j4.transform_engine(dict(
        duckdb_memory="64MB",
        catalog_macros=(),catalog_udfs=(),
    ))
    try:
        lines=[]
        for batch,overflows in j4.transformed_line_batches(
            engine,mapping,raw,sequence=7,
            collect_overflow=True,delivery_dense_order=True):
            assert overflows==[]
            lines.extend(batch.to_pylist())
    finally:
        engine.close()
    decoded=[json.loads(line) for line in lines]
    assert decoded[0]["category"]=="a"
    assert decoded[0]["n"]==2
    assert decoded[0]["total"]==30.0
    assert decoded[0]["__op"]==0
    assert decoded[1]["category"]=="b"
    assert decoded[1]["__op"]==1

    for bad in (
        lambda: aggregate_target_mapping.build(
            "x","y",schema,"missing"),
        lambda: aggregate_target_mapping.build(
            "x","y",
            pa.schema([pa.field("_sync_bad",pa.int64())]),
            "_sync_bad"),
    ):
        try:
            bad()
            raise AssertionError("invalid aggregate target mapping accepted")
        except ValueError:
            pass

    print(
        "aggregate_target_mapping_test ok identity projection "
        "target_constraints upsert_delete_json",
        flush=True,
    )


if __name__=="__main__":
    main()
