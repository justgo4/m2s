#!/usr/bin/env python3
from pathlib import Path
import sys

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import join_target_mapping


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected "+error.__name__)


def rows(
        pair_nullable="NO",
        pair_key="PRI",
        projection_key=""
):
    return {
        join_target_mapping.PAIR_COLUMN:(
            join_target_mapping.PAIR_COLUMN,
            "varchar(1024)",
            pair_nullable,pair_key,None,""),
        "customer_name":(
            "customer_name","varchar(64)",
            "YES",projection_key,None,""),
        "amount":(
            "amount","bigint",
            "YES","",None,""),
    }


def main():
    projection=pa.schema([
        pa.field(
            "customer_name",pa.string()),
        pa.field(
            "amount",pa.int64()),
    ])
    mapping=join_target_mapping.build(
        "join_sink","join_result",
        projection,plan_version=61)
    assert j4.pk_columns(mapping)==[
        join_target_mapping.PAIR_COLUMN]
    assert mapping["_output_columns"]==[
        join_target_mapping.PAIR_COLUMN,
        "customer_name","amount",
    ]
    assert mapping["_plan_version"]==61
    assert mapping["_join_pair_identity"]==(
        join_target_mapping.PAIR_COLUMN)

    bound=join_target_mapping.bind_target(
        mapping,rows())
    assert bound["_target_schema"][
        join_target_mapping.PAIR_COLUMN
    ]["nullable"] is False

    expect_error(
        lambda: join_target_mapping.bind_target(
            mapping,rows(pair_nullable="YES")),
        ValueError)
    expect_error(
        lambda: join_target_mapping.bind_target(
            mapping,rows(pair_key="")),
        ValueError)
    expect_error(
        lambda: join_target_mapping.bind_target(
            mapping,rows(projection_key="PRI")),
        ValueError)

    missing=rows()
    missing.pop(
        join_target_mapping.PAIR_COLUMN)
    expect_error(
        lambda: join_target_mapping.bind_target(
            mapping,missing),
        ValueError)

    expect_error(
        lambda: join_target_mapping.build(
            "join_sink","join_result",
            pa.schema([
                pa.field(
                    join_target_mapping.PAIR_COLUMN,
                    pa.string()),
            ])),
        ValueError)
    expect_error(
        lambda: join_target_mapping.build(
            "join_sink","join_result",
            pa.schema([
                pa.field("_sync_bad",pa.int64()),
            ])),
        ValueError)

    print(
        "join_target_mapping_test ok pair_pk "
        "not_null projection_not_key reserved_names",
        flush=True,
    )


if __name__=="__main__":
    main()
