#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import join_ir
import join_target_mapping
import join_task_catalog


LEFT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("customer_id","bigint","bigint","YES",None,""),
    ("amount","bigint","bigint","YES",None,""),
]
RIGHT_SIG=[
    ("id","bigint","bigint","NO",None,""),
    ("name","varchar","varchar(64)","YES","utf8mb4_bin",""),
]


def plan():
    return join_ir.compile_sql(
        "db.orders",LEFT_SIG,"id",
        "db.customers",RIGHT_SIG,"id",
        'SELECT r.name AS customer_name,l.amount AS amount '
        'FROM left_batch l JOIN right_batch r '
        'ON l.customer_id=r.id',
    )


def target_schema():
    return [
        dict(
            name=join_target_mapping.PAIR_COLUMN,
            type="VARCHAR(1024)",nullable=False,key=True),
        dict(
            name="customer_name",
            type="VARCHAR(64)",nullable=True,key=False),
        dict(
            name="amount",
            type="BIGINT",nullable=True,key=False),
    ]


def expect_error(function,error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError(
        "expected "+error.__name__)


def main():
    con=sqlite3.connect(
        ":memory:",isolation_level=None)
    join_task_catalog.install(con)
    ir=plan()

    task=join_task_catalog.register_task(
        con,"join-task","join_sink",71,ir,
        "join_result","join-state",
        "join-consumer",target_schema())
    assert task["status"]=="candidate"
    assert task["source_relations"]==[
        "db.orders","db.customers"]
    assert task["generation_id"]=="sink:join_sink:plan:71"
    assert task["ir_id"]==join_ir.semantic_id(ir)
    assert task["target_schema"]==target_schema()

    mapping=join_target_mapping.mapping_from_descriptor(
        task)
    assert mapping["_plan_version"]==71
    assert mapping["sr_table"]=="join_result"
    assert mapping["_join_pair_identity"]==join_target_mapping.PAIR_COLUMN
    assert mapping["_output_columns"]==[
        join_target_mapping.PAIR_COLUMN,
        "customer_name","amount",
    ]

    same=join_task_catalog.register_task(
        con,"join-task","join_sink",71,ir,
        "join_result","join-state",
        "join-consumer",target_schema())
    assert same["descriptor_hash"]==task["descriptor_hash"]

    bad_schema=target_schema()
    bad_schema[2]=dict(
        name="amount",type="DOUBLE",
        nullable=True,key=False)
    expect_error(
        lambda: join_task_catalog.register_task(
            con,"join-task","join_sink",71,ir,
            "join_result","join-state",
            "join-consumer",bad_schema),
        RuntimeError)

    bad_key=target_schema()
    bad_key[1]=dict(
        name="customer_name",type="VARCHAR(64)",
        nullable=True,key=True)
    expect_error(
        lambda: join_task_catalog.normalize_target_schema(
            ir,bad_key),
        ValueError)

    bad_pair=target_schema()
    bad_pair[0]=dict(
        name=join_target_mapping.PAIR_COLUMN,
        type="BIGINT",nullable=False,key=True)
    expect_error(
        lambda: join_task_catalog.normalize_target_schema(
            ir,bad_pair),
        ValueError)

    active=join_task_catalog.set_status(
        con,"join-task","active")
    assert active["status"]=="active"
    retired=join_task_catalog.set_status(
        con,"join-task","retired")
    assert retired["status"]=="retired"
    expect_error(
        lambda: join_task_catalog.set_status(
            con,"join-task","active"),
        RuntimeError)
    expect_error(
        lambda: join_task_catalog.register_task(
            con,"join-task","join_sink",71,ir,
            "join_result","join-state",
            "join-consumer",target_schema()),
        RuntimeError)

    # Persisted descriptor hash must fail closed if storage is corrupted.
    con.execute("""
        UPDATE join_task_descriptors
        SET descriptor_hash='tampered'
        WHERE task_id='join-task'
    """)
    expect_error(
        lambda: join_task_catalog.task_info(
            con,"join-task"),
        RuntimeError)
    con.close()

    print(
        "join_task_catalog_test ok canonical_ir "
        "pair_target generation_identity integrity terminal",
        flush=True,
    )


if __name__=="__main__":
    main()
