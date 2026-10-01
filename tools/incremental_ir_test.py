#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import incremental_ir
import relational_ir


def relation():
    mapping = dict(
        src_table="orders",
        primary_key=["id"],
        _sql=(
            'SELECT "id", "amount" * 2 AS "double_amount" '
            'FROM arrow_batch WHERE "status" = \'paid\''
        ),
        _filter_sql='"amount" > 0',
        _schema_signature=[
            ("id","bigint","bigint",None,None,False),
            ("amount","decimal","decimal(18,2)",None,None,True),
            ("status","varchar","varchar(16)",None,"utf8mb4_bin",True),
        ],
    )
    return relational_ir.mapping_ir(mapping)


def main():
    rel = relation()
    delta = incremental_ir.compile_ir(rel)
    assert delta["relation_semantic_id"] == relational_ir.semantic_id(rel)
    assert delta["input_changes"]["update_requires_before_image"]
    assert incremental_ir.semantic_id(delta) == incremental_ir.semantic_id(
        incremental_ir.compile_ir(rel))

    # Four UPDATE cases encoded as delete(old)+upsert(new):
    # paid->open deletes output; open->paid inserts output;
    # paid amount change retracts old then inserts new; NULL stays filtered.
    rows = [
        dict(id=1,amount=Decimal("2.00"),status="paid",op=1,order=0),
        dict(id=1,amount=Decimal("2.00"),status="open",op=0,order=1),
        dict(id=2,amount=Decimal("3.00"),status="open",op=1,order=2),
        dict(id=2,amount=Decimal("3.00"),status="paid",op=0,order=3),
        dict(id=3,amount=Decimal("4.00"),status="paid",op=1,order=4),
        dict(id=3,amount=Decimal("5.00"),status="paid",op=0,order=5),
        dict(id=4,amount=None,status="paid",op=1,order=6),
        dict(id=4,amount=None,status="paid",op=0,order=7),
    ]
    raw = pa.Table.from_pylist([
        dict(
            id=row["id"],amount=row["amount"],status=row["status"],
            _sync_op=row["op"],_sync_order=row["order"])
        for row in rows
    ],schema=pa.schema([
        pa.field("id",pa.int64()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("status",pa.string()),
        pa.field("_sync_op",pa.int8()),
        pa.field("_sync_order",pa.int64()),
    ]))
    con=duckdb.connect(":memory:")
    con.register("_sync_raw",raw)
    actual=con.execute(
        incremental_ir.to_duckdb_sql(delta,rel)
        + ' ORDER BY "_sync_order"'
    ).fetchall()
    con.close()
    assert actual == [
        (1,Decimal("4.00"),1,0),
        (2,Decimal("6.00"),0,3),
        (3,Decimal("8.00"),1,4),
        (3,Decimal("10.00"),0,5),
    ]

    broken=dict(delta)
    broken["relation_semantic_id"]="0"*64
    try:
        incremental_ir.to_duckdb_sql(broken,rel)
        raise AssertionError("mismatched relational/delta IR was accepted")
    except ValueError:
        pass

    print("incremental_ir_test ok",flush=True)


if __name__=="__main__":
    main()
