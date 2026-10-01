#!/usr/bin/env python3
from decimal import Decimal
from pathlib import Path
import random
import sys

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import incremental_ir
import relational_ir


SEED = 20261001
OPERATIONS = 5000
BATCH = 100


def build_ir():
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
    rel=relational_ir.mapping_ir(mapping)
    return rel,incremental_ir.compile_ir(rel)


def arrow_rows(rows, include_meta):
    fields=[
        pa.field("id",pa.int64()),
        pa.field("amount",pa.decimal128(18,2)),
        pa.field("status",pa.string()),
    ]
    if include_meta:
        fields += [
            pa.field("_sync_op",pa.int8()),
            pa.field("_sync_order",pa.int64()),
        ]
    return pa.Table.from_pylist(rows,schema=pa.schema(fields))


def full_expected(con, rel, source):
    rows=[
        dict(id=key,amount=value[0],status=value[1])
        for key,value in sorted(source.items())
    ]
    raw=arrow_rows(rows,False)
    con.register("_sync_raw",raw)
    result=con.execute(
        relational_ir.to_duckdb_sql(rel)
    ).fetchall()
    con.unregister("_sync_raw")
    return {int(row[0]):row[1] for row in result}


def apply_delta(con, rel, delta, events, output):
    if not events:
        return
    raw=arrow_rows(events,True)
    con.register("_sync_raw",raw)
    rows=con.execute(
        incremental_ir.to_duckdb_sql(delta,rel)
        + ' ORDER BY "_sync_order"'
    ).fetchall()
    con.unregister("_sync_raw")
    for key,value,op,_ in rows:
        key=int(key)
        if int(op)==1:
            output.pop(key,None)
        else:
            output[key]=value


def random_value(rng):
    amount_choice=rng.randrange(8)
    amount=(
        None if amount_choice==0
        else Decimal(rng.randrange(-500,1501)).scaleb(-2)
    )
    status=rng.choice(["paid","open","cancelled",None])
    return amount,status


def main():
    rng=random.Random(SEED)
    rel,delta=build_ir()
    source={}
    output={}
    con=duckdb.connect(":memory:")
    pending=[]
    order=0

    for step in range(OPERATIONS):
        key=rng.randrange(1,251)
        old=source.get(key)
        action=rng.randrange(10)
        if old is None:
            new=random_value(rng)
        elif action < 2:
            new=None
        else:
            new=random_value(rng)

        if old is not None:
            pending.append(dict(
                id=key,amount=old[0],status=old[1],
                _sync_op=1,_sync_order=order))
            order+=1
        if new is not None:
            pending.append(dict(
                id=key,amount=new[0],status=new[1],
                _sync_op=0,_sync_order=order))
            order+=1
            source[key]=new
        else:
            source.pop(key,None)

        if (step+1)%BATCH==0 or step+1==OPERATIONS:
            apply_delta(con,rel,delta,pending,output)
            pending=[]
            expected=full_expected(con,rel,source)
            if output != expected:
                missing=sorted(set(expected)-set(output))[:10]
                extra=sorted(set(output)-set(expected))[:10]
                changed=sorted(
                    key for key in set(output)&set(expected)
                    if output[key] != expected[key]
                )[:10]
                raise AssertionError(
                    "incremental/full state mismatch "
                    f"step={step+1} missing={missing} extra={extra} "
                    f"changed={changed}"
                )

    con.close()
    print(
        f"incremental_ir_state_oracle ok seed={SEED} "
        f"operations={OPERATIONS} final_rows={len(output)}",
        flush=True,
    )


if __name__=="__main__":
    main()
