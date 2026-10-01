#!/usr/bin/env python3
"""Durable execution descriptors for candidate INNER JOIN tasks.

The user SQL catalog still rejects stateful JOIN. This descriptor makes the
candidate runtime restart-self-describing by pinning JOIN IR, ordered two-source
identity, target contract, state/consumer IDs and generation identity.
"""
import contextlib
import hashlib
import json
import time

import aggregate_target_mapping
import join_ir
import join_target_mapping
import task_generation


STATUSES={"candidate","active","retired","failed"}


@contextlib.contextmanager
def transaction(con):
    if con.in_transaction:
        yield
        return
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def install(con):
    if con.in_transaction:
        raise RuntimeError(
            "JOIN task descriptor schema must be installed before "
            "transactional use")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS join_task_descriptors(
            task_id TEXT PRIMARY KEY,
            sink_key TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            generation_id TEXT NOT NULL,
            left_relation TEXT NOT NULL,
            right_relation TEXT NOT NULL,
            target_table TEXT NOT NULL,
            state_id TEXT NOT NULL,
            consumer_id TEXT NOT NULL UNIQUE,
            ir_id TEXT NOT NULL,
            ir_json TEXT NOT NULL,
            target_schema_json TEXT NOT NULL,
            descriptor_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL,
            UNIQUE(sink_key,plan_version));
        CREATE INDEX IF NOT EXISTS join_task_status
            ON join_task_descriptors(status,updated);
    """)


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _canonical(value):
    return json.dumps(
        value,ensure_ascii=False,sort_keys=True,
        separators=(",",":"))


def expected_outputs(ir):
    join_ir.validate_ir(ir)
    return [
        join_target_mapping.PAIR_COLUMN,
        *join_ir.output_columns(ir),
    ]


def normalize_target_schema(ir,target_schema):
    join_ir.validate_ir(ir)
    if not isinstance(target_schema,(list,tuple)) or not target_schema:
        raise ValueError("JOIN target schema must be a non-empty list")
    expected=expected_outputs(ir)
    result=[]
    seen=set()
    key_names=[]
    for item in target_schema:
        if not isinstance(item,dict):
            raise ValueError("JOIN target column must be a dict")
        if set(item)!={"name","type","nullable","key"}:
            raise ValueError(
                "JOIN target column fields must be name/type/nullable/key")
        name=_text(item["name"],"target column name")
        type_sql=_text(item["type"],"target column type").upper()
        aggregate_target_mapping.target_arrow_type(type_sql)
        if name in seen:
            raise ValueError("duplicate JOIN target column: "+name)
        seen.add(name)
        nullable=bool(item["nullable"])
        key=bool(item["key"])
        if key:
            key_names.append(name)
            if nullable:
                raise ValueError(
                    "JOIN target primary key cannot be nullable: "+name)
        result.append(dict(
            name=name,type=type_sql,
            nullable=nullable,key=key))
    actual=[item["name"] for item in result]
    if actual!=expected:
        raise ValueError(
            "JOIN target columns/order differ from materialized output "
            "expected=%r actual=%r" % (expected,actual))
    if key_names!=[join_target_mapping.PAIR_COLUMN]:
        raise ValueError(
            "JOIN target primary key must be the internal pair identity only")
    pair=result[0]
    if not (
        pair["type"].startswith("VARCHAR(")
        or pair["type"].startswith("CHAR(")
        or pair["type"]=="STRING"
    ):
        raise ValueError(
            "JOIN pair identity target must be a text type")
    return result


def descriptor(
        task_id,sink_key,plan_version,ir,target_table,
        state_id,consumer_id,target_schema
):
    join_ir.validate_ir(ir)
    task_id=_text(task_id,"task_id")
    sink_key=_text(sink_key,"sink_key")
    target_table=_text(target_table,"target_table")
    state_id=_text(state_id,"state_id")
    consumer_id=_text(consumer_id,"consumer_id")
    plan_version=int(plan_version)
    if plan_version<0:
        raise ValueError(
            "JOIN task plan_version cannot be negative")
    normalized_schema=normalize_target_schema(
        ir,target_schema)
    left=str(ir["sources"]["left"]["relation"])
    right=str(ir["sources"]["right"]["relation"])
    value=dict(
        format_version=1,
        task_id=task_id,
        sink_key=sink_key,
        plan_version=plan_version,
        generation_id=task_generation.generation_id(
            sink_key,plan_version),
        source_relations=[left,right],
        target_table=target_table,
        state_id=state_id,
        consumer_id=consumer_id,
        ir_id=join_ir.semantic_id(ir),
        ir=ir,
        target_schema=normalized_schema,
    )
    value["descriptor_hash"]=hashlib.sha256(
        _canonical(value).encode("utf-8")
    ).hexdigest()
    return value


def _row(row):
    if not row:
        raise KeyError(
            "JOIN task descriptor does not exist")
    ir=json.loads(row[9])
    target_schema=json.loads(row[10])
    value=dict(
        task_id=str(row[0]),
        sink_key=str(row[1]),
        plan_version=int(row[2]),
        generation_id=str(row[3]),
        source_relations=[
            str(row[4]),str(row[5])],
        target_table=str(row[6]),
        state_id=str(row[7]),
        consumer_id=str(row[8]),
        ir_id=join_ir.semantic_id(ir),
        ir=ir,
        target_schema=target_schema,
        descriptor_hash=str(row[11]),
        status=str(row[12]),
        created=float(row[13]),
        updated=float(row[14]),
    )
    expected=descriptor(
        value["task_id"],value["sink_key"],
        value["plan_version"],value["ir"],
        value["target_table"],value["state_id"],
        value["consumer_id"],value["target_schema"])
    if (
        value["generation_id"]!=expected["generation_id"]
        or value["source_relations"]!=expected["source_relations"]
        or value["ir_id"]!=expected["ir_id"]
        or value["descriptor_hash"]!=expected["descriptor_hash"]
    ):
        raise RuntimeError(
            "JOIN task descriptor integrity/semantic identity mismatch")
    if value["status"] not in STATUSES:
        raise RuntimeError(
            "JOIN task descriptor has invalid status")
    return value


def task_info(con,task_id):
    row=con.execute("""
        SELECT task_id,sink_key,plan_version,generation_id,
               left_relation,right_relation,target_table,
               state_id,consumer_id,ir_id,ir_json,
               target_schema_json,descriptor_hash,status,created,updated
        FROM join_task_descriptors
        WHERE task_id=?
    """,(_text(task_id,"task_id"),)).fetchone()
    if not row:
        raise KeyError(
            "JOIN task descriptor does not exist")
    materialized=(
        row[0],row[1],row[2],row[3],row[4],row[5],
        row[6],row[7],row[8],row[10],row[11],
        row[12],row[13],row[14],row[15])
    value=_row(materialized)
    if str(row[9])!=value["ir_id"]:
        raise RuntimeError(
            "JOIN task stored IR id differs from IR")
    return value


def register_task(
        con,task_id,sink_key,plan_version,ir,target_table,
        state_id,consumer_id,target_schema
):
    expected=descriptor(
        task_id,sink_key,plan_version,ir,target_table,
        state_id,consumer_id,target_schema)
    try:
        current=task_info(
            con,expected["task_id"])
    except KeyError:
        current=None
    if current is not None:
        if current["descriptor_hash"]!=expected["descriptor_hash"]:
            raise RuntimeError(
                "JOIN task id was reused with different semantics")
        if current["status"] in {
            "retired","failed"
        }:
            raise RuntimeError(
                "terminal JOIN task descriptor cannot be revived")
        return current

    conflict=con.execute("""
        SELECT task_id FROM join_task_descriptors
        WHERE sink_key=? AND plan_version=?
    """,(
        expected["sink_key"],
        expected["plan_version"],
    )).fetchone()
    if conflict:
        raise RuntimeError(
            "JOIN sink/plan generation already belongs to task "
            +str(conflict[0]))

    now=time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO join_task_descriptors(
                task_id,sink_key,plan_version,generation_id,
                left_relation,right_relation,target_table,
                state_id,consumer_id,ir_id,ir_json,
                target_schema_json,descriptor_hash,status,
                created,updated)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'candidate',?,?)
        """,(
            expected["task_id"],expected["sink_key"],
            expected["plan_version"],expected["generation_id"],
            expected["source_relations"][0],
            expected["source_relations"][1],
            expected["target_table"],expected["state_id"],
            expected["consumer_id"],expected["ir_id"],
            _canonical(expected["ir"]),
            _canonical(expected["target_schema"]),
            expected["descriptor_hash"],now,now))
    return task_info(
        con,expected["task_id"])


def set_status(con,task_id,status):
    task_id=_text(task_id,"task_id")
    status=str(status)
    if status not in STATUSES:
        raise ValueError(
            "invalid JOIN task status")
    current=task_info(
        con,task_id)
    if current["status"]==status:
        return current
    if current["status"] in {
        "retired","failed"
    }:
        raise RuntimeError(
            "terminal JOIN task descriptor cannot transition")
    if status=="candidate":
        raise RuntimeError(
            "JOIN task cannot transition back to candidate")
    now=time.time()
    with transaction(con):
        con.execute("""
            UPDATE join_task_descriptors
            SET status=?,updated=?
            WHERE task_id=?
        """,(status,now,task_id))
    return task_info(
        con,task_id)


def list_tasks(con,status=None):
    if status is None:
        rows=con.execute("""
            SELECT task_id FROM join_task_descriptors
            ORDER BY task_id
        """).fetchall()
    else:
        status=str(status)
        if status not in STATUSES:
            raise ValueError(
                "invalid JOIN task status")
        rows=con.execute("""
            SELECT task_id FROM join_task_descriptors
            WHERE status=? ORDER BY task_id
        """,(status,)).fetchall()
    return [
        task_info(con,row[0])
        for row in rows
    ]
