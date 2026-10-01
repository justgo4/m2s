#!/usr/bin/env python3
"""Durable execution descriptors for candidate aggregate tasks.

This is not the user SQL catalog. cdc_catalog continues to reject stateful SQL.
A descriptor is written only after an aggregate candidate has been compiled and
its target schema has been checked. It makes restart recovery self-describing
without serializing Python/Arrow objects.
"""
import contextlib
import hashlib
import json
import time

import aggregate_ir
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
            "aggregate task descriptor schema must be installed before "
            "transactional use")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS aggregate_task_descriptors(
            task_id TEXT PRIMARY KEY,
            sink_key TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            generation_id TEXT NOT NULL,
            source_relation TEXT NOT NULL,
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
        CREATE INDEX IF NOT EXISTS aggregate_task_status
            ON aggregate_task_descriptors(status,updated);
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


def _expected_outputs(ir):
    return list(ir["group_keys"])+[
        item["output"] for item in ir["aggregates"]]


def normalize_target_schema(ir,target_schema):
    aggregate_ir.validate_ir(ir)
    if not isinstance(target_schema,(list,tuple)) or not target_schema:
        raise ValueError("aggregate target schema must be a non-empty list")
    expected=_expected_outputs(ir)
    result=[]
    seen=set()
    key_names=[]
    for item in target_schema:
        if not isinstance(item,dict):
            raise ValueError("aggregate target column must be a dict")
        if set(item)!={"name","type","nullable","key"}:
            raise ValueError(
                "aggregate target column fields must be name/type/nullable/key")
        name=_text(item["name"],"target column name")
        type_sql=_text(item["type"],"target column type").upper()
        if name in seen:
            raise ValueError("duplicate aggregate target column: "+name)
        seen.add(name)
        key=bool(item["key"])
        nullable=bool(item["nullable"])
        if key:
            key_names.append(name)
            if nullable:
                raise ValueError(
                    "aggregate target primary key cannot be nullable: "+name)
        result.append(dict(
            name=name,type=type_sql,nullable=nullable,key=key))
    if [item["name"] for item in result]!=expected:
        raise ValueError(
            "aggregate target columns/order differ from query output "
            "expected=%r actual=%r" % (
                expected,[item["name"] for item in result]))
    if key_names!=list(ir["group_keys"]):
        raise ValueError(
            "aggregate target primary key must equal GROUP BY key order")
    for aggregate in ir["aggregates"]:
        column=next(
            item for item in result
            if item["name"]==aggregate["output"])
        if aggregate["function"]=="count" and column["nullable"]:
            raise ValueError(
                "COUNT aggregate target must be NOT NULL: "
                +aggregate["output"])
        if aggregate["function"] in {"sum","avg"} and not column["nullable"]:
            raise ValueError(
                aggregate["function"].upper()
                +" aggregate target must be nullable: "
                +aggregate["output"])
    return result


def descriptor(
        task_id,sink_key,plan_version,ir,target_table,state_id,
        consumer_id,target_schema
):
    aggregate_ir.validate_ir(ir)
    task_id=_text(task_id,"task_id")
    sink_key=_text(sink_key,"sink_key")
    target_table=_text(target_table,"target_table")
    state_id=_text(state_id,"state_id")
    consumer_id=_text(consumer_id,"consumer_id")
    plan_version=int(plan_version)
    if plan_version<0:
        raise ValueError("aggregate task plan_version cannot be negative")
    normalized_schema=normalize_target_schema(ir,target_schema)
    value=dict(
        format_version=1,
        task_id=task_id,
        sink_key=sink_key,
        plan_version=plan_version,
        generation_id=task_generation.generation_id(
            sink_key,plan_version),
        source_relation=str(ir["source"]["relation"]),
        target_table=target_table,
        state_id=state_id,
        consumer_id=consumer_id,
        ir_id=aggregate_ir.semantic_id(ir),
        ir=ir,
        target_schema=normalized_schema,
    )
    value["descriptor_hash"]=hashlib.sha256(
        _canonical(value).encode("utf-8")).hexdigest()
    return value


def _row(row):
    if not row:
        raise KeyError("aggregate task descriptor does not exist")
    ir=json.loads(row[8])
    target_schema=json.loads(row[9])
    value=dict(
        task_id=str(row[0]),
        sink_key=str(row[1]),
        plan_version=int(row[2]),
        generation_id=str(row[3]),
        source_relation=str(row[4]),
        target_table=str(row[5]),
        state_id=str(row[6]),
        consumer_id=str(row[7]),
        ir_id=aggregate_ir.semantic_id(ir),
        ir=ir,
        target_schema=target_schema,
        descriptor_hash=str(row[10]),
        status=str(row[11]),
        created=float(row[12]),
        updated=float(row[13]),
    )
    expected=descriptor(
        value["task_id"],value["sink_key"],value["plan_version"],
        value["ir"],value["target_table"],value["state_id"],
        value["consumer_id"],value["target_schema"])
    if (
        value["generation_id"]!=expected["generation_id"]
        or value["source_relation"]!=expected["source_relation"]
        or value["ir_id"]!=expected["ir_id"]
        or value["descriptor_hash"]!=expected["descriptor_hash"]
    ):
        raise RuntimeError(
            "aggregate task descriptor integrity/semantic identity mismatch")
    if value["status"] not in STATUSES:
        raise RuntimeError("aggregate task descriptor has invalid status")
    return value


def task_info(con,task_id):
    row=con.execute("""
        SELECT task_id,sink_key,plan_version,generation_id,source_relation,
               target_table,state_id,consumer_id,ir_id,ir_json,
               target_schema_json,descriptor_hash,status,created,updated
        FROM aggregate_task_descriptors WHERE task_id=?
    """,(_text(task_id,"task_id"),)).fetchone()
    # _row expects ir_json at index 8; omit stored ir_id from materialized row.
    if not row:
        raise KeyError("aggregate task descriptor does not exist")
    materialized=(
        row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],
        row[9],row[10],row[11],row[12],row[13],row[14])
    value=_row(materialized)
    if str(row[8])!=value["ir_id"]:
        raise RuntimeError("aggregate task stored IR id differs from IR")
    return value


def register_task(
        con,task_id,sink_key,plan_version,ir,target_table,state_id,
        consumer_id,target_schema
):
    expected=descriptor(
        task_id,sink_key,plan_version,ir,target_table,state_id,
        consumer_id,target_schema)
    try:
        current=task_info(con,expected["task_id"])
    except KeyError:
        current=None
    if current is not None:
        if current["descriptor_hash"]!=expected["descriptor_hash"]:
            raise RuntimeError(
                "aggregate task id was reused with different semantics")
        if current["status"] in {"retired","failed"}:
            raise RuntimeError(
                "terminal aggregate task descriptor cannot be revived")
        return current
    conflict=con.execute("""
        SELECT task_id FROM aggregate_task_descriptors
        WHERE sink_key=? AND plan_version=?
    """,(expected["sink_key"],expected["plan_version"])).fetchone()
    if conflict:
        raise RuntimeError(
            "aggregate sink/plan generation already belongs to task "
            +str(conflict[0]))
    now=time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO aggregate_task_descriptors(
                task_id,sink_key,plan_version,generation_id,source_relation,
                target_table,state_id,consumer_id,ir_id,ir_json,
                target_schema_json,descriptor_hash,status,created,updated)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'candidate',?,?)
        """,(
            expected["task_id"],expected["sink_key"],
            expected["plan_version"],expected["generation_id"],
            expected["source_relation"],expected["target_table"],
            expected["state_id"],expected["consumer_id"],
            expected["ir_id"],_canonical(expected["ir"]),
            _canonical(expected["target_schema"]),
            expected["descriptor_hash"],now,now))
    return task_info(con,expected["task_id"])


def set_status(con,task_id,status):
    task_id=_text(task_id,"task_id")
    status=str(status)
    if status not in STATUSES:
        raise ValueError("invalid aggregate task status")
    current=task_info(con,task_id)
    if current["status"]==status:
        return current
    terminal={"retired","failed"}
    if current["status"] in terminal:
        raise RuntimeError(
            "terminal aggregate task descriptor cannot transition")
    if status=="candidate" and current["status"]!="candidate":
        raise RuntimeError(
            "aggregate task cannot transition back to candidate")
    with transaction(con):
        con.execute("""
            UPDATE aggregate_task_descriptors
            SET status=?,updated=? WHERE task_id=?
        """,(status,time.time(),task_id))
    return task_info(con,task_id)


def list_tasks(con,statuses=None):
    statuses=None if statuses is None else [str(value) for value in statuses]
    if statuses:
        if any(value not in STATUSES for value in statuses):
            raise ValueError("invalid aggregate task status filter")
        marks=",".join("?" for _ in statuses)
        rows=con.execute(
            "SELECT task_id FROM aggregate_task_descriptors "
            "WHERE status IN ("+marks+") ORDER BY task_id",
            statuses).fetchall()
    else:
        rows=con.execute(
            "SELECT task_id FROM aggregate_task_descriptors "
            "ORDER BY task_id").fetchall()
    return [task_info(con,row[0]) for row in rows]
