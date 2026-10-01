#!/usr/bin/env python3
"""Bridge aggregate outbox commits into the proven j4 durable job pipeline.

The bridge is intentionally target-agnostic except for receiving a prepared
identity mapping. It reuses j4 Arrow routing/lane partitioning and writes normal
CDC jobs plus a sidecar link. acknowledge_delivery() advances the aggregate
outbox frontier only after all jobs for one source_seq are retired.
"""
import datetime
import decimal
import tempfile
import time

import duckdb
import pyarrow as pa

import aggregate_outbox
import j4


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _delete_placeholder(dtype):
    if pa.types.is_boolean(dtype):
        return False
    if pa.types.is_integer(dtype):
        return 0
    if pa.types.is_floating(dtype):
        return 0.0
    if pa.types.is_decimal(dtype):
        return decimal.Decimal(0)
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return ""
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return b""
    if pa.types.is_date(dtype):
        return datetime.date(1970,1,1)
    if pa.types.is_timestamp(dtype):
        return datetime.datetime(1970,1,1)
    raise ValueError(
        "aggregate delete cannot synthesize required target value for "
        +str(dtype))


def _delete_wire_row(mapping,row):
    result=dict(row)
    keys=set(j4.pk_columns(mapping))
    target_schema=mapping.get("_target_schema",{})
    for name,dtype in mapping["_schema"]:
        if name in result or name in keys:
            continue
        target=target_schema.get(name,{})
        nullable=bool(target.get("nullable",True))
        result[name]=None if nullable else _delete_placeholder(dtype)
    return result


def _mutations(con,consumer_id,source_seq,mapping):
    rows=aggregate_outbox.commit_rows(
        con,consumer_id,source_seq)
    result=[]
    for item in rows:
        op=int(item["op"])
        row=dict(item["row"])
        if op==1:
            row=_delete_wire_row(mapping,row)
        result.append((op,row))
    return result


def _already_staged(con,consumer_id,source_seq):
    return [
        int(row[0]) for row in con.execute("""
            SELECT job_id FROM aggregate_job_links
            WHERE consumer_id=? AND source_seq=?
            ORDER BY job_id
        """,(_text(consumer_id,"consumer_id"),int(source_seq))).fetchall()
    ]


def stage_commit(con,consumer_id,source_seq,mapping,cfg,engine=None):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    commit=aggregate_outbox.commit_info(
        con,consumer_id,source_seq)
    if commit["visible"]:
        return dict(
            consumer_id=consumer_id,source_seq=source_seq,
            visible=True,job_ids=[])
    existing=_already_staged(
        con,consumer_id,source_seq)
    if existing:
        return dict(
            consumer_id=consumer_id,source_seq=source_seq,
            visible=False,job_ids=existing)

    mutations=_mutations(
        con,consumer_id,source_seq,mapping)
    if not mutations:
        aggregate_outbox.mark_visible(
            con,consumer_id,source_seq)
        return dict(
            consumer_id=consumer_id,source_seq=source_seq,
            visible=True,job_ids=[])

    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    try:
        raw=j4.raw_arrow(mapping,mutations)
        routed=j4.route_arrow(
            engine,mapping,raw,j4.key_partition_count(cfg))
        with tempfile.SpooledTemporaryFile(
            max_size=1024**2,
            dir=j4.state_temp_dir(cfg)) as spool:
            j4.spool_routed(
                spool,mapping,routed,cfg)
            spool.seek(0)
            records=[]
            while True:
                record=j4.read_spool_record(spool)
                if record is None:
                    break
                records.append(record)
    finally:
        if own_engine:
            engine.close()

    if not records:
        raise RuntimeError(
            "aggregate output rows produced no routed durable jobs")
    stream=aggregate_outbox.stream_info(
        con,consumer_id)
    table=j4.mapping_key(mapping)
    now=time.time()
    job_ids=[]
    logical_total=0
    with j4.state_transaction(con):
        if _already_staged(
            con,consumer_id,source_seq):
            raise RuntimeError(
                "aggregate outbox commit was staged concurrently")
        for record in records:
            record_table,lane,payload,nrows,logical_bytes=record
            if record_table!=table:
                raise RuntimeError(
                    "aggregate bridge routed an unexpected table")
            cur=con.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,logical_bytes,
                    plan_version,source_file,source_pos,source_seq,
                    source_time,created)
                VALUES(?,?,'cdc',?,?,?,?,NULL,NULL,?,?,?)
            """,(
                table,int(lane),bytes(payload),int(nrows),
                int(logical_bytes),int(stream["plan_version"]),
                source_seq,now,now))
            job_id=int(cur.lastrowid)
            con.execute("""
                INSERT INTO aggregate_job_links(
                    job_id,consumer_id,source_seq)
                VALUES(?,?,?)
            """,(job_id,consumer_id,source_seq))
            job_ids.append(job_id)
            logical_total+=int(logical_bytes)
        j4.meta_set(
            con,"pending_bytes",
            j4.meta_get(con,"pending_bytes",0)+logical_total)
    return dict(
        consumer_id=consumer_id,source_seq=source_seq,
        visible=False,job_ids=job_ids)


def stage_pending(con,consumer_id,mapping,cfg,limit=100,engine=None):
    result=[]
    for commit in aggregate_outbox.pending_commits(
        con,consumer_id,limit=limit):
        result.append(stage_commit(
            con,consumer_id,commit["source_seq"],
            mapping,cfg,engine=engine))
    return result
