#!/usr/bin/env python3
"""Bridge INNER JOIN outbox commits into the proven j4 durable job pipeline.

JOIN bag semantics require a stable row identity even when projected values are
identical. The bridge therefore materializes the stream-versioned durable target identity
as an internal text column named _j4_pair_id. Writer mappings for JOIN targets
must use that column as their sole Primary Key.
"""
import tempfile
import time

import duckdb

import j4
import join_outbox
from join_target_mapping import PAIR_COLUMN


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _pair_text(
        pair_id,identity_format=join_outbox.DEFAULT_IDENTITY_FORMAT
):
    return join_outbox.target_id(
        pair_id,identity_format)


def validate_mapping(mapping):
    keys=list(j4.pk_columns(mapping))
    if keys!=[PAIR_COLUMN]:
        raise ValueError(
            "JOIN writer mapping primary key must be "+PAIR_COLUMN)
    if PAIR_COLUMN not in list(mapping.get("_output_columns",())):
        raise ValueError(
            "JOIN writer mapping must output "+PAIR_COLUMN)
    return mapping


def _mutations(con,consumer_id,source_seq,mapping):
    validate_mapping(mapping)
    result=[]
    for item in join_outbox.commit_rows(
        con,consumer_id,source_seq
    ):
        row=dict(item["row"])
        row[PAIR_COLUMN]=join_outbox.target_id_for_pair(
            con,consumer_id,item["pair_id"])
        result.append((int(item["op"]),row))
    return result


def _already_staged(con,consumer_id,source_seq):
    return [
        int(row[0]) for row in con.execute("""
            SELECT job_id FROM join_job_links
            WHERE consumer_id=? AND source_seq=?
            ORDER BY job_id
        """,(
            _text(consumer_id,"consumer_id"),
            int(source_seq),
        )).fetchall()
    ]


def stage_commit(
        con,consumer_id,source_seq,mapping,cfg,engine=None
):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    validate_mapping(mapping)
    commit=join_outbox.commit_info(
        con,consumer_id,source_seq)
    if commit["visible"]:
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=True,job_ids=[])

    existing=_already_staged(
        con,consumer_id,source_seq)
    if existing:
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=False,job_ids=existing)

    mutations=_mutations(
        con,consumer_id,source_seq,mapping)
    if not mutations:
        join_outbox.mark_visible(
            con,consumer_id,source_seq)
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=True,job_ids=[])

    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    try:
        raw=j4.raw_arrow(
            mapping,mutations)
        routed=j4.route_arrow(
            engine,mapping,raw,
            j4.key_partition_count(cfg))
        with tempfile.SpooledTemporaryFile(
            max_size=1024**2,
            dir=j4.state_temp_dir(cfg),
        ) as spool:
            j4.spool_routed(
                spool,mapping,routed,cfg)
            spool.seek(0)
            records=[]
            while True:
                record=j4.read_spool_record(
                    spool)
                if record is None:
                    break
                records.append(record)
    finally:
        if own_engine:
            engine.close()

    if not records:
        raise RuntimeError(
            "JOIN output rows produced no routed durable jobs")

    stream=join_outbox.stream_info(
        con,consumer_id)
    table=j4.mapping_key(mapping)
    now=time.time()
    job_ids=[]
    logical_total=0
    with j4.state_transaction(con):
        if _already_staged(
            con,consumer_id,source_seq
        ):
            raise RuntimeError(
                "JOIN outbox commit was staged concurrently")
        for record in records:
            (
                record_table,lane,payload,
                nrows,logical_bytes
            )=record
            if record_table!=table:
                raise RuntimeError(
                    "JOIN bridge routed an unexpected table")
            cur=con.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,
                    logical_bytes,plan_version,
                    source_file,source_pos,source_seq,
                    source_time,created)
                VALUES(?,?,'cdc',?,?,?,?,NULL,NULL,?,?,?)
            """,(
                table,int(lane),bytes(payload),
                int(nrows),int(logical_bytes),
                int(stream["plan_version"]),
                source_seq,now,now,
            ))
            job_id=int(cur.lastrowid)
            con.execute("""
                INSERT INTO join_job_links(
                    job_id,consumer_id,source_seq)
                VALUES(?,?,?)
            """,(
                job_id,consumer_id,source_seq))
            job_ids.append(job_id)
            logical_total+=int(logical_bytes)
        j4.meta_set(
            con,"pending_bytes",
            j4.meta_get(
                con,"pending_bytes",0
            )+logical_total)
    return dict(
        consumer_id=consumer_id,
        source_seq=source_seq,
        visible=False,job_ids=job_ids)


def stage_pending(
        con,consumer_id,mapping,cfg,limit=100,engine=None
):
    result=[]
    for commit in join_outbox.pending_commits(
        con,consumer_id,limit=limit
    ):
        result.append(stage_commit(
            con,consumer_id,
            commit["source_seq"],
            mapping,cfg,engine=engine))
    return result
