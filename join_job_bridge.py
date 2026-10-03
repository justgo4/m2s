#!/usr/bin/env python3
"""Bridge INNER JOIN outbox commits into the proven j4 durable job pipeline.

JOIN bag semantics require a stable row identity even when projected values are
identical. The bridge therefore materializes the stream-versioned durable target identity
as an internal text column named _j4_pair_id. Writer mappings for JOIN targets
must use that column as their sole Primary Key.
"""
import pickle
import hashlib
import tempfile
import time

import duckdb

import j4
import join_outbox
import stateful_task_plan
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


def _mutation_batches(con,consumer_id,source_seq,mapping,cfg):
    validate_mapping(mapping)
    row_limit=max(1,min(int(cfg.get("batch_rows",4096)),4096))
    byte_limit=max(1,int(cfg["batch_bytes"]))
    cursor=con.execute("""
        SELECT pair_id,op,row_payload FROM join_output_rows
        WHERE consumer_id=? AND source_seq=? ORDER BY pair_id
    """,(consumer_id,source_seq))
    rows=[]
    size=0
    try:
        for pair_id,op,payload in cursor:
            identity=join_outbox.target_id_for_pair(con,consumer_id,pair_id)
            row_bytes=len(payload)+len(identity.encode("utf-8"))
            if rows and (len(rows)>=row_limit or size+row_bytes>byte_limit):
                yield rows
                rows=[]
                size=0
            row=pickle.loads(payload)
            row[PAIR_COLUMN]=identity
            rows.append((int(op),row))
            size+=row_bytes
        if rows:
            yield rows
    finally:
        cursor.close()


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


def _staging_info(con,consumer_id,source_seq):
    return con.execute("""
        SELECT spool_digest,record_count,next_record,spool_offset,sealed
        FROM join_job_staging WHERE consumer_id=? AND source_seq=?
    """,(consumer_id,source_seq)).fetchone()


def _validate_spool(spool,table,cfg):
    # Validate before making any durable partial jobs; read in bounded buffers.
    count=0
    spool.seek(0)
    while True:
        record=j4.read_spool_record(spool)
        if record is None:
            break
        name,lane,payload,nrows,logical_bytes=record
        if (name!=table or not 0<=int(lane)<j4.key_partition_count(cfg)
                or int(nrows)<=0 or int(logical_bytes)<0):
            raise RuntimeError("JOIN bridge routed an invalid durable job")
        count+=1
    digest=hashlib.sha256()
    spool.seek(0)
    while True:
        block=spool.read(1024**2)
        if not block:
            break
        digest.update(block)
    spool.seek(0)
    return digest.hexdigest(),count


def _register_chunk(con,consumer_id,source_seq,stream,records,expected,end_offset,done):
    with j4.state_transaction(con):
        current=_staging_info(con,consumer_id,source_seq)
        if current!=expected:
            return False
        logical_total=0
        now=time.time()
        for table,lane,payload,nrows,logical_bytes in records:
            cur=con.execute("""
                INSERT INTO jobs(
                    table_name,lane,kind,payload,nrows,logical_bytes,plan_version,
                    source_file,source_pos,source_seq,source_time,created)
                VALUES(?,?,'cdc',?,?,?,?,NULL,NULL,?,?,?)
            """,(table,int(lane),bytes(payload),int(nrows),int(logical_bytes),
                  stateful_task_plan.writer_plan_version(stream["plan_version"]),
                  source_seq,now,now))
            con.execute("""
                INSERT INTO join_job_links(job_id,consumer_id,source_seq)
                VALUES(?,?,?)
            """,(int(cur.lastrowid),consumer_id,source_seq))
            logical_total+=int(logical_bytes)
        j4.meta_set(con,"pending_bytes",j4.meta_get(con,"pending_bytes",0)+logical_total)
        con.execute("""
            UPDATE join_job_staging SET next_record=?,spool_offset=?,sealed=?,updated=?
            WHERE consumer_id=? AND source_seq=?
        """,(int(expected[2])+len(records),end_offset,int(done),now,consumer_id,source_seq))
    return True


def stage_commit(
        con,consumer_id,source_seq,mapping,cfg,engine=None
):
    consumer_id=_text(consumer_id,"consumer_id")
    source_seq=int(source_seq)
    validate_mapping(mapping)
    if con.in_transaction:
        raise RuntimeError("JOIN job staging requires independent bounded transactions")
    commit=join_outbox.commit_info(
        con,consumer_id,source_seq)
    if not commit["sealed"]:
        raise RuntimeError("cannot stage unsealed JOIN output")
    if commit["visible"]:
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=True,job_ids=[])

    existing=_already_staged(
        con,consumer_id,source_seq)
    staging=_staging_info(con,consumer_id,source_seq)
    if existing and (staging is None or staging[4]):
        return dict(
            consumer_id=consumer_id,
            source_seq=source_seq,
            visible=False,job_ids=existing)

    if con.execute("""
        SELECT 1 FROM join_job_staging
        WHERE consumer_id=? AND source_seq<? AND sealed=0 LIMIT 1
    """,(consumer_id,source_seq)).fetchone():
        raise RuntimeError("earlier JOIN output jobs are not sealed")

    if int(commit["nrows"])==0:
        if con.execute("""
            SELECT 1 FROM join_output_rows WHERE consumer_id=? AND source_seq=? LIMIT 1
        """,(consumer_id,source_seq)).fetchone():
            raise RuntimeError("empty JOIN output commit contains rows")
        join_outbox.mark_visible(con,consumer_id,source_seq)
        return dict(consumer_id=consumer_id,source_seq=source_seq,visible=True,job_ids=[])

    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    try:
        with tempfile.SpooledTemporaryFile(
            max_size=1024**2,dir=j4.state_temp_dir(cfg)
        ) as spool:
            total_rows=0
            for mutations in _mutation_batches(con,consumer_id,source_seq,mapping,cfg):
                raw=j4.raw_arrow(mapping,mutations)
                routed=j4.route_arrow(engine,mapping,raw,j4.key_partition_count(cfg))
                j4.spool_routed(spool,mapping,routed,cfg)
                total_rows+=len(mutations)
                # Keep only the current batch, not the completed Arrow buffers.
                del mutations,raw,routed
            if total_rows!=int(commit["nrows"]):
                raise RuntimeError("JOIN output commit row count is inconsistent")
            spool.seek(0)
            stream=join_outbox.stream_info(con,consumer_id)
            table=j4.mapping_key(mapping)
            spool_digest,record_count=_validate_spool(spool,table,cfg)
            if not record_count:
                raise RuntimeError("JOIN output rows produced no routed durable jobs")
            with j4.state_transaction(con):
                if con.execute("""
                    SELECT 1 FROM join_job_staging
                    WHERE consumer_id=? AND source_seq<? AND sealed=0 LIMIT 1
                """,(consumer_id,source_seq)).fetchone():
                    raise RuntimeError("earlier JOIN output jobs are not sealed")
                current=_staging_info(con,consumer_id,source_seq)
                if current is None:
                    if _already_staged(con,consumer_id,source_seq):
                        return dict(consumer_id=consumer_id,source_seq=source_seq,visible=False,
                                    job_ids=_already_staged(con,consumer_id,source_seq))
                    con.execute("""
                        INSERT INTO join_job_staging(
                            consumer_id,source_seq,spool_digest,record_count,updated)
                        VALUES(?,?,?,?,?)
                    """,(consumer_id,source_seq,spool_digest,record_count,time.time()))
            # A sealed manifest publishes all chunks through active_jobs without
            # rewriting their payloads. Recreated spools must match byte-exactly.
            job_limit=max(1,min(int(cfg.get("join_enqueue_jobs",32)),32))
            byte_limit=max(1,min(int(cfg.get("join_enqueue_bytes",16*1024**2)),16*1024**2))
            while True:
                current=_staging_info(con,consumer_id,source_seq)
                if current[:2]!=(spool_digest,record_count):
                    raise RuntimeError("JOIN staging spool changed across restart")
                if current[4]:
                    break
                spool.seek(int(current[3]))
                records=[]
                size=0
                while True:
                    position=spool.tell()
                    record=j4.read_spool_record(spool)
                    if record is None:
                        break
                    if records and (len(records)>=job_limit or size+len(record[2])>byte_limit):
                        spool.seek(position)
                        break
                    records.append(record)
                    size+=len(record[2])
                done=int(current[2])+len(records)==record_count
                if not records and not done:
                    raise RuntimeError("JOIN staging cursor did not advance")
                _register_chunk(con,consumer_id,source_seq,stream,records,current,spool.tell(),done)
            job_ids=_already_staged(con,consumer_id,source_seq)
            return dict(consumer_id=consumer_id,source_seq=source_seq,visible=False,job_ids=job_ids)
    finally:
        if own_engine:
            engine.close()


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
