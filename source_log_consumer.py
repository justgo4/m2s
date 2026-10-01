#!/usr/bin/env python3
"""Correctness-first source-log task consumer candidate for P8A.

This module deliberately does not replace j4.py's direct fan-out yet. It proves
one narrower contract: a task can consume the durable source commit sequence,
run the already-validated stateless incremental IR, and atomically persist both
its transformed outbox record and consumer watermark.

The transformed outbox is an intermediate journal, not a StarRocks delivery
protocol. A later daemon integration can translate it into the existing jobs /
lane machinery after exact dual-run and crash tests pass.
"""
import contextlib
import time

import duckdb
import pyarrow as pa

import incremental_ir
import relational_ir
import source_state


OUTBOX_ARROW_MAGIC = b"M2STASK1\0"
OUTBOX_WRITE_OPTIONS = pa.ipc.IpcWriteOptions(compression="zstd")


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
    con.executescript("""
        CREATE TABLE IF NOT EXISTS source_task_outbox(
            consumer_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL,
            plan_version INTEGER NOT NULL,
            source_file TEXT NOT NULL,
            source_pos INTEGER NOT NULL,
            relation_semantic_id TEXT NOT NULL,
            incremental_semantic_id TEXT NOT NULL,
            nrows INTEGER NOT NULL,
            payload BLOB,
            created REAL NOT NULL,
            PRIMARY KEY(consumer_id,source_seq));
        CREATE INDEX IF NOT EXISTS source_task_outbox_seq
            ON source_task_outbox(source_seq,consumer_id);
    """)


def _text(value, name):
    value = str(value or "").strip()
    if not value:
        raise ValueError(name + " must be non-empty")
    return value


def _metadata(source_relation, plan_version, rel, delta_ir):
    relational_ir.validate_ir(rel)
    incremental_ir.validate_ir(delta_ir)
    if delta_ir["relation_semantic_id"] != relational_ir.semantic_id(rel):
        raise ValueError("incremental IR does not match relational IR")
    return dict(
        source_relation=_text(source_relation,"source_relation"),
        plan_version=int(plan_version),
        relation_semantic_id=relational_ir.semantic_id(rel),
        incremental_semantic_id=incremental_ir.semantic_id(delta_ir),
    )


def ensure_consumer(
        con, consumer_id, source_relation, plan_version, rel, delta_ir,
        watermark
):
    install(con)
    consumer_id = _text(consumer_id,"consumer_id")
    metadata = _metadata(
        source_relation,plan_version,rel,delta_ir)
    try:
        current = source_state.consumer_info(con,consumer_id)
    except KeyError:
        return source_state.register_consumer(
            con,consumer_id,int(watermark),
            owner="task:"+consumer_id,metadata=metadata)
    if current["metadata"] != metadata:
        raise RuntimeError(
            "source-log task consumer semantic identity changed across restart")
    if int(current["watermark"]) != int(watermark):
        raise RuntimeError(
            "source-log task consumer exists at a different watermark; "
            "resume from its durable watermark")
    return current


def _encode_table(table):
    sink=pa.BufferOutputStream()
    sink.write(OUTBOX_ARROW_MAGIC)
    with pa.ipc.new_stream(
        sink,table.schema,options=OUTBOX_WRITE_OPTIONS
    ) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def decode_outbox(payload):
    if payload is None:
        return None
    view=memoryview(payload)
    if (
        len(view)<len(OUTBOX_ARROW_MAGIC)
        or view[:len(OUTBOX_ARROW_MAGIC)].tobytes()!=OUTBOX_ARROW_MAGIC
    ):
        raise ValueError("invalid source-task outbox Arrow payload")
    return pa.ipc.open_stream(
        pa.BufferReader(view[len(OUTBOX_ARROW_MAGIC):])
    ).read_all()


def transform_commit(commit, source_relation, rel, delta_ir, engine=None):
    source_relation=_text(source_relation,"source_relation")
    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    outputs=[]
    try:
        sql=incremental_ir.to_duckdb_sql(
            delta_ir,rel,input_name="_sync_raw")
        for part in commit.get("parts",()):
            if str(part["table_name"]) != source_relation:
                continue
            batch=source_state.decode_batch(part["payload"])
            if not batch.num_rows:
                continue
            engine.register("_sync_raw",batch)
            try:
                out=engine.execute(sql).fetch_arrow_table()
            finally:
                with contextlib.suppress(Exception):
                    engine.unregister("_sync_raw")
            if out.num_rows:
                outputs.append(out)
        if not outputs:
            return None
        if len(outputs)==1:
            return outputs[0]
        return pa.concat_tables(outputs,promote_options="none")
    finally:
        if own_engine:
            engine.close()


def _load_next_commit(con, watermark):
    applied=source_state.base_applied_seq(con)
    watermark=int(watermark)
    if watermark>applied:
        raise RuntimeError("task consumer watermark is ahead of applied source base")
    if watermark==applied:
        return None
    rows=source_state.read_commits(
        con,watermark,through_seq=applied,limit=1)
    if not rows:
        raise RuntimeError(
            "source changelog gap: applied base is ahead of consumer but "
            "next commit is unavailable")
    commit=rows[0]
    if int(commit["seq"]) != watermark+1:
        raise RuntimeError(
            "source changelog is not contiguous for task consumer "
            "expected=%d got=%d" % (watermark+1,int(commit["seq"])))
    return commit


def process_next(
        con, consumer_id, rel, delta_ir, engine=None,
        fault_after_outbox=None
):
    """Consume at most one applied source transaction.

    Outbox insert and source_consumers.watermark update commit atomically.
    fault_after_outbox exists only for deterministic crash-boundary tests.
    """
    consumer_id=_text(consumer_id,"consumer_id")
    current=source_state.consumer_info(con,consumer_id)
    metadata=current["metadata"]
    expected=_metadata(
        metadata.get("source_relation"),
        metadata.get("plan_version"),
        rel,delta_ir)
    if metadata != expected:
        raise RuntimeError(
            "source-log task consumer metadata differs from execution plan")
    commit=_load_next_commit(con,current["watermark"])
    if commit is None:
        return None

    transformed=transform_commit(
        commit,metadata["source_relation"],rel,delta_ir,engine=engine)
    payload=None if transformed is None else _encode_table(transformed)
    nrows=0 if transformed is None else int(transformed.num_rows)
    seq=int(commit["seq"])

    with transaction(con):
        durable=source_state.consumer_info(con,consumer_id)
        if int(durable["watermark"]) != int(current["watermark"]):
            raise RuntimeError(
                "source-log task consumer advanced concurrently; retry from "
                "the new durable watermark")
        if source_state.base_applied_seq(con) < seq:
            raise RuntimeError(
                "source base regressed behind transformed task commit")
        row=con.execute(
            "SELECT seq FROM source_commits WHERE seq=?",(seq,)
        ).fetchone()
        if not row:
            raise RuntimeError(
                "source commit disappeared before task outbox commit")
        con.execute("""
            INSERT INTO source_task_outbox(
                consumer_id,source_seq,plan_version,
                source_file,source_pos,
                relation_semantic_id,incremental_semantic_id,
                nrows,payload,created)
            VALUES(?,?,?,?,?,?,?,?,?,?)
        """,(
            consumer_id,seq,int(metadata["plan_version"]),
            str(commit["position"][0]),int(commit["position"][1]),
            metadata["relation_semantic_id"],
            metadata["incremental_semantic_id"],
            nrows,payload,time.time(),
        ))
        if fault_after_outbox is not None:
            fault_after_outbox(seq)
        changed=con.execute("""
            UPDATE source_consumers
            SET watermark=?,updated=?
            WHERE consumer_id=? AND watermark=?
        """,(
            seq,time.time(),consumer_id,int(current["watermark"])
        )).rowcount
        if int(changed) != 1:
            raise RuntimeError(
                "source-log task consumer watermark CAS failed")
    return dict(
        consumer_id=consumer_id,source_seq=seq,nrows=nrows,
        source_file=str(commit["position"][0]),
        source_pos=int(commit["position"][1]))


def outbox_rows(con, consumer_id):
    consumer_id=_text(consumer_id,"consumer_id")
    result=[]
    for seq,plan_version,source_file,source_pos,nrows,payload in con.execute("""
        SELECT source_seq,plan_version,source_file,source_pos,nrows,payload
        FROM source_task_outbox
        WHERE consumer_id=? ORDER BY source_seq
    """,(consumer_id,)):
        result.append(dict(
            source_seq=int(seq),plan_version=int(plan_version),
            position=(str(source_file),int(source_pos)),
            nrows=int(nrows),
            table=decode_outbox(payload)))
    return result
