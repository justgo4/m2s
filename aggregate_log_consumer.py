#!/usr/bin/env python3
"""Atomic source-log consumer for the first GROUP BY aggregate candidate.

This module is not a daemon/catalog execution path yet. It proves that one
source commit can be transformed through aggregate_ir and committed atomically
to aggregate_state plus the durable source consumer watermark.
"""
import contextlib
import time

import duckdb

import aggregate_ir
import aggregate_outbox
import aggregate_physical_state
import aggregate_state
import source_state


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _metadata(source_relation,plan_version,ir,state_id):
    aggregate_ir.validate_ir(ir)
    return dict(
        kind="group_aggregate_v1",
        source_relation=_text(source_relation,"source_relation"),
        plan_version=int(plan_version),
        aggregate_ir_id=aggregate_ir.semantic_id(ir),
        aggregate_state_id=_text(state_id,"aggregate_state_id"),
        aggregate_state_spec_id=aggregate_state.semantic_id(
            aggregate_ir.state_spec(ir)),
    )


def ensure_consumer(
        con,consumer_id,source_relation,plan_version,ir,state_id,watermark
):
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    watermark=int(watermark)
    spec=aggregate_ir.state_spec(ir)
    state=aggregate_state.ensure_state(con,state_id,spec,watermark)
    state=aggregate_state.bind_input_semantics(
        con,state_id,aggregate_ir.semantic_id(ir))
    aggregate_physical_state.sync_instance(
        con,state_id,ir,generation=1)
    aggregate_outbox.install(con)
    generation_id="aggregate:%s:plan:%d" % (
        consumer_id,int(plan_version))
    aggregate_outbox.ensure_stream(
        con,consumer_id,state_id,int(plan_version),
        generation_id,watermark)
    aggregate_outbox.seed_bootstrap(
        con,consumer_id,state_id,int(plan_version),
        generation_id,watermark)
    if int(state["watermark"])!=watermark:
        raise RuntimeError(
            "aggregate state exists at a different watermark; resume from its "
            "durable watermark")
    metadata=_metadata(source_relation,plan_version,ir,state_id)
    try:
        current=source_state.consumer_info(con,consumer_id)
    except KeyError:
        current=source_state.register_consumer(
            con,consumer_id,watermark,
            owner="aggregate:"+consumer_id,metadata=metadata)
    if current["metadata"]!=metadata:
        raise RuntimeError(
            "aggregate source consumer semantic identity changed across restart")
    if int(current["watermark"])!=watermark:
        raise RuntimeError(
            "aggregate source consumer exists at a different watermark")
    return current


def _load_next(con,watermark):
    watermark=int(watermark)
    applied=source_state.base_applied_seq(con)
    if watermark>applied:
        raise RuntimeError(
            "aggregate consumer watermark is ahead of applied source base")
    if watermark==applied:
        return None
    rows=source_state.read_commits(
        con,watermark,through_seq=applied,limit=1)
    if not rows:
        raise RuntimeError(
            "source changelog gap before aggregate consumer")
    commit=rows[0]
    if int(commit["seq"])!=watermark+1:
        raise RuntimeError(
            "aggregate consumer source sequence is not contiguous "
            "expected=%d got=%d" % (watermark+1,int(commit["seq"])))
    return commit


def transform_commit(commit,source_relation,ir,engine=None):
    source_relation=_text(source_relation,"source_relation")
    aggregate_ir.validate_ir(ir)
    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    sql=aggregate_ir.to_duckdb_input_sql(ir,"_sync_raw")
    changes=[]
    try:
        for part in commit.get("parts",()):
            if str(part["table_name"])!=source_relation:
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
                changes.extend(out.to_pylist())
    finally:
        if own_engine:
            engine.close()
    return changes


def process_next(
        con,consumer_id,ir,engine=None,fault_after_state=None
):
    consumer_id=_text(consumer_id,"consumer_id")
    current=source_state.consumer_info(con,consumer_id)
    metadata=current["metadata"]
    state_id=_text(metadata.get("aggregate_state_id"),"aggregate_state_id")
    expected=_metadata(
        metadata.get("source_relation"),
        metadata.get("plan_version"),
        ir,state_id)
    if metadata!=expected:
        raise RuntimeError(
            "aggregate consumer metadata differs from execution IR")
    state=aggregate_state.state_info(con,state_id)
    if int(state["watermark"])!=int(current["watermark"]):
        raise RuntimeError(
            "aggregate state/consumer watermarks diverged before consume")
    commit=_load_next(con,current["watermark"])
    if commit is None:
        return None
    seq=int(commit["seq"])
    changes=transform_commit(
        commit,metadata["source_relation"],ir,engine=engine)

    con.execute("BEGIN IMMEDIATE")
    try:
        durable=source_state.consumer_info(con,consumer_id)
        state=aggregate_state.state_info(con,state_id)
        if (
            int(durable["watermark"])!=int(current["watermark"])
            or int(state["watermark"])!=int(current["watermark"])
        ):
            raise RuntimeError(
                "aggregate state/consumer advanced concurrently")
        if source_state.base_applied_seq(con)<seq:
            raise RuntimeError(
                "applied source base regressed before aggregate commit")
        if not con.execute(
            "SELECT 1 FROM source_commits WHERE seq=?",(seq,)
        ).fetchone():
            raise RuntimeError(
                "source commit disappeared before aggregate commit")

        changed=aggregate_state.apply_transaction(
            con,state_id,seq,changes)
        if not changed:
            raise RuntimeError(
                "aggregate state unexpectedly treated next source seq as retry")
        if fault_after_state is not None:
            fault_after_state(seq)
        aggregate_physical_state.sync_instance(
            con,state_id,ir,generation=1)
        aggregate_outbox.enqueue_incremental(
            con,consumer_id,state_id,seq,changes)
        updated=con.execute("""
            UPDATE source_consumers
            SET watermark=?,updated=?
            WHERE consumer_id=? AND watermark=?
        """,(
            seq,time.time(),consumer_id,int(current["watermark"])
        )).rowcount
        if int(updated)!=1:
            raise RuntimeError(
                "aggregate consumer watermark CAS failed")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return dict(
        consumer_id=consumer_id,state_id=state_id,
        source_seq=seq,nrows=len(changes),
        position=(
            str(commit["position"][0]),int(commit["position"][1])),
    )


def state_rows(con,consumer_id):
    consumer=source_state.consumer_info(con,_text(consumer_id,"consumer_id"))
    state_id=_text(
        consumer["metadata"].get("aggregate_state_id"),
        "aggregate_state_id")
    state=aggregate_state.state_info(con,state_id)
    if int(state["watermark"])!=int(consumer["watermark"]):
        raise RuntimeError(
            "aggregate state/consumer watermarks diverged")
    return aggregate_state.read_rows(con,state_id)
