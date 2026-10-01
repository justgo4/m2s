#!/usr/bin/env python3
"""Atomic source-log consumer for the first INNER JOIN candidate.

The source consumer watermark and durable JOIN state watermark advance in the
same SQLite transaction. Every global source_seq is consumed, including commits
that touch neither JOIN input, so source_state retention can use one contiguous
consumer frontier.
"""
import time

import join_ir
import join_outbox
import join_state
import source_state


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _metadata(plan_version,ir,state_id,generation_id):
    join_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    generation_id=_text(generation_id,"generation_id")
    return dict(
        kind="inner_join_v1",
        plan_version=int(plan_version),
        generation_id=generation_id,
        join_ir_id=join_ir.semantic_id(ir),
        join_state_id=state_id,
        join_state_spec_id=join_state.semantic_id(
            join_ir.state_spec(ir)),
        left_relation=str(ir["sources"]["left"]["relation"]),
        right_relation=str(ir["sources"]["right"]["relation"]),
    )


def ensure_consumer(
        con,consumer_id,plan_version,ir,state_id,watermark,
        generation_id=None
):
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    watermark=int(watermark)
    generation_id=(
        "join:%s:plan:%d" % (consumer_id,int(plan_version))
        if generation_id is None
        else _text(generation_id,"generation_id"))
    state=join_state.state_info(con,state_id)
    if not state["bootstrap_complete"]:
        raise RuntimeError(
            "cannot create JOIN consumer before fixed-W bootstrap completes")
    if int(state["watermark"])!=watermark:
        raise RuntimeError(
            "JOIN state and requested consumer watermark differ")
    if state["spec_hash"]!=join_state.semantic_id(
        join_ir.state_spec(ir)
    ):
        raise RuntimeError(
            "JOIN state semantics differ from consumer IR")
    join_outbox.ensure_installed(con)
    join_outbox.ensure_stream(
        con,consumer_id,state_id,int(plan_version),
        generation_id,watermark)
    join_outbox.seed_bootstrap(
        con,consumer_id,state_id,int(plan_version),
        generation_id,watermark)
    metadata=_metadata(
        plan_version,ir,state_id,generation_id)
    try:
        current=source_state.consumer_info(
            con,consumer_id)
    except KeyError:
        current=source_state.register_consumer(
            con,consumer_id,watermark,
            owner="join:"+consumer_id,metadata=metadata)
    if current["metadata"]!=metadata:
        raise RuntimeError(
            "JOIN source consumer semantic identity changed across restart")
    if int(current["watermark"])!=watermark:
        raise RuntimeError(
            "JOIN source consumer exists at a different watermark")
    return current


def _load_next(con,watermark):
    watermark=int(watermark)
    applied=source_state.base_applied_seq(con)
    if watermark>applied:
        raise RuntimeError(
            "JOIN consumer watermark is ahead of applied source base")
    if watermark==applied:
        return None
    rows=source_state.read_commits(
        con,watermark,through_seq=applied,limit=1)
    if not rows:
        raise RuntimeError(
            "source changelog gap before JOIN consumer")
    commit=rows[0]
    if int(commit["seq"])!=watermark+1:
        raise RuntimeError(
            "JOIN source sequence is not contiguous expected=%d got=%d"
            % (watermark+1,int(commit["seq"])))
    return commit


def transform_commit(commit,ir):
    join_ir.validate_ir(ir)
    relations={
        str(ir["sources"]["left"]["relation"]):"left",
        str(ir["sources"]["right"]["relation"]):"right",
    }
    changes=[]
    for part in commit.get("parts",()):
        side=relations.get(str(part["table_name"]))
        if side is None:
            continue
        batch=source_state.decode_batch(
            part["payload"])
        if not batch.num_rows:
            continue
        for row in batch.to_pylist():
            changes.append((side,row))
    return changes


def process_next(
        con,consumer_id,ir,fault_after_state=None
):
    consumer_id=_text(consumer_id,"consumer_id")
    current=source_state.consumer_info(
        con,consumer_id)
    metadata=current["metadata"]
    state_id=_text(
        metadata.get("join_state_id"),
        "join_state_id")
    expected=_metadata(
        metadata.get("plan_version"),ir,state_id,
        metadata.get("generation_id"))
    if metadata!=expected:
        raise RuntimeError(
            "JOIN consumer metadata differs from execution IR")
    state=join_state.state_info(
        con,state_id)
    if int(state["watermark"])!=int(current["watermark"]):
        raise RuntimeError(
            "JOIN state/consumer watermarks diverged before consume")
    commit=_load_next(
        con,current["watermark"])
    if commit is None:
        return None
    seq=int(commit["seq"])
    changes=transform_commit(commit,ir)

    con.execute("BEGIN IMMEDIATE")
    try:
        durable=source_state.consumer_info(
            con,consumer_id)
        state=join_state.state_info(
            con,state_id)
        if (
            int(durable["watermark"])!=int(current["watermark"])
            or int(state["watermark"])!=int(current["watermark"])
        ):
            raise RuntimeError(
                "JOIN state/consumer advanced concurrently")
        if source_state.base_applied_seq(con)<seq:
            raise RuntimeError(
                "applied source base regressed before JOIN commit")
        if not con.execute(
            "SELECT 1 FROM source_commits WHERE seq=?",
            (seq,)
        ).fetchone():
            raise RuntimeError(
                "source commit disappeared before JOIN commit")

        applied=join_state.apply_transaction(
            con,state_id,seq,changes,
            fault_after_rows=(
                None if fault_after_state is None
                else lambda _seq: fault_after_state(seq)
            ))
        if not applied["applied"]:
            raise RuntimeError(
                "JOIN state unexpectedly treated next source seq as retry")
        output_commit=join_outbox.enqueue_incremental(
            con,consumer_id,state_id,seq,
            applied["deltas"])
        updated=con.execute("""
            UPDATE source_consumers
            SET watermark=?,updated=?
            WHERE consumer_id=? AND watermark=?
        """,(
            seq,time.time(),consumer_id,
            int(current["watermark"]),
        )).rowcount
        if int(updated)!=1:
            raise RuntimeError(
                "JOIN consumer watermark CAS failed")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return dict(
        consumer_id=consumer_id,
        state_id=state_id,
        source_seq=seq,
        nchanges=len(changes),
        deltas=applied["deltas"],
        output_commit=output_commit,
        position=(
            str(commit["position"][0]),
            int(commit["position"][1]),
        ),
    )
