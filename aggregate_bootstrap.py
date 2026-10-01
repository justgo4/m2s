#!/usr/bin/env python3
"""Fixed-W bootstrap for the first durable aggregate candidate.

This module keeps dynamic GROUP BY off the daemon/catalog path until the
bootstrap/catch-up contract is proven. A pinned S(W) is scanned in resumable
chunks into aggregate_state; only after completion is a source consumer created
at W and the source pin released.
"""
import contextlib

import duckdb

import aggregate_ir
import aggregate_log_consumer
import aggregate_state
import source_state


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _pin_watermark(con,pin_id):
    return source_state.pin_watermark(con,_text(pin_id,"pin_id"))


def ensure_build(con,state_id,ir,pin_id):
    aggregate_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    pin_id=_text(pin_id,"pin_id")
    fixed_w=_pin_watermark(con,pin_id)
    relation=ir["source"]["relation"]
    relation_info=source_state.relation_info(con,relation)
    if (
        relation_info["complete_seq"] is None
        or int(relation_info["complete_seq"])>fixed_w
    ):
        raise RuntimeError(
            "aggregate fixed-W predates complete source relation")
    return aggregate_state.begin_bootstrap(
        con,state_id,aggregate_ir.state_spec(ir),fixed_w)


def _transform_snapshot(table,ir,engine=None):
    own_engine=engine is None
    engine=duckdb.connect(":memory:") if own_engine else engine
    try:
        if not table.num_rows:
            return []
        engine.register("_sync_raw",table)
        try:
            out=engine.execute(
                aggregate_ir.to_duckdb_input_sql(
                    ir,"_sync_raw")).fetch_arrow_table()
        finally:
            with contextlib.suppress(Exception):
                engine.unregister("_sync_raw")
        return out.to_pylist()
    finally:
        if own_engine:
            engine.close()


def process_next_chunk(
        con,state_id,ir,pin_id,limit=1000,engine=None,
        fault_after_changes=None
):
    aggregate_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    pin_id=_text(pin_id,"pin_id")
    limit=max(1,int(limit))
    current=aggregate_state.state_info(con,state_id)
    fixed_w=_pin_watermark(con,pin_id)
    if current["watermark"]!=fixed_w:
        raise RuntimeError(
            "aggregate bootstrap state and source pin fixed-W differ")
    if current["spec_hash"]!=aggregate_state.semantic_id(
        aggregate_ir.state_spec(ir)
    ):
        raise RuntimeError(
            "aggregate bootstrap IR differs from durable state")
    if current["bootstrap_complete"]:
        return dict(
            state_id=state_id,fixed_w=fixed_w,done=True,nrows=0,
            cursor=current["bootstrap_cursor"])

    table,next_cursor=source_state.read_snapshot_batch(
        con,pin_id,ir["source"]["relation"],
        after_key=current["bootstrap_cursor"],limit=limit)
    is_last=int(table.num_rows)<limit
    changes=_transform_snapshot(table,ir,engine=engine)
    aggregate_state.apply_bootstrap_chunk(
        con,state_id,fixed_w,changes,next_cursor,is_last,
        fault_after_changes=fault_after_changes)
    updated=aggregate_state.state_info(con,state_id)
    return dict(
        state_id=state_id,fixed_w=fixed_w,
        done=updated["bootstrap_complete"],
        nrows=len(changes),cursor=updated["bootstrap_cursor"])


def activate_consumer(
        con,consumer_id,source_relation,plan_version,ir,state_id,pin_id
):
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    pin_id=_text(pin_id,"pin_id")
    current=aggregate_state.state_info(con,state_id)
    if not current["bootstrap_complete"]:
        raise RuntimeError(
            "cannot activate aggregate consumer before fixed-W bootstrap")
    fixed_w=_pin_watermark(con,pin_id)
    if current["watermark"]!=fixed_w:
        raise RuntimeError(
            "aggregate state/pin fixed-W differ during activation")
    consumer=aggregate_log_consumer.ensure_consumer(
        con,consumer_id,source_relation,plan_version,
        ir,state_id,fixed_w)
    # Registration is durable before release. A crash between these two steps
    # only retains extra history; restart repeats ensure_consumer then releases.
    source_state.release_pin(con,pin_id)
    return consumer
