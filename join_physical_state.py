#!/usr/bin/env python3
"""Physical-catalog adapter for the correctness-first INNER JOIN state.

The current SQLite JOIN backend is current-only.  It exposes one exact readable
watermark at a time, so physical catalog min_readable_watermark tracks the live
watermark and any fixed-W reuse pin blocks advancement until an MVCC backend is
introduced.
"""
import json

import incremental_contract
import join_ir
import join_state
import physical_state_catalog
import source_state


BACKEND="sqlite"
FORMAT_TAG="join-current-v1"


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def instance_id(state_id):
    return "join:"+_text(state_id,"state_id")


def _relation_identity(info):
    return (
        _text(info["source_epoch"],"source_epoch")
        +"::"+_text(info["table_name"],"table_name")
    )


def physical_spec(con,ir):
    join_ir.validate_ir(ir)
    left=source_state.relation_info(
        con,ir["sources"]["left"]["relation"])
    right=source_state.relation_info(
        con,ir["sources"]["right"]["relation"])
    key_exprs=[
        "left.%s=right.%s" % (
            pair["left"],pair["right"])
        for pair in ir["join_pairs"]
    ]
    values=[
        "%s.%s AS %s" % (
            item["source"],item["column"],item["output"])
        for item in ir["projections"]
    ]
    predicate=" AND ".join(key_exprs)
    collation=json.dumps(
        [left["schema_hash"],right["schema_hash"]],
        ensure_ascii=False,separators=(",",":"))
    return incremental_contract.state_spec(
        "arrangement",
        [_relation_identity(left),_relation_identity(right)],
        [left["schema_epoch"],right["schema_epoch"]],
        key_exprs=key_exprs,
        value_exprs=values+[
            "join_ir:"+join_ir.semantic_id(ir)
        ],
        predicate=predicate,
        collation=collation,
        semantics_version=1,
    )


def _ensure_catalog(con):
    exists=con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='physical_states'
    """).fetchone()
    if exists:
        return
    if con.in_transaction:
        raise RuntimeError(
            "physical state catalog must be installed before transactional sync")
    physical_state_catalog.install(con)


def sync_instance(con,state_id,ir,generation=1):
    join_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    generation=int(generation)
    if generation<1:
        raise ValueError("physical JOIN generation must be >= 1")
    current=join_state.state_info(con,state_id)
    expected=join_state.semantic_id(
        join_ir.state_spec(ir))
    if current["spec_hash"]!=expected:
        raise RuntimeError(
            "JOIN backing state is not bound to execution IR")
    spec=physical_spec(con,ir)
    iid=instance_id(state_id)
    metadata=dict(
        join_state_id=state_id,
        join_ir_id=join_ir.semantic_id(ir),
        version_model="current_only",
    )
    health="ready" if current["bootstrap_complete"] else "building"
    _ensure_catalog(con)
    physical=physical_state_catalog.ensure_state(
        con,spec,BACKEND,FORMAT_TAG,current["watermark"],
        min_readable_watermark=current["watermark"],
        generation=generation,health=health,metadata=metadata,
        instance_id=iid)
    if physical["watermark"]>current["watermark"]:
        raise RuntimeError(
            "physical JOIN catalog is ahead of backing state")
    if (
        physical["watermark"]<current["watermark"]
        or physical["min_readable_watermark"]<current["watermark"]
    ):
        physical=physical_state_catalog.advance_state(
            con,iid,current["watermark"],
            min_readable_watermark=current["watermark"])
    if physical["health"] in {"failed","retired"}:
        raise RuntimeError(
            "cannot revive terminal JOIN physical state")
    if physical["health"]!=health:
        physical=physical_state_catalog.set_health(
            con,iid,health)
    return physical


def acquire_current(con,state_id,ir,owner,role="consumer",generation=1):
    physical=sync_instance(
        con,state_id,ir,generation=generation)
    return physical_state_catalog.acquire_reusable_state(
        con,physical["spec"],owner,physical["watermark"],
        backend=BACKEND,format_tag=FORMAT_TAG,role=role)
