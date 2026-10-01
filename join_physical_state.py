#!/usr/bin/env python3
"""Physical-catalog adapter for the correctness-first INNER JOIN state.

The current SQLite JOIN backend is current-only. It exposes one exact readable
watermark at a time. Generic catalog pins do not preserve backing bytes; reuse
is therefore allowed only through an atomic SQLite clone that revalidates and
copies the backing state under one IMMEDIATE write transaction.
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


def clone_reusable_current(con,target_state_id,ir,watermark,generation=1):
    join_ir.validate_ir(ir)
    target_state_id=_text(
        target_state_id,"target_state_id")
    watermark=int(watermark)
    generation=int(generation)
    if watermark<0:
        raise ValueError("JOIN clone watermark cannot be negative")
    if generation<1:
        raise ValueError("JOIN clone generation must be >= 1")
    _ensure_catalog(con)
    spec=physical_spec(con,ir)
    ir_id=join_ir.semantic_id(ir)

    with join_state.transaction(con):
        try:
            join_state.state_info(
                con,target_state_id)
        except KeyError:
            pass
        else:
            return None

        chosen=None
        expected_state_id=join_state.semantic_id(
            join_ir.state_spec(ir))
        for physical in physical_state_catalog.find_semantic(
            con,spec
        ):
            if not physical_state_catalog.physically_reusable(
                physical,BACKEND,FORMAT_TAG
            ):
                continue
            if (
                int(physical["watermark"])!=watermark
                or int(physical["min_readable_watermark"])!=watermark
            ):
                continue
            metadata=physical["metadata"]
            if (
                metadata.get("version_model")!="current_only"
                or metadata.get("join_ir_id")!=ir_id
            ):
                continue
            source_state_id=str(
                metadata.get("join_state_id") or "")
            if not source_state_id or source_state_id==target_state_id:
                continue
            try:
                backing=join_state.state_info(
                    con,source_state_id)
            except KeyError:
                continue
            if (
                not backing["bootstrap_complete"]
                or not backing["left_complete"]
                or not backing["right_complete"]
                or int(backing["watermark"])!=watermark
                or backing["spec_hash"]!=expected_state_id
            ):
                continue
            chosen=(physical,source_state_id)
            break

        if chosen is None:
            return None
        source_physical,source_state_id=chosen
        cloned=join_state.clone_complete_state(
            con,source_state_id,target_state_id,
            join_ir.state_spec(ir),watermark)
        target_physical=sync_instance(
            con,target_state_id,ir,generation=generation)
        return dict(
            source_physical=source_physical,
            source_state_id=source_state_id,
            state=cloned,
            physical=target_physical,
        )


def acquire_current(con,state_id,ir,owner,role="consumer",generation=1):
    raise RuntimeError(
        "current-only JOIN state cannot be safely retained by a logical "
        "physical pin; use clone_reusable_current() for atomic bytes-at-W reuse")
