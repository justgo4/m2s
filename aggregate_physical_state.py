#!/usr/bin/env python3
"""Physical-catalog adapter for the first aggregate state implementation.

The SQLite aggregate backend is current-only: after watermark N advances to
N+1 it does not retain an independently readable copy of N. Therefore the
physical catalog always records min_readable_watermark == watermark. Generic
catalog pins do not preserve backing bytes; reuse is allowed only through an
atomic SQLite clone that revalidates and copies the backing state under one
IMMEDIATE write transaction.
"""
import json

import aggregate_ir
import aggregate_state
import incremental_contract
import physical_state_catalog
import source_state


BACKEND="sqlite"
FORMAT_TAG="aggregate-current-v2"


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def instance_id(state_id):
    return "aggregate:v2:"+_text(state_id,"state_id")


def _relation_identity(info):
    return (
        _text(info["source_epoch"],"source_epoch")
        +"::"+_text(info["table_name"],"table_name")
    )


def _collation(ir):
    schema=ir["source"]["schema"]
    by_name={str(item[0]):item for item in schema}
    values=[]
    for name in ir["group_keys"]:
        item=by_name[name]
        value=(
            str(item[4]) if len(item)>4 and str(item[4]) not in {"","None"}
            else "binary"
        )
        values.append([name,value])
    return json.dumps(values,ensure_ascii=False,separators=(",",":"))


def physical_spec(con,ir):
    aggregate_ir.validate_ir(ir)
    relation=ir["source"]["relation"]
    relation_info=source_state.relation_info(con,relation)
    aggregates=[
        "%s(%s) AS %s" % (
            item["function"].upper(),item["input"],item["output"])
        for item in ir["aggregates"]
    ]
    predicate=" AND ".join(
        "(%s)" % value
        for value in (ir["source_filter"],ir["query_filter"])
        if value
    ) or "TRUE"
    return incremental_contract.state_spec(
        "materialized_subview",
        [_relation_identity(relation_info)],
        [relation_info["schema_epoch"]],
        key_exprs=list(ir["group_keys"]),
        value_exprs=aggregates+[
            "aggregate_ir:"+aggregate_ir.semantic_id(ir)
        ],
        predicate=predicate,
        collation=_collation(ir),
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
    aggregate_ir.validate_ir(ir)
    state_id=_text(state_id,"state_id")
    generation=int(generation)
    if generation<1:
        raise ValueError("physical aggregate generation must be >= 1")
    aggregate=aggregate_state.state_info(con,state_id)
    expected_ir=aggregate_ir.semantic_id(ir)
    if aggregate["input_semantic_id"]!=expected_ir:
        raise RuntimeError(
            "aggregate backing state is not bound to execution IR")
    spec=physical_spec(con,ir)
    iid=instance_id(state_id)
    metadata=dict(
        aggregate_state_id=state_id,
        aggregate_ir_id=expected_ir,
        version_model="current_only",
    )
    health="ready" if aggregate["bootstrap_complete"] else "building"
    _ensure_catalog(con)
    try:
        physical=physical_state_catalog.ensure_state(
            con,spec,BACKEND,FORMAT_TAG,aggregate["watermark"],
            min_readable_watermark=aggregate["watermark"],
            generation=generation,health=health,metadata=metadata,
            instance_id=iid)
    except RuntimeError:
        raise

    if physical["watermark"]>aggregate["watermark"]:
        raise RuntimeError(
            "physical aggregate catalog is ahead of backing state")
    if (
        physical["watermark"]<aggregate["watermark"]
        or physical["min_readable_watermark"]<aggregate["watermark"]
    ):
        physical=physical_state_catalog.advance_state(
            con,iid,aggregate["watermark"],
            min_readable_watermark=aggregate["watermark"])
    if physical["health"] in {"failed","retired"}:
        raise RuntimeError(
            "cannot revive terminal aggregate physical state")
    if physical["health"]!=health:
        physical=physical_state_catalog.set_health(con,iid,health)
    return physical


def clone_reusable_current(con,target_state_id,ir,watermark,generation=1):
    """Clone a semantically identical ready aggregate state at exact W.

    Selection and backing-state validation happen under the same SQLite
    IMMEDIATE transaction as the copy. This provides actual bytes-at-W reuse
    without pretending that a current-only physical-state pin is MVCC.
    """
    aggregate_ir.validate_ir(ir)
    target_state_id=_text(target_state_id,"target_state_id")
    watermark=int(watermark)
    generation=int(generation)
    if watermark<0:
        raise ValueError("aggregate clone watermark cannot be negative")
    if generation<1:
        raise ValueError("aggregate clone generation must be >= 1")
    _ensure_catalog(con)
    spec=physical_spec(con,ir)
    input_id=aggregate_ir.semantic_id(ir)

    with aggregate_state.transaction(con):
        try:
            aggregate_state.state_info(con,target_state_id)
        except KeyError:
            pass
        else:
            return None

        chosen=None
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
                or metadata.get("aggregate_ir_id")!=input_id
            ):
                continue
            source_state_id=str(
                metadata.get("aggregate_state_id") or "")
            if not source_state_id or source_state_id==target_state_id:
                continue
            try:
                backing=aggregate_state.state_info(
                    con,source_state_id)
            except KeyError:
                continue
            if (
                not backing["bootstrap_complete"]
                or int(backing["watermark"])!=watermark
                or backing["input_semantic_id"]!=input_id
                or backing["spec_hash"]!=aggregate_state.semantic_id(
                    aggregate_ir.state_spec(ir))
            ):
                continue
            chosen=(physical,source_state_id)
            break

        if chosen is None:
            return None
        source_physical,source_state_id=chosen
        cloned=aggregate_state.clone_complete_state(
            con,source_state_id,target_state_id,
            aggregate_ir.state_spec(ir),input_id,watermark)
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
        "current-only aggregate state cannot be safely retained by a logical "
        "physical pin; use clone_reusable_current() for atomic bytes-at-W reuse")
