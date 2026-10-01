#!/usr/bin/env python3
"""Bridge live aggregate/JOIN task state into the reusable physical-state catalog.

This is the first P9 runtime integration: it does not yet choose shared plans.
It publishes only state that has a complete semantic identity, a readable
watermark and a healthy durable task generation, so future reuse cannot infer
compatibility from table names alone.
"""
import json

import aggregate_ir
import incremental_contract
import join_ir
import physical_state_catalog
import source_state


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _source_identity(info):
    return (
        _text(info["source_epoch"],"source_epoch")
        +"::"+_text(info["table_name"],"table_name")
    )


def _relation_infos(con,task):
    ir=task["ir"]
    if task.get("kind")=="aggregate":
        relations=[str(ir["source"]["relation"])]
    elif task.get("kind")=="inner_join":
        relations=[
            str(ir["sources"]["left"]["relation"]),
            str(ir["sources"]["right"]["relation"]),
        ]
    else:
        raise ValueError(
            "unsupported stateful physical task kind: "
            +str(task.get("kind")))
    return [
        source_state.relation_info(con,relation)
        for relation in relations
    ]


def _collation_identity(infos):
    # Arrow schema bytes + source relation identity are already protected by
    # source_state.schema_hash/source_epoch. Keep the exact ordered hashes in
    # the canonical collation slot until incremental_contract v2 grows a
    # dedicated type/collation vector.
    return "source-schema:"+json.dumps(
        [info["schema_hash"] for info in infos],
        separators=(",",":"))


def task_state_spec(con,kind,task):
    task=dict(task)
    task["kind"]=str(kind)
    infos=_relation_infos(con,task)
    relations=[_source_identity(info) for info in infos]
    epochs=[int(info["schema_epoch"]) for info in infos]
    if kind=="aggregate":
        ir=task["ir"]
        aggregate_ir.validate_ir(ir)
        keys=[
            str(value)
            for value in ir["group_keys"]
        ]
        semantic=aggregate_ir.semantic_id(ir)
    elif kind=="inner_join":
        ir=task["ir"]
        join_ir.validate_ir(ir)
        keys=[]
        for pair in ir["join_pairs"]:
            keys.append("left."+str(pair["left"]))
            keys.append("right."+str(pair["right"]))
        semantic=join_ir.semantic_id(ir)
    else:
        raise ValueError(
            "unsupported stateful physical task kind: "+str(kind))
    return incremental_contract.state_spec(
        "task_state",
        relations,
        epochs,
        key_exprs=keys,
        value_exprs=["ir:"+semantic],
        predicate="TRUE",
        collation=_collation_identity(infos),
        semantics_version=1,
    )


def instance_id(kind,task):
    return (
        "stateful:"+str(kind)+":"
        +_text(task["state_id"],"state_id")
    )


def sync_ready(con,kind,task,watermark):
    watermark=int(watermark)
    if watermark<0:
        raise ValueError(
            "stateful physical watermark cannot be negative")
    spec=task_state_spec(con,kind,task)
    identity=instance_id(kind,task)
    format_tag=(
        "aggregate-state-v1"
        if kind=="aggregate"
        else "join-state-v1"
    )
    metadata=dict(
        task_id=str(task["task_id"]),
        sink_key=str(task["sink_key"]),
        state_id=str(task["state_id"]),
        generation_id=str(task["generation_id"]),
    )
    try:
        current=physical_state_catalog.state_info(
            con,identity)
    except KeyError:
        current=physical_state_catalog.register_state(
            con,spec,"sqlite",format_tag,
            watermark,
            min_readable_watermark=watermark,
            generation=1,
            health="ready",
            metadata=metadata,
            instance_id=identity)
    else:
        if not physical_state_catalog.semantic_compatible(
            current,spec):
            raise RuntimeError(
                "stateful physical instance semantic identity changed")
        if current["backend"]!="sqlite" or current["format_tag"]!=format_tag:
            raise RuntimeError(
                "stateful physical backend/format changed")
        if current["metadata"]!=metadata:
            raise RuntimeError(
                "stateful physical metadata changed")
        if watermark<int(current["watermark"]):
            raise RuntimeError(
                "stateful physical watermark moved backwards")
        if watermark>int(current["watermark"]):
            current=physical_state_catalog.advance_state(
                con,identity,watermark,
                min_readable_watermark=watermark)
        if current["health"]!="ready":
            current=physical_state_catalog.set_health(
                con,identity,"ready")
    physical_state_catalog.retain_state(
        con,identity,str(task["task_id"]),"owner")
    return physical_state_catalog.state_info(
        con,identity)


def retire(con,kind,task):
    identity=instance_id(kind,task)
    try:
        physical_state_catalog.state_info(
            con,identity)
    except KeyError:
        return None
    physical_state_catalog.release_state(
        con,identity,str(task["task_id"]),"owner")
    return physical_state_catalog.set_health(
        con,identity,"retired")


def sync_runtime_result(con,item,result):
    task=result.get("task") or item["task"]
    generation=result.get("generation") or {}
    if (
        str(task.get("status"))=="active"
        and str(generation.get("status"))=="ready"
    ):
        frontier=result.get("visible_frontier")
        if frontier is None:
            frontier=generation.get("fixed_w")
        return sync_ready(
            con,item["kind"],task,int(frontier))
    return None
