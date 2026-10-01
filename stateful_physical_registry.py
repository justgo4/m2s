#!/usr/bin/env python3
"""Publish live aggregate/JOIN backing state into the reusable state catalog.

This bridge records only verified backing state.  It delegates semantic identity
and current-only watermark rules to the concrete aggregate/JOIN physical
adapters, then attaches task ownership through physical-state refs.
"""
import aggregate_physical_state
import join_physical_state
import physical_state_catalog


def _adapter(kind):
    kind=str(kind)
    if kind=="aggregate":
        return aggregate_physical_state
    if kind=="inner_join":
        return join_physical_state
    raise ValueError(
        "unsupported stateful physical task kind: "+kind)


def task_state_spec(con,kind,task):
    return _adapter(kind).physical_spec(
        con,task["ir"])


def instance_id(kind,task):
    return _adapter(kind).instance_id(
        task["state_id"])


def sync_ready(con,kind,task,watermark=None):
    adapter=_adapter(kind)
    physical=adapter.sync_instance(
        con,task["state_id"],task["ir"],generation=1)
    if watermark is not None and int(watermark)>int(physical["watermark"]):
        raise RuntimeError(
            "reported stateful watermark is ahead of backing physical state")
    physical_state_catalog.retain_state(
        con,physical["instance_id"],
        str(task["task_id"]),"owner")
    return physical_state_catalog.state_info(
        con,physical["instance_id"])


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
        return sync_ready(
            con,item["kind"],task,
            watermark=result.get("visible_frontier"))
    return None
