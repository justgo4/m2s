#!/usr/bin/env python3
"""Bind aggregate fixed-W bootstrap to the durable task generation lifecycle.

This remains a candidate path, not a catalog/daemon GROUP BY execution path.
It proves one generation owns exactly one source W/pin during bootstrap, then
atomically hands retention to a durable source consumer before releasing the
pin and entering history_staged/catch-up.
"""
import aggregate_bootstrap
import aggregate_ir
import aggregate_log_consumer
import aggregate_outbox
import aggregate_state
import source_state
import task_generation


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def _expected_consumer_metadata(source_relation,plan_version,ir,state_id):
    return dict(
        kind="group_aggregate_v1",
        source_relation=_text(source_relation,"source_relation"),
        plan_version=int(plan_version),
        aggregate_ir_id=aggregate_ir.semantic_id(ir),
        aggregate_state_id=_text(state_id,"state_id"),
        aggregate_state_spec_id=aggregate_state.semantic_id(
            aggregate_ir.state_spec(ir)),
    )


def _validate_existing_consumer(
        con,consumer_id,generation,ir,state_id
):
    consumer=source_state.consumer_info(con,consumer_id)
    expected=_expected_consumer_metadata(
        generation["source_relation"],
        generation["plan_version"],ir,state_id)
    if consumer["metadata"]!=expected:
        raise RuntimeError(
            "aggregate generation consumer semantics changed across restart")
    if int(consumer["watermark"])<int(generation["fixed_w"]):
        raise RuntimeError(
            "aggregate generation consumer moved behind fixed-W")
    return consumer


def begin(con,sink_key,plan_version,ir,state_id):
    aggregate_ir.validate_ir(ir)
    sink_key=_text(sink_key,"sink_key")
    state_id=_text(state_id,"state_id")
    plan_version=int(plan_version)
    relation=ir["source"]["relation"]
    existing=task_generation.maybe_info(
        con,sink_key,plan_version)
    if existing is not None:
        if existing["source_relation"]!=relation:
            raise RuntimeError(
                "aggregate generation source relation changed across restart")
        if existing["status"] in {"failed","retired"}:
            raise RuntimeError(
                "aggregate generation cannot resume terminal status "
                +existing["status"])
        if existing["source_pin_released"]:
            state=aggregate_state.state_info(con,state_id)
            if state["watermark"]<int(existing["fixed_w"]):
                raise RuntimeError(
                    "aggregate state is behind released generation fixed-W")
            return dict(
                generation=existing,pin=None,state=state,
                phase="catchup" if existing["status"]=="history_staged"
                else "ready")
        owner=existing["generation_id"]
        pin=source_state.acquire_or_resume_pin(
            con,owner,[relation])
        if (
            pin["pin_id"]!=existing["source_pin_id"]
            or int(pin["watermark"])!=int(existing["fixed_w"])
        ):
            raise RuntimeError(
                "aggregate generation source pin changed across restart")
        state=aggregate_bootstrap.ensure_build(
            con,state_id,ir,pin["pin_id"])
        return dict(
            generation=existing,pin=pin,state=state,phase="bootstrap")

    owner=task_generation.generation_id(
        sink_key,plan_version)
    pin=source_state.acquire_or_resume_pin(
        con,owner,[relation])
    generation=task_generation.ensure_build(
        con,sink_key,plan_version,relation,
        pin["watermark"],pin["pin_id"])
    state=aggregate_bootstrap.ensure_build(
        con,state_id,ir,pin["pin_id"])
    if int(state["watermark"])!=int(generation["fixed_w"]):
        raise RuntimeError(
            "aggregate state fixed-W differs from task generation")
    return dict(
        generation=generation,pin=pin,state=state,phase="bootstrap")


def process_next_chunk(
        con,sink_key,plan_version,ir,state_id,limit=1000,engine=None,
        fault_after_changes=None
):
    current=begin(
        con,sink_key,plan_version,ir,state_id)
    generation=current["generation"]
    if current["phase"]!="bootstrap":
        raise RuntimeError(
            "aggregate generation bootstrap is already complete")
    if generation["status"]!="building":
        raise RuntimeError(
            "aggregate bootstrap requires building generation")
    result=aggregate_bootstrap.process_next_chunk(
        con,state_id,ir,current["pin"]["pin_id"],
        limit=limit,engine=engine,
        fault_after_changes=fault_after_changes)
    result["generation_id"]=generation["generation_id"]
    return result


def activate_catchup(
        con,sink_key,plan_version,consumer_id,ir,state_id,
        fault_after_consumer=None
):
    aggregate_ir.validate_ir(ir)
    sink_key=_text(sink_key,"sink_key")
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    generation=task_generation.info(
        con,sink_key,int(plan_version))
    if generation["source_relation"]!=ir["source"]["relation"]:
        raise RuntimeError(
            "aggregate generation source differs from execution IR")
    state=aggregate_state.state_info(con,state_id)
    if not state["bootstrap_complete"]:
        raise RuntimeError(
            "cannot activate catch-up before aggregate fixed-W bootstrap")
    if int(state["watermark"])<int(generation["fixed_w"]):
        raise RuntimeError(
            "aggregate state is behind generation fixed-W")

    if generation["source_pin_released"]:
        if generation["status"] not in {"history_staged","ready"}:
            raise RuntimeError(
                "released aggregate generation has invalid lifecycle status")
        consumer=_validate_existing_consumer(
            con,consumer_id,generation,ir,state_id)
        return dict(generation=generation,consumer=consumer)

    if generation["status"]!="building":
        raise RuntimeError(
            "aggregate generation cannot activate catch-up from status "
            +generation["status"])
    aggregate_outbox.ensure_installed(con)
    pin_w=source_state.pin_watermark(
        con,generation["source_pin_id"])
    if int(pin_w)!=int(generation["fixed_w"]):
        raise RuntimeError(
            "aggregate generation fixed-W pin changed before activation")
    if int(state["watermark"])!=int(generation["fixed_w"]):
        raise RuntimeError(
            "aggregate bootstrap must finish exactly at generation fixed-W")

    with task_generation.transaction(con):
        consumer=aggregate_log_consumer.ensure_consumer(
            con,consumer_id,generation["source_relation"],
            generation["plan_version"],ir,state_id,
            generation["fixed_w"])
        if fault_after_consumer is not None:
            fault_after_consumer()
        generation=task_generation.finalize_history_and_release_pin(
            con,sink_key,generation["plan_version"])
    return dict(generation=generation,consumer=consumer)
