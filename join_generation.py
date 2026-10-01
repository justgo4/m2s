#!/usr/bin/env python3
"""Bind INNER JOIN fixed-W bootstrap to the shared durable generation lifecycle.

One generation owns one source_state pin whose watermark W covers both JOIN
source relations. After both sides finish bootstrap, the durable source consumer
and JOIN outbox are established before task_generation atomically marks history
staged and releases the pin.
"""
import join_bootstrap
import join_ir
import join_log_consumer
import join_outbox
import join_state
import source_state
import task_generation


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def source_relations(ir):
    join_ir.validate_ir(ir)
    values=[
        str(ir["sources"]["left"]["relation"]),
        str(ir["sources"]["right"]["relation"]),
    ]
    return task_generation.normalize_source_relations(
        values)


def _validate_existing_consumer(
        con,consumer_id,generation,ir,state_id
):
    consumer=source_state.consumer_info(
        con,consumer_id)
    expected=join_log_consumer.consumer_metadata(
        generation["plan_version"],ir,state_id,
        generation["generation_id"])
    if consumer["metadata"]!=expected:
        raise RuntimeError(
            "JOIN generation consumer semantics changed across restart")
    if int(consumer["watermark"])<int(generation["fixed_w"]):
        raise RuntimeError(
            "JOIN generation consumer moved behind fixed-W")
    return consumer


def begin(
        con,sink_key,plan_version,ir,state_id
):
    join_ir.validate_ir(ir)
    sink_key=_text(sink_key,"sink_key")
    state_id=_text(state_id,"state_id")
    plan_version=int(plan_version)
    relations=source_relations(ir)

    existing=task_generation.maybe_info(
        con,sink_key,plan_version)
    if existing is not None:
        if task_generation.source_relations(
            con,sink_key,plan_version
        )!=relations:
            raise RuntimeError(
                "JOIN generation source set changed across restart")
        if existing["status"] in {
            "failed","retired"
        }:
            raise RuntimeError(
                "JOIN generation cannot resume terminal status "
                +existing["status"])
        if existing["source_pin_released"]:
            state=join_state.state_info(
                con,state_id)
            if state["watermark"]<int(
                existing["fixed_w"]
            ):
                raise RuntimeError(
                    "JOIN state is behind released generation fixed-W")
            return dict(
                generation=existing,pin=None,state=state,
                phase=(
                    "catchup"
                    if existing["status"]=="history_staged"
                    else "ready"),
            )
        owner=existing["generation_id"]
        pin=source_state.acquire_or_resume_pin(
            con,owner,relations)
        if (
            pin["pin_id"]!=existing["source_pin_id"]
            or int(pin["watermark"])!=int(
                existing["fixed_w"])
        ):
            raise RuntimeError(
                "JOIN generation source pin changed across restart")
        state=join_bootstrap.ensure_build(
            con,state_id,ir,pin["pin_id"])
        return dict(
            generation=existing,pin=pin,state=state,
            phase="bootstrap")

    owner=task_generation.generation_id(
        sink_key,plan_version)
    pin=source_state.acquire_or_resume_pin(
        con,owner,relations)
    generation=task_generation.ensure_build_multi(
        con,sink_key,plan_version,relations,
        pin["watermark"],pin["pin_id"])
    state=join_bootstrap.ensure_build(
        con,state_id,ir,pin["pin_id"])
    if int(state["watermark"])!=int(
        generation["fixed_w"]
    ):
        raise RuntimeError(
            "JOIN state fixed-W differs from task generation")
    return dict(
        generation=generation,pin=pin,state=state,
        phase="bootstrap")


def process_next_chunk(
        con,sink_key,plan_version,ir,state_id,
        limit=1000,fault_after_rows=None
):
    current=begin(
        con,sink_key,plan_version,ir,state_id)
    generation=current["generation"]
    if current["phase"]!="bootstrap":
        raise RuntimeError(
            "JOIN generation bootstrap is already complete")
    if generation["status"]!="building":
        raise RuntimeError(
            "JOIN bootstrap requires building generation")
    result=join_bootstrap.process_next_chunk(
        con,state_id,ir,current["pin"]["pin_id"],
        limit=limit,
        fault_after_rows=fault_after_rows)
    result["generation_id"]=generation[
        "generation_id"]
    return result


def activate_catchup(
        con,sink_key,plan_version,consumer_id,ir,state_id,
        fault_after_consumer=None
):
    join_ir.validate_ir(ir)
    sink_key=_text(sink_key,"sink_key")
    consumer_id=_text(consumer_id,"consumer_id")
    state_id=_text(state_id,"state_id")
    relations=source_relations(ir)
    generation=task_generation.info(
        con,sink_key,int(plan_version))
    if task_generation.source_relations(
        con,sink_key,int(plan_version)
    )!=relations:
        raise RuntimeError(
            "JOIN generation source set differs from execution IR")

    state=join_state.state_info(
        con,state_id)
    if not state["bootstrap_complete"]:
        raise RuntimeError(
            "cannot activate JOIN catch-up before fixed-W bootstrap")
    if int(state["watermark"])<int(
        generation["fixed_w"]
    ):
        raise RuntimeError(
            "JOIN state is behind generation fixed-W")

    if generation["source_pin_released"]:
        if generation["status"] not in {
            "history_staged","ready"
        }:
            raise RuntimeError(
                "released JOIN generation has invalid lifecycle status")
        consumer=_validate_existing_consumer(
            con,consumer_id,generation,ir,state_id)
        return dict(
            generation=generation,
            consumer=consumer)

    if generation["status"]!="building":
        raise RuntimeError(
            "JOIN generation cannot activate catch-up from status "
            +generation["status"])
    pin_w=source_state.pin_watermark(
        con,generation["source_pin_id"])
    if int(pin_w)!=int(generation["fixed_w"]):
        raise RuntimeError(
            "JOIN generation fixed-W pin changed before activation")
    if int(state["watermark"])!=int(
        generation["fixed_w"]
    ):
        raise RuntimeError(
            "JOIN bootstrap must finish exactly at generation fixed-W")

    join_outbox.ensure_installed(con)
    with task_generation.transaction(con):
        consumer=join_log_consumer.ensure_consumer(
            con,consumer_id,
            generation["plan_version"],ir,state_id,
            generation["fixed_w"],
            generation_id=generation["generation_id"])
        if fault_after_consumer is not None:
            fault_after_consumer()
        generation=task_generation.finalize_history_and_release_pin(
            con,sink_key,generation["plan_version"])
    return dict(
        generation=generation,
        consumer=consumer)
