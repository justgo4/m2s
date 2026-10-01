#!/usr/bin/env python3
"""Candidate runtime state machine for one aggregate generation.

Still not exposed through cdc_catalog. The worker composes the already-proven
contracts: fixed-W generation bootstrap, atomic source-log consumption,
durable output outbox, and bridge into j4 delivery jobs. A generation is
published ready only after it is caught up to the current applied source base
and the target-visible outbox frontier covers the same source sequence.
"""
import aggregate_generation
import aggregate_job_bridge
import aggregate_log_consumer
import aggregate_outbox
import source_state
import task_generation


def step(
        con,sink_key,plan_version,consumer_id,ir,state_id,
        mapping,cfg,bootstrap_limit=1000
):
    current=aggregate_generation.begin(
        con,sink_key,plan_version,ir,state_id)
    generation=current["generation"]
    reused_physical=bool(
        current.get("reused_physical",False))

    if current["phase"]=="bootstrap":
        result=aggregate_generation.process_next_chunk(
            con,sink_key,plan_version,ir,state_id,
            limit=bootstrap_limit)
        if result["done"]:
            activated=aggregate_generation.activate_catchup(
                con,sink_key,plan_version,consumer_id,ir,state_id)
            generation=activated["generation"]
    else:
        generation=task_generation.info(
            con,sink_key,int(plan_version))

    consumer=None
    if generation["source_pin_released"]:
        consumer=source_state.consumer_info(
            con,consumer_id)
        if int(consumer["watermark"])<source_state.base_applied_seq(con):
            aggregate_log_consumer.process_next(
                con,consumer_id,ir)
            consumer=source_state.consumer_info(
                con,consumer_id)

        aggregate_job_bridge.stage_pending(
            con,consumer_id,mapping,cfg)
        visible=aggregate_outbox.visible_frontier(
            con,consumer_id)
        applied=source_state.base_applied_seq(con)
        if (
            generation["status"]=="history_staged"
            and int(consumer["watermark"])==int(applied)
            and int(visible)>=int(consumer["watermark"])
        ):
            generation=task_generation.mark_ready_if_exists(
                con,sink_key,int(plan_version))
    else:
        visible=None
        applied=source_state.base_applied_seq(con)

    return dict(
        generation=generation,
        consumer=consumer,
        source_applied=int(applied),
        visible_frontier=visible,
        reused_physical=reused_physical,
        phase=(
            "bootstrap" if not generation["source_pin_released"]
            else "ready" if generation["status"]=="ready"
            else "catchup"),
    )
