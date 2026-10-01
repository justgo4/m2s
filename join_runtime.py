#!/usr/bin/env python3
"""Candidate runtime state machine for one durable INNER JOIN generation.

The runtime composes the proven contracts: two-source fixed-W bootstrap,
atomic source-log consumption, durable JOIN outbox and the ordinary j4 durable
writer pipeline. A generation is published ready only when compute has caught
up to the applied source base and the target-visible outbox frontier covers the
same source sequence.
"""
import join_generation
import join_job_bridge
import join_log_consumer
import join_outbox
import source_state
import task_generation


def step(
        con,sink_key,plan_version,consumer_id,ir,state_id,
        mapping,cfg,bootstrap_limit=1000
):
    current=join_generation.begin(
        con,sink_key,plan_version,ir,state_id)
    generation=current["generation"]
    reused_physical=bool(
        current.get("reused_physical",False))

    if current["phase"]=="bootstrap":
        result=join_generation.process_next_chunk(
            con,sink_key,plan_version,ir,state_id,
            limit=bootstrap_limit)
        if result["done"]:
            activated=join_generation.activate_catchup(
                con,sink_key,plan_version,consumer_id,
                ir,state_id)
            generation=activated["generation"]
    else:
        generation=task_generation.info(
            con,sink_key,int(plan_version))

    consumer=None
    if generation["source_pin_released"]:
        consumer=source_state.consumer_info(
            con,consumer_id)
        if int(consumer["watermark"])<source_state.base_applied_seq(con):
            join_log_consumer.process_next(
                con,consumer_id,ir)
            consumer=source_state.consumer_info(
                con,consumer_id)

        join_job_bridge.stage_pending(
            con,consumer_id,mapping,cfg)
        visible=join_outbox.visible_frontier(
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
            "bootstrap"
            if not generation["source_pin_released"]
            else "ready"
            if generation["status"]=="ready"
            else "catchup"
        ),
    )
