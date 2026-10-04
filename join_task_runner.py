#!/usr/bin/env python3
"""Restart-safe executor for one durable INNER JOIN task descriptor.

Task identity, two-source JOIN IR, generation, state, source consumer and target
contract all come from SQLite durable state. On restart the real StarRocks
target is re-read before writes resume unless an already verified mapping is
passed for repeated steps within the same process.
"""
import j4
import cdc_event_trace
import time
import join_runtime
import join_shared_runtime
import join_target_mapping
import join_task_catalog
import stateful_share_policy
import stateful_rebuild
import task_generation


RUNNABLE={"candidate","active"}


def _validate_mapping(task,mapping,con=None):
    if j4.mapping_key(mapping)!=task["sink_key"]:
        raise RuntimeError(
            "JOIN writer mapping sink identity changed")
    if str(mapping.get("sr_table"))!=task["target_table"]:
        rebuild=(
            None if con is None else
            stateful_rebuild.maybe_info(
                con,task["sink_key"]))
        if not (
            rebuild is not None
            and rebuild["new_task_id"]==task["task_id"]
            and rebuild["shadow_target"]==str(
                mapping.get("sr_table"))
            and rebuild["phase"] in {
                "building_shadow","fencing",
                "ready_to_swap"
            }
        ):
            raise RuntimeError(
                "JOIN writer mapping target table changed")
    if int(mapping.get("_plan_version",-1))!=int(
        task["plan_version"]
    ):
        raise RuntimeError(
            "JOIN writer mapping plan version changed")
    if list(j4.pk_columns(mapping))!=[
        join_target_mapping.PAIR_COLUMN
    ]:
        raise RuntimeError(
            "JOIN writer mapping pair identity changed")
    expected=[
        item["name"]
        for item in task["target_schema"]
    ]
    if list(mapping.get("_output_columns",()))!=expected:
        raise RuntimeError(
            "JOIN writer mapping output contract changed")
    if mapping.get("_join_pair_identity")!=join_target_mapping.PAIR_COLUMN:
        raise RuntimeError(
            "JOIN writer mapping lost durable pair identity marker")
    return mapping


def _validate_active_generation(con,task):
    if task["status"]!="active":
        return
    generation=task_generation.maybe_info(
        con,task["sink_key"],task["plan_version"])
    if generation is None:
        raise RuntimeError(
            "active JOIN task is missing its durable generation")
    if generation["generation_id"]!=task["generation_id"]:
        raise RuntimeError(
            "active JOIN task generation identity changed")
    if task_generation.source_relations(
        con,task["sink_key"],task["plan_version"]
    )!=task["source_relations"]:
        raise RuntimeError(
            "active JOIN task generation source set changed")
    if generation["status"]!="ready":
        raise RuntimeError(
            "active JOIN task generation is not ready")


def load_task(
        con,task_id,cfg,mapping_loader=None
):
    task=join_task_catalog.task_info(
        con,task_id)
    if task["status"] not in RUNNABLE:
        raise RuntimeError(
            "JOIN task is not runnable: "
            +task["status"])
    _validate_active_generation(
        con,task)
    loader=(
        join_target_mapping.load_target_mapping
        if mapping_loader is None
        else mapping_loader)
    mapping=loader(
        cfg,task)
    return dict(
        task=task,
        mapping=_validate_mapping(
            task,mapping,con),
    )


def step(
        con,task_id,cfg,mapping=None,
        bootstrap_limit=1000
):
    trace_started=time.monotonic() if cdc_event_trace.enabled() else None
    task=join_task_catalog.task_info(
        con,task_id)
    if task["status"] not in RUNNABLE:
        raise RuntimeError(
            "JOIN task is not runnable: "
            +task["status"])
    _validate_active_generation(
        con,task)
    if mapping is None:
        mapping=join_target_mapping.load_target_mapping(
            cfg,task)
    mapping=_validate_mapping(
        task,mapping,con)

    rebuild=stateful_rebuild.for_task(
        con,task["task_id"])
    rebuild_candidate=(
        rebuild is not None
        and rebuild["new_task_id"]==task["task_id"]
        and rebuild["phase"] in {
            "building_shadow","fencing","ready_to_swap"
        }
    )
    binding=join_shared_runtime.maybe_binding(
        con,task["task_id"])
    if rebuild_candidate and binding is not None:
        raise RuntimeError(
            "JOIN rebuild generation cannot use shared follower state")
    if (
        binding is None
        and task["status"]=="candidate"
        and not rebuild_candidate
    ):
        binding=join_shared_runtime.try_bind(
            con,task,cfg=cfg)
        if (
            binding is None
            and stateful_share_policy.preference_pending(
                con,"inner_join",task["task_id"],cfg=cfg)
        ):
            preference=stateful_share_policy.preference_info(
                con,task["task_id"])
            return dict(
                task=task,
                phase="waiting_shared_leader",
                waiting_shared_leader=True,
                preferred_leader_task_id=(
                    preference["preferred_leader_task_id"]),
            )
    if binding is not None:
        try:
            result=join_shared_runtime.step(
                con,task,mapping,cfg,bootstrap_limit=bootstrap_limit)
        except KeyError:
            if join_shared_runtime.maybe_binding(
                con,task["task_id"]
            ) is not None:
                raise
            result=join_runtime.step(
                con,task["sink_key"],task["plan_version"],
                task["consumer_id"],task["ir"],
                task["state_id"],mapping,cfg,
                bootstrap_limit=bootstrap_limit)
    else:
        result=join_runtime.step(
            con,task["sink_key"],task["plan_version"],
            task["consumer_id"],task["ir"],
            task["state_id"],mapping,cfg,
            bootstrap_limit=bootstrap_limit)
    generation=result["generation"]
    if generation["generation_id"]!=task["generation_id"]:
        raise RuntimeError(
            "JOIN runtime returned a different generation identity")
    if task_generation.source_relations(
        con,task["sink_key"],task["plan_version"]
    )!=task["source_relations"]:
        raise RuntimeError(
            "JOIN runtime generation source set differs from descriptor")
    if task["status"]=="active" and generation["status"]!="ready":
        raise RuntimeError(
            "active JOIN task regressed from ready generation")
    if (
        task["status"]=="candidate"
        and generation["status"]=="ready"
    ):
        task=join_task_catalog.set_status(
            con,task["task_id"],"active")
    result["task"]=task
    consumer=result.get("consumer")
    if trace_started is not None and consumer is not None:
        cdc_event_trace.task_frontier(task["sink_key"],task["generation_id"],
            consumer["watermark"],time.monotonic()-trace_started)
    return result
