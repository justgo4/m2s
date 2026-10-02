#!/usr/bin/env python3
"""Restart-safe executor for one durable aggregate task descriptor.

The user SQL catalog still does not expose GROUP BY. This adapter closes the
candidate execution boundary first: task identity, aggregate IR, generation,
source consumer and target contract come from SQLite durable state. On a real
restart the StarRocks target is re-read before writes resume; callers may pass
an already verified mapping while repeatedly stepping the same task.
"""
import aggregate_runtime
import aggregate_shared_runtime
import aggregate_target_mapping
import aggregate_task_catalog
import j4
import stateful_share_policy
import task_generation


RUNNABLE={"candidate","active"}


def _validate_mapping(task,mapping):
    if j4.mapping_key(mapping)!=task["sink_key"]:
        raise RuntimeError("aggregate writer mapping sink identity changed")
    if str(mapping.get("sr_table"))!=task["target_table"]:
        raise RuntimeError("aggregate writer mapping target table changed")
    if int(mapping.get("_plan_version",-1))!=int(task["plan_version"]):
        raise RuntimeError("aggregate writer mapping plan version changed")
    if list(j4.pk_columns(mapping))!=list(task["ir"]["group_keys"]):
        raise RuntimeError("aggregate writer mapping primary key changed")
    expected=[
        *task["ir"]["group_keys"],
        *(item["output"] for item in task["ir"]["aggregates"]),
    ]
    if list(mapping.get("_output_columns",()))!=expected:
        raise RuntimeError("aggregate writer mapping output contract changed")
    return mapping


def _validate_active_generation(con,task):
    if task["status"]!="active":
        return
    generation=task_generation.maybe_info(
        con,task["sink_key"],task["plan_version"])
    if generation is None:
        raise RuntimeError(
            "active aggregate task is missing its durable generation")
    if generation["generation_id"]!=task["generation_id"]:
        raise RuntimeError(
            "active aggregate task generation identity changed")
    if generation["status"]!="ready":
        raise RuntimeError(
            "active aggregate task generation is not ready")


def load_task(con,task_id,cfg,mapping_loader=None):
    task=aggregate_task_catalog.task_info(con,task_id)
    if task["status"] not in RUNNABLE:
        raise RuntimeError(
            "aggregate task is not runnable: "+task["status"])
    _validate_active_generation(con,task)
    loader=(
        aggregate_target_mapping.load_target_mapping
        if mapping_loader is None else mapping_loader)
    mapping=loader(cfg,task)
    return dict(task=task,mapping=_validate_mapping(task,mapping))


def step(
        con,task_id,cfg,mapping=None,bootstrap_limit=1000
):
    task=aggregate_task_catalog.task_info(con,task_id)
    if task["status"] not in RUNNABLE:
        raise RuntimeError(
            "aggregate task is not runnable: "+task["status"])
    _validate_active_generation(con,task)
    if mapping is None:
        mapping=aggregate_target_mapping.load_target_mapping(
            cfg,task)
    mapping=_validate_mapping(task,mapping)

    binding=aggregate_shared_runtime.maybe_binding(
        con,task["task_id"])
    if binding is None and task["status"]=="candidate":
        binding=aggregate_shared_runtime.try_bind(
            con,task,cfg=cfg)
        if (
            binding is None
            and stateful_share_policy.preference_pending(
                con,"aggregate",task["task_id"],cfg=cfg)
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
            result=aggregate_shared_runtime.step(
                con,task,mapping,cfg)
        except KeyError:
            # Owner retirement can atomically promote this follower between
            # binding lookup and step on another SQLite connection. Fall back
            # only when the durable binding is now gone; other missing durable
            # state remains a real corruption error.
            if aggregate_shared_runtime.maybe_binding(
                con,task["task_id"]
            ) is not None:
                raise
            result=aggregate_runtime.step(
                con,task["sink_key"],task["plan_version"],
                task["consumer_id"],task["ir"],task["state_id"],
                mapping,cfg,bootstrap_limit=bootstrap_limit)
    else:
        result=aggregate_runtime.step(
            con,task["sink_key"],task["plan_version"],
            task["consumer_id"],task["ir"],task["state_id"],
            mapping,cfg,bootstrap_limit=bootstrap_limit)
    generation=result["generation"]
    if generation["generation_id"]!=task["generation_id"]:
        raise RuntimeError(
            "aggregate runtime returned a different generation identity")
    if task["status"]=="active" and generation["status"]!="ready":
        raise RuntimeError(
            "active aggregate task regressed from ready generation")
    if (
        task["status"]=="candidate"
        and generation["status"]=="ready"
    ):
        task=aggregate_task_catalog.set_status(
            con,task["task_id"],"active")
    result["task"]=task
    return result
