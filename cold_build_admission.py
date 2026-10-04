"""Opt-in, process-wide admission for durable stateful history builders.

The existing task/generation records are the queue. There is no durable lease
to strand on exit. This does not bound the duration of an individual step.
"""
import threading

import aggregate_task_catalog
import join_task_catalog
import source_state
import stateful_share_policy
import task_generation


def task_info(con, kind, task_id):
    catalog = aggregate_task_catalog if kind == "aggregate" else join_task_catalog
    return catalog.task_info(con, task_id)


def needs_history(con, task):
    if task["status"] != "candidate":
        return False
    generation = task_generation.maybe_info(
        con, task["sink_key"], task["plan_version"])
    return generation is None or generation["status"] == "building"


def eligible(con, cfg, runtime):
    rows = con.execute("""
        SELECT 'aggregate',task_id,created FROM aggregate_task_descriptors
        WHERE status='candidate'
        UNION ALL
        SELECT 'inner_join',task_id,created FROM join_task_descriptors
        WHERE status='candidate'
        ORDER BY 3,2
    """).fetchall()
    active = runtime.get("stateful_active_task_ids")
    candidates = {str(row[1]) for row in rows}
    result = []
    for kind, task_id, created in rows:
        if active is not None and task_id not in active:
            continue
        task = task_info(con, kind, task_id)
        if not needs_history(con, task):
            continue
        relations = ([task["source_relation"]] if kind == "aggregate"
                     else task["source_relations"])
        if any(source_state.relation_info(con, relation)["complete_seq"] is None
               for relation in relations):
            continue
        if stateful_share_policy.policy_mode(cfg) != "off":
            preference = stateful_share_policy.maybe_preference(con, task_id)
            if preference is not None:
                # A waiting follower must not monopolize admission ahead of
                # the candidate leader that makes its placement possible.
                if preference["preferred_leader_task_id"] in candidates:
                    continue
        result.append(str(task_id))
    return result


def step(con, cfg, runtime, kind, task_id, run_step, bootstrap_limit):
    if not cfg.get("cold_build_admission", False):
        return run_step(bootstrap_limit)
    task = task_info(con, kind, task_id)
    if not needs_history(con, task):
        return run_step(bootstrap_limit)
    lock = runtime.setdefault("cold_build_lock", threading.Lock())
    if not lock.acquire(blocking=False):
        return dict(rebuild_paused=True, cold_build_paused=True)
    try:
        # Re-read under the shared process guard; other workers may have
        # completed history or retired an earlier queued task meanwhile.
        task = task_info(con, kind, task_id)
        if not needs_history(con, task):
            return run_step(bootstrap_limit)
        queue = eligible(con, cfg, runtime)
        stats = dict(runtime.get("cold_build_status", {}))
        stats.update(queued=len(queue), owner=queue[0] if queue else None)
        if not queue or queue[0] != str(task_id):
            stats["deferrals"] = stats.get("deferrals", 0) + 1
            runtime["cold_build_status"] = stats
            return dict(rebuild_paused=True, cold_build_paused=True)
        stats["steps"] = stats.get("steps", 0) + 1
        runtime["cold_build_status"] = stats
        limit = min(bootstrap_limit, max(1, int(cfg.get("cold_build_rows", 256))))
        return run_step(limit)
    finally:
        lock.release()
