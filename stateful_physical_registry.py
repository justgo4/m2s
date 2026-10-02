#!/usr/bin/env python3
"""Publish live aggregate/JOIN backing state into the reusable state catalog.

This bridge records only verified backing state.  It delegates semantic identity
and current-only watermark rules to the concrete aggregate/JOIN physical
adapters, then attaches task ownership through physical-state refs.
"""
import aggregate_physical_state
import aggregate_shared_runtime
import join_physical_state
import join_shared_runtime
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
    refs=physical_state_catalog.state_refs(
        con,identity)
    if refs:
        # A shared physical state stays live until the final owner/dependency
        # releases it. Retiring one task must never terminalize state still
        # referenced by another task.
        return physical_state_catalog.state_info(
            con,identity)
    return physical_state_catalog.set_health(
        con,identity,"retired")


def sync_runtime_result(con,item,result):
    task=result.get("task") or item["task"]
    generation=result.get("generation") or {}
    if result.get("shared_physical"):
        binding=result.get("shared_state_id")
        if not binding:
            raise RuntimeError(
                "shared stateful runtime omitted physical state identity")
        if item["kind"]=="aggregate":
            identity=aggregate_physical_state.instance_id(
                binding)
        elif item["kind"]=="inner_join":
            identity=join_physical_state.instance_id(
                binding)
        else:
            raise RuntimeError(
                "unsupported shared physical runtime kind: "
                +str(item["kind"]))
        return physical_state_catalog.state_info(
            con,identity)
    if (
        str(task.get("status"))=="active"
        and str(generation.get("status"))=="ready"
    ):
        return sync_ready(
            con,item["kind"],task,
            watermark=result.get("visible_frontier"))
    return None



def _retired_task_for_state(con,kind,state_id):
    if kind=="aggregate":
        table="aggregate_task_descriptors"
    elif kind=="inner_join":
        table="join_task_descriptors"
    else:
        raise ValueError("unsupported stateful physical task kind: "+str(kind))
    rows=con.execute(
        """
        SELECT task_id,sink_key,plan_version,consumer_id,status
        FROM %s
        WHERE state_id=?
        ORDER BY task_id
        """ % table,
        (str(state_id),)
    ).fetchall()
    if len(rows)!=1:
        return None
    task_id,sink_key,plan_version,consumer_id,status=rows[0]
    if str(status) not in {"retired","failed"}:
        return None
    return dict(
        task_id=str(task_id),
        sink_key=str(sink_key),
        plan_version=int(plan_version),
        consumer_id=str(consumer_id),
        status=str(status),
    )


def _gc_ready(con,kind,task):
    consumer=task["consumer_id"]
    if con.execute(
        "SELECT 1 FROM source_consumers WHERE consumer_id=?",
        (consumer,)
    ).fetchone():
        return False
    if con.execute(
        "SELECT 1 FROM stateful_retirements WHERE task_id=?",
        (task["task_id"],)
    ).fetchone():
        return False
    generation=con.execute("""
        SELECT status FROM task_generations
        WHERE sink_key=? AND plan_version=?
    """,(
        task["sink_key"],task["plan_version"],
    )).fetchone()
    if generation is not None and str(generation[0]) not in {
        "retired","failed"
    }:
        return False

    if kind=="aggregate":
        link_table="aggregate_job_links"
        commit_table="aggregate_output_commits"
    else:
        link_table="join_job_links"
        commit_table="join_output_commits"
    active=con.execute(
        """
        SELECT 1
        FROM %s l
        LEFT JOIN retired_jobs r ON r.job_id=l.job_id
        WHERE l.consumer_id=? AND r.job_id IS NULL
        LIMIT 1
        """ % link_table,
        (consumer,)
    ).fetchone()
    if active:
        return False
    if con.execute(
        """
        SELECT 1 FROM %s
        WHERE consumer_id=? AND visible=0
        LIMIT 1
        """ % commit_table,
        (consumer,)
    ).fetchone():
        return False
    return True


def gc_retired(con,limit=64):
    """Reclaim retired task backing state/outbox after all safety refs drain."""
    limit=max(1,int(limit))
    removed=[
        dict(
            instance_id=None,
            kind="aggregate_shared_follower",
            task_id=item["follower_task_id"],
            state_id=item["shared_state_id"],
        )
        for item in aggregate_shared_runtime.gc_retired_followers(
            con,limit=limit)
    ]
    removed.extend(
        dict(
            instance_id=None,
            kind="join_shared_follower",
            task_id=item["follower_task_id"],
            state_id=item["shared_state_id"],
        )
        for item in join_shared_runtime.gc_retired_followers(
            con,limit=limit)
    )
    rows=con.execute("""
        SELECT instance_id,format_tag,metadata_json
        FROM physical_states
        WHERE health='retired'
        ORDER BY updated,instance_id
        LIMIT ?
    """,(limit,)).fetchall()
    import json
    for instance_id,format_tag,metadata_json in rows:
        instance_id=str(instance_id)
        if not physical_state_catalog.gc_eligible(
            con,instance_id):
            continue
        metadata=json.loads(metadata_json)
        if str(format_tag).startswith("aggregate-current-"):
            kind="aggregate"
            state_id=str(
                metadata.get("aggregate_state_id") or "")
            state_table="aggregate_states"
            stream_table="aggregate_output_streams"
        elif str(format_tag).startswith("join-current-"):
            kind="inner_join"
            state_id=str(
                metadata.get("join_state_id") or "")
            state_table="join_states"
            stream_table="join_output_streams"
        else:
            continue
        if not state_id:
            continue
        task=_retired_task_for_state(
            con,kind,state_id)
        if task is None or not _gc_ready(
            con,kind,task):
            continue
        with physical_state_catalog.transaction(con):
            if not physical_state_catalog.gc_eligible(
                con,instance_id):
                continue
            current=_retired_task_for_state(
                con,kind,state_id)
            if (
                current is None
                or current["task_id"]!=task["task_id"]
                or not _gc_ready(con,kind,current)
            ):
                continue
            con.execute(
                "DELETE FROM %s WHERE consumer_id=?" % stream_table,
                (task["consumer_id"],))
            con.execute(
                "DELETE FROM %s WHERE state_id=?" % state_table,
                (state_id,))
            physical_state_catalog.delete_state(
                con,instance_id)
        removed.append(dict(
            instance_id=instance_id,
            kind=kind,
            task_id=task["task_id"],
            state_id=state_id,
        ))
    return removed
