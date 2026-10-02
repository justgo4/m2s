#!/usr/bin/env python3
"""Durable sharing for exact and projection-subview INNER JOIN tasks."""
import time

import join_ir
import join_job_bridge
import join_log_consumer
import join_outbox
import join_physical_state
import join_state
import join_task_catalog
import physical_state_catalog
import source_state
import stateful_share_policy
import task_generation


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS join_shared_followers(
            follower_task_id TEXT PRIMARY KEY,
            leader_task_id TEXT NOT NULL,
            shared_state_id TEXT NOT NULL,
            leader_consumer_id TEXT NOT NULL,
            fixed_w INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS join_shared_leader
            ON join_shared_followers(leader_task_id,follower_task_id);
    """)


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def binding_info(con,follower_task_id):
    follower_task_id=_text(
        follower_task_id,"follower_task_id")
    row=con.execute("""
        SELECT leader_task_id,shared_state_id,leader_consumer_id,
               fixed_w,created,updated
        FROM join_shared_followers
        WHERE follower_task_id=?
    """,(follower_task_id,)).fetchone()
    if row is None:
        raise KeyError(
            "JOIN shared follower binding does not exist")
    return dict(
        follower_task_id=follower_task_id,
        leader_task_id=str(row[0]),
        shared_state_id=str(row[1]),
        leader_consumer_id=str(row[2]),
        fixed_w=int(row[3]),
        created=float(row[4]),
        updated=float(row[5]),
    )


def maybe_binding(con,follower_task_id):
    try:
        return binding_info(
            con,follower_task_id)
    except KeyError:
        return None


def followers(con,leader_task_id):
    rows=con.execute("""
        SELECT follower_task_id
        FROM join_shared_followers
        WHERE leader_task_id=?
        ORDER BY follower_task_id
    """,(_text(leader_task_id,"leader_task_id"),)).fetchall()
    return [
        binding_info(con,row[0])
        for row in rows
    ]


def _follower_metadata(task,binding):
    return dict(
        kind="join_shared_follower_v1",
        follower_task_id=str(task["task_id"]),
        leader_task_id=str(binding["leader_task_id"]),
        shared_state_id=str(binding["shared_state_id"]),
        leader_consumer_id=str(binding["leader_consumer_id"]),
        join_ir_id=str(task["ir_id"]),
        generation_id=str(task["generation_id"]),
    )


def _leader_candidates(con,task):
    rows=con.execute("""
        SELECT d.task_id
        FROM join_task_descriptors d
        JOIN task_generations g
          ON g.sink_key=d.sink_key
         AND g.plan_version=d.plan_version
        WHERE d.task_id<>?
          AND d.status='active'
          AND d.left_relation=?
          AND d.right_relation=?
          AND g.status='ready'
          AND g.source_pin_released=1
        ORDER BY d.updated,d.task_id
    """,(
        task["task_id"],
        task["ir"]["sources"]["left"]["relation"],
        task["ir"]["sources"]["right"]["relation"],
    )).fetchall()
    result=[]
    for row in rows:
        leader=join_task_catalog.task_info(
            con,row[0])
        if maybe_binding(con,leader["task_id"]) is not None:
            continue
        reuse=join_ir.reuse_plan(
            leader["ir"],task["ir"])
        if reuse is None:
            continue
        try:
            state=join_state.state_info(
                con,leader["state_id"])
            consumer=source_state.consumer_info(
                con,leader["consumer_id"])
            physical=physical_state_catalog.state_info(
                con,join_physical_state.instance_id(
                    leader["state_id"]))
        except KeyError:
            continue
        requested=join_physical_state.physical_spec(
            con,leader["ir"])
        if (
            not state["bootstrap_complete"]
            or int(state["watermark"])!=int(consumer["watermark"])
            or not physical_state_catalog.semantic_compatible(
                physical,requested)
            or not physical_state_catalog.physically_reusable(
                physical,
                join_physical_state.BACKEND,
                join_physical_state.FORMAT_TAG)
            or physical["metadata"].get(
                "join_state_id")!=leader["state_id"]
        ):
            continue
        result.append((
            0 if reuse["mode"]=="exact" else 1,
            int(reuse["surplus_projections"]),
            leader["created"],leader["task_id"],
            leader,state,consumer,physical,reuse,
        ))
    result.sort(key=lambda item:item[:4])
    return [
        item[4:]
        for item in result
    ]


def try_bind(con,task,cfg=None):
    task=join_task_catalog.task_info(
        con,task["task_id"])
    if task["status"]!="candidate":
        return maybe_binding(
            con,task["task_id"])
    existing=maybe_binding(
        con,task["task_id"])
    if existing is not None:
        return existing

    with join_state.transaction(con):
        existing=maybe_binding(
            con,task["task_id"])
        if existing is not None:
            return existing
        try:
            join_state.state_info(
                con,task["state_id"])
        except KeyError:
            pass
        else:
            return None
        leaders=_leader_candidates(
            con,task)
        chosen=stateful_share_policy.choose(
            con,"inner_join",task,leaders,cfg=cfg)
        if chosen is None:
            return None
        leader,state,consumer,physical,reuse=chosen
        fixed_w=int(state["watermark"])
        if fixed_w!=int(consumer["watermark"]):
            return None
        if fixed_w>source_state.base_applied_seq(con):
            raise RuntimeError(
                "shared JOIN leader is ahead of authoritative base")

        generation=task_generation.import_existing_multi(
            con,task["sink_key"],task["plan_version"],
            task["source_relations"],"history_staged")
        now=time.time()
        con.execute("""
            INSERT INTO join_shared_followers(
                follower_task_id,leader_task_id,shared_state_id,
                leader_consumer_id,fixed_w,created,updated)
            VALUES(?,?,?,?,?,?,?)
        """,(
            task["task_id"],leader["task_id"],
            leader["state_id"],leader["consumer_id"],
            fixed_w,now,now,
        ))
        binding=binding_info(
            con,task["task_id"])
        source_state.register_consumer(
            con,task["consumer_id"],fixed_w,
            owner="join-shared:"+task["task_id"],
            metadata=_follower_metadata(
                task,binding))
        join_outbox.ensure_stream(
            con,task["consumer_id"],leader["state_id"],
            task["plan_version"],generation["generation_id"],
            fixed_w)
        if reuse["mode"]=="exact":
            join_outbox.seed_bootstrap(
                con,task["consumer_id"],leader["state_id"],
                task["plan_version"],generation["generation_id"],
                fixed_w)
        else:
            join_outbox.seed_bootstrap_projected(
                con,task["consumer_id"],leader["state_id"],
                task["plan_version"],generation["generation_id"],
                fixed_w,join_ir.state_spec(task["ir"]))
        physical_state_catalog.retain_state(
            con,physical["instance_id"],
            task["task_id"],"dependency")
    return binding_info(
        con,task["task_id"])


def _validate_binding(con,task,binding):
    leader=join_task_catalog.task_info(
        con,binding["leader_task_id"])
    if leader["status"]!="active":
        raise RuntimeError(
            "JOIN shared leader is not active")
    reuse=join_ir.reuse_plan(
        leader["ir"],task["ir"])
    if (
        reuse is None
        or leader["state_id"]!=binding["shared_state_id"]
        or leader["consumer_id"]!=binding["leader_consumer_id"]
    ):
        raise RuntimeError(
            "JOIN shared leader identity/coverage changed")
    follower=source_state.consumer_info(
        con,task["consumer_id"])
    if follower["metadata"]!=_follower_metadata(
        task,binding
    ):
        raise RuntimeError(
            "JOIN shared follower consumer metadata changed")
    stream=join_outbox.stream_info(
        con,task["consumer_id"])
    if (
        stream["state_id"]!=binding["shared_state_id"]
        or int(stream["fixed_w"])!=int(binding["fixed_w"])
    ):
        raise RuntimeError(
            "JOIN shared follower outbox binding changed")
    return leader,follower,stream,reuse


def step(con,task,mapping,cfg):
    binding=binding_info(
        con,task["task_id"])
    leader,follower,_,reuse=_validate_binding(
        con,task,binding)
    leader_consumer=source_state.consumer_info(
        con,leader["consumer_id"])
    leader_state=join_state.state_info(
        con,leader["state_id"])
    if int(leader_state["watermark"])!=int(
        leader_consumer["watermark"]
    ):
        raise RuntimeError(
            "JOIN shared leader state/consumer watermarks diverged")

    copied_sequences=0
    if int(follower["watermark"])<int(
        leader_consumer["watermark"]
    ):
        next_seq=int(follower["watermark"])+1
        if reuse["mode"]=="exact":
            join_outbox.copy_commit(
                con,leader["consumer_id"],
                task["consumer_id"],next_seq)
        else:
            join_outbox.copy_commit_projected(
                con,leader["consumer_id"],
                task["consumer_id"],next_seq,
                join_ir.state_spec(task["ir"]))
        source_state.advance_consumer(
            con,task["consumer_id"],next_seq)
        follower=source_state.consumer_info(
            con,task["consumer_id"])
        copied_sequences+=1

    join_job_bridge.stage_pending(
        con,task["consumer_id"],mapping,cfg)
    visible=join_outbox.visible_frontier(
        con,task["consumer_id"])
    applied=source_state.base_applied_seq(con)
    generation=task_generation.info(
        con,task["sink_key"],task["plan_version"])
    if (
        generation["status"]=="history_staged"
        and int(follower["watermark"])==int(applied)
        and int(leader_consumer["watermark"])==int(applied)
        and int(visible)>=int(follower["watermark"])
    ):
        generation=task_generation.mark_ready_if_exists(
            con,task["sink_key"],task["plan_version"])
    stateful_share_policy.observe(
        con,task["task_id"],
        int(leader_consumer["watermark"]),
        int(follower["watermark"]),
        int(applied),int(visible),
        copied_sequences=copied_sequences)
    return dict(
        generation=generation,
        consumer=follower,
        source_applied=int(applied),
        visible_frontier=int(visible),
        phase=(
            "ready"
            if generation["status"]=="ready"
            else "catchup"
        ),
        shared_physical=True,
        shared_leader_task_id=leader["task_id"],
        shared_state_id=leader["state_id"],
        shared_reuse_mode=reuse["mode"],
    )


def _copy_through(
        con,leader_consumer_id,follower_consumer_id,
        through_w,reuse,target_ir
):
    follower=source_state.consumer_info(
        con,follower_consumer_id)
    while int(follower["watermark"])<int(through_w):
        seq=int(follower["watermark"])+1
        if reuse["mode"]=="exact":
            join_outbox.copy_commit(
                con,leader_consumer_id,
                follower_consumer_id,seq)
        else:
            join_outbox.copy_commit_projected(
                con,leader_consumer_id,
                follower_consumer_id,seq,
                join_ir.state_spec(target_ir))
        follower=source_state.advance_consumer(
            con,follower_consumer_id,seq)
    return follower


def promote_followers(con,leader_task):
    leader_task=join_task_catalog.task_info(
        con,leader_task["task_id"])
    bindings=followers(
        con,leader_task["task_id"])
    if not bindings:
        return []
    promoted=[]
    with join_state.transaction(con):
        leader_state=join_state.state_info(
            con,leader_task["state_id"])
        leader_consumer=source_state.consumer_info(
            con,leader_task["consumer_id"])
        frontier=int(leader_consumer["watermark"])
        if int(leader_state["watermark"])!=frontier:
            raise RuntimeError(
                "cannot promote shared JOIN followers from divergent leader")
        for binding in bindings:
            follower_task=join_task_catalog.task_info(
                con,binding["follower_task_id"])
            if follower_task["status"] in {"retired","failed"}:
                continue
            reuse=join_ir.reuse_plan(
                leader_task["ir"],follower_task["ir"])
            if reuse is None:
                raise RuntimeError(
                    "cannot promote JOIN follower after reuse semantics changed")
            _copy_through(
                con,leader_task["consumer_id"],
                follower_task["consumer_id"],frontier,
                reuse,follower_task["ir"])
            try:
                private=join_state.state_info(
                    con,follower_task["state_id"])
            except KeyError:
                if reuse["mode"]=="exact":
                    private=join_state.clone_complete_state(
                        con,leader_task["state_id"],
                        follower_task["state_id"],
                        leader_state["spec"],frontier)
                else:
                    private=join_state.clone_projected_state(
                        con,leader_task["state_id"],
                        follower_task["state_id"],
                        join_ir.state_spec(
                            follower_task["ir"]),frontier)
            if int(private["watermark"])!=frontier:
                raise RuntimeError(
                    "promoted JOIN follower private state has wrong W")
            join_physical_state.sync_instance(
                con,follower_task["state_id"],
                follower_task["ir"],generation=1)
            stream=join_outbox.stream_info(
                con,follower_task["consumer_id"])
            if stream["state_id"]!=leader_task["state_id"]:
                raise RuntimeError(
                    "JOIN shared follower stream already detached")
            con.execute("""
                UPDATE join_output_streams
                SET state_id=?,updated=?
                WHERE consumer_id=?
            """,(
                follower_task["state_id"],time.time(),
                follower_task["consumer_id"],
            ))
            source_state.remove_consumer(
                con,follower_task["consumer_id"])
            source_state.register_consumer(
                con,follower_task["consumer_id"],frontier,
                owner="join:"+follower_task["consumer_id"],
                metadata=join_log_consumer.consumer_metadata(
                    follower_task["plan_version"],
                    follower_task["ir"],
                    follower_task["state_id"],
                    follower_task["generation_id"]))
            physical_state_catalog.release_state(
                con,join_physical_state.instance_id(
                    leader_task["state_id"]),
                follower_task["task_id"],"dependency")
            con.execute("""
                DELETE FROM join_shared_followers
                WHERE follower_task_id=?
            """,(follower_task["task_id"],))
            promoted.append(dict(
                follower_task_id=follower_task["task_id"],
                state_id=follower_task["state_id"],
                frontier=frontier,
            ))
    return promoted


def release_dependency(con,task_id):
    binding=maybe_binding(
        con,task_id)
    if binding is None:
        return None
    try:
        physical_state_catalog.release_state(
            con,join_physical_state.instance_id(
                binding["shared_state_id"]),
            binding["follower_task_id"],"dependency")
    except KeyError:
        pass
    return binding


def release_binding(con,task_id):
    binding=release_dependency(
        con,task_id)
    if binding is None:
        return None
    con.execute("""
        DELETE FROM join_shared_followers
        WHERE follower_task_id=?
    """,(binding["follower_task_id"],))
    return binding


def gc_retired_followers(con,limit=64):
    rows=con.execute("""
        SELECT f.follower_task_id
        FROM join_shared_followers f
        JOIN join_task_descriptors d
          ON d.task_id=f.follower_task_id
        WHERE d.status IN ('retired','failed')
        ORDER BY d.updated,d.task_id
        LIMIT ?
    """,(max(1,int(limit)),)).fetchall()
    removed=[]
    for row in rows:
        task=join_task_catalog.task_info(
            con,row[0])
        consumer=task["consumer_id"]
        active=con.execute("""
            SELECT 1
            FROM join_job_links l
            LEFT JOIN retired_jobs r ON r.job_id=l.job_id
            WHERE l.consumer_id=? AND r.job_id IS NULL
            LIMIT 1
        """,(consumer,)).fetchone()
        invisible=con.execute("""
            SELECT 1 FROM join_output_commits
            WHERE consumer_id=? AND visible=0
            LIMIT 1
        """,(consumer,)).fetchone()
        retained=con.execute("""
            SELECT 1 FROM source_consumers
            WHERE consumer_id=?
        """,(consumer,)).fetchone()
        if active or invisible or retained:
            continue
        binding=release_binding(
            con,task["task_id"])
        con.execute("""
            DELETE FROM join_output_streams
            WHERE consumer_id=?
        """,(consumer,))
        removed.append(binding)
    return removed
