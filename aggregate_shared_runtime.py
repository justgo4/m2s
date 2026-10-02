#!/usr/bin/env python3
"""Durable exact-semantics sharing for identical aggregate tasks.

One active aggregate task remains the compute owner. A follower keeps its own
target/outbox/source-retention frontier but reuses the owner's current backing
state and byte-exact output commits. If the owner is retired, followers are
atomically promoted to private state at the owner's frozen watermark before the
owner consumer is removed.
"""
import time

import aggregate_job_bridge
import aggregate_log_consumer
import aggregate_outbox
import aggregate_physical_state
import aggregate_state
import aggregate_task_catalog
import physical_state_catalog
import source_state
import task_generation


KIND="aggregate"


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS aggregate_shared_followers(
            follower_task_id TEXT PRIMARY KEY,
            leader_task_id TEXT NOT NULL,
            shared_state_id TEXT NOT NULL,
            leader_consumer_id TEXT NOT NULL,
            fixed_w INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS aggregate_shared_leader
            ON aggregate_shared_followers(leader_task_id,follower_task_id);
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
        FROM aggregate_shared_followers
        WHERE follower_task_id=?
    """,(follower_task_id,)).fetchone()
    if row is None:
        raise KeyError(
            "aggregate shared follower binding does not exist")
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
    leader_task_id=_text(
        leader_task_id,"leader_task_id")
    rows=con.execute("""
        SELECT follower_task_id
        FROM aggregate_shared_followers
        WHERE leader_task_id=?
        ORDER BY follower_task_id
    """,(leader_task_id,)).fetchall()
    return [
        binding_info(con,row[0])
        for row in rows
    ]


def _follower_metadata(task,binding):
    return dict(
        kind="aggregate_shared_follower_v1",
        follower_task_id=str(task["task_id"]),
        leader_task_id=str(binding["leader_task_id"]),
        shared_state_id=str(binding["shared_state_id"]),
        leader_consumer_id=str(binding["leader_consumer_id"]),
        aggregate_ir_id=str(task["ir_id"]),
        generation_id=str(task["generation_id"]),
    )


def _leader_candidates(con,task):
    rows=con.execute("""
        SELECT d.task_id
        FROM aggregate_task_descriptors d
        JOIN task_generations g
          ON g.sink_key=d.sink_key
         AND g.plan_version=d.plan_version
        WHERE d.task_id<>?
          AND d.status='active'
          AND d.ir_id=?
          AND g.status='ready'
          AND g.source_pin_released=1
        ORDER BY d.updated,d.task_id
    """,(task["task_id"],task["ir_id"])).fetchall()
    result=[]
    for row in rows:
        leader=aggregate_task_catalog.task_info(
            con,row[0])
        # A follower may publish the same semantic result but is not a compute
        # owner. Avoid follower chains; bind only to a task with its own state.
        if maybe_binding(con,leader["task_id"]) is not None:
            continue
        try:
            state=aggregate_state.state_info(
                con,leader["state_id"])
            consumer=source_state.consumer_info(
                con,leader["consumer_id"])
            physical=physical_state_catalog.state_info(
                con,aggregate_physical_state.instance_id(
                    leader["state_id"]))
        except KeyError:
            continue
        requested=aggregate_physical_state.physical_spec(
            con,task["ir"])
        if (
            not state["bootstrap_complete"]
            or state["input_semantic_id"]!=task["ir_id"]
            or int(state["watermark"])!=int(consumer["watermark"])
            or not physical_state_catalog.semantic_compatible(
                physical,requested)
            or not physical_state_catalog.physically_reusable(
                physical,
                aggregate_physical_state.BACKEND,
                aggregate_physical_state.FORMAT_TAG)
            or physical["metadata"].get(
                "aggregate_state_id")!=leader["state_id"]
        ):
            continue
        result.append((leader,state,consumer,physical))
    return result


def try_bind(con,task):
    """Attach a candidate task to an exact active compute owner, if available."""
    task=aggregate_task_catalog.task_info(
        con,task["task_id"])
    if task["status"]!="candidate":
        return maybe_binding(
            con,task["task_id"])
    existing=maybe_binding(
        con,task["task_id"])
    if existing is not None:
        return existing

    with aggregate_state.transaction(con):
        # Recheck after taking the SQLite writer lock; owner state cannot move
        # while the bootstrap output is copied.
        existing=maybe_binding(
            con,task["task_id"])
        if existing is not None:
            return existing
        try:
            aggregate_state.state_info(
                con,task["state_id"])
        except KeyError:
            pass
        else:
            return None
        leaders=_leader_candidates(
            con,task)
        if not leaders:
            return None
        leader,state,consumer,physical=leaders[0]
        fixed_w=int(state["watermark"])
        if fixed_w!=int(consumer["watermark"]):
            return None
        if fixed_w>source_state.base_applied_seq(con):
            raise RuntimeError(
                "shared aggregate leader is ahead of authoritative base")

        generation=task_generation.import_existing(
            con,task["sink_key"],task["plan_version"],
            task["source_relation"],"history_staged")
        now=time.time()
        binding=dict(
            follower_task_id=task["task_id"],
            leader_task_id=leader["task_id"],
            shared_state_id=leader["state_id"],
            leader_consumer_id=leader["consumer_id"],
            fixed_w=fixed_w,
        )
        con.execute("""
            INSERT INTO aggregate_shared_followers(
                follower_task_id,leader_task_id,shared_state_id,
                leader_consumer_id,fixed_w,created,updated)
            VALUES(?,?,?,?,?,?,?)
        """,(
            binding["follower_task_id"],
            binding["leader_task_id"],
            binding["shared_state_id"],
            binding["leader_consumer_id"],
            fixed_w,now,now,
        ))
        binding=binding_info(
            con,task["task_id"])
        source_state.register_consumer(
            con,task["consumer_id"],fixed_w,
            owner="aggregate-shared:"+task["task_id"],
            metadata=_follower_metadata(
                task,binding))
        aggregate_outbox.ensure_stream(
            con,task["consumer_id"],leader["state_id"],
            task["plan_version"],generation["generation_id"],
            fixed_w)
        aggregate_outbox.seed_bootstrap(
            con,task["consumer_id"],leader["state_id"],
            task["plan_version"],generation["generation_id"],
            fixed_w)
        physical_state_catalog.retain_state(
            con,physical["instance_id"],
            task["task_id"],"dependency")
    return binding_info(
        con,task["task_id"])


def _validate_binding(con,task,binding):
    if binding["follower_task_id"]!=task["task_id"]:
        raise RuntimeError(
            "aggregate shared binding task identity changed")
    leader=aggregate_task_catalog.task_info(
        con,binding["leader_task_id"])
    if leader["status"]!="active":
        raise RuntimeError(
            "aggregate shared leader is not active")
    if leader["ir_id"]!=task["ir_id"]:
        raise RuntimeError(
            "aggregate shared leader semantics changed")
    if leader["state_id"]!=binding["shared_state_id"]:
        raise RuntimeError(
            "aggregate shared leader state identity changed")
    if leader["consumer_id"]!=binding["leader_consumer_id"]:
        raise RuntimeError(
            "aggregate shared leader consumer identity changed")
    follower=source_state.consumer_info(
        con,task["consumer_id"])
    if follower["metadata"]!=_follower_metadata(
        task,binding
    ):
        raise RuntimeError(
            "aggregate shared follower consumer metadata changed")
    stream=aggregate_outbox.stream_info(
        con,task["consumer_id"])
    if (
        stream["state_id"]!=binding["shared_state_id"]
        or int(stream["fixed_w"])!=int(binding["fixed_w"])
    ):
        raise RuntimeError(
            "aggregate shared follower outbox binding changed")
    return leader,follower,stream


def step(con,task,mapping,cfg):
    """Advance one follower using the leader's already-computed output journal."""
    binding=binding_info(
        con,task["task_id"])
    leader,follower,stream=_validate_binding(
        con,task,binding)
    leader_consumer=source_state.consumer_info(
        con,leader["consumer_id"])
    leader_state=aggregate_state.state_info(
        con,leader["state_id"])
    if int(leader_state["watermark"])!=int(
        leader_consumer["watermark"]
    ):
        raise RuntimeError(
            "aggregate shared leader state/consumer watermarks diverged")

    if int(follower["watermark"])<int(
        leader_consumer["watermark"]
    ):
        next_seq=int(follower["watermark"])+1
        aggregate_outbox.copy_commit(
            con,leader["consumer_id"],
            task["consumer_id"],next_seq)
        source_state.advance_consumer(
            con,task["consumer_id"],next_seq)
        follower=source_state.consumer_info(
            con,task["consumer_id"])

    aggregate_job_bridge.stage_pending(
        con,task["consumer_id"],mapping,cfg)
    visible=aggregate_outbox.visible_frontier(
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
    )


def _copy_through(con,leader_consumer_id,follower_consumer_id,through_w):
    through_w=int(through_w)
    follower=source_state.consumer_info(
        con,follower_consumer_id)
    while int(follower["watermark"])<through_w:
        seq=int(follower["watermark"])+1
        aggregate_outbox.copy_commit(
            con,leader_consumer_id,
            follower_consumer_id,seq)
        follower=source_state.advance_consumer(
            con,follower_consumer_id,seq)
    return follower


def promote_followers(con,leader_task):
    """Detach all followers before their compute owner is retired.

    Each follower receives an atomic private clone at the leader's current W,
    catches its target-neutral outbox up through W, and swaps its retention
    consumer to the ordinary aggregate consumer identity. Future steps then use
    the normal aggregate runtime with no dependency on the retiring leader.
    """
    leader_task=aggregate_task_catalog.task_info(
        con,leader_task["task_id"])
    bindings=followers(
        con,leader_task["task_id"])
    if not bindings:
        return []
    promoted=[]
    with aggregate_state.transaction(con):
        leader_state=aggregate_state.state_info(
            con,leader_task["state_id"])
        leader_consumer=source_state.consumer_info(
            con,leader_task["consumer_id"])
        frontier=int(leader_consumer["watermark"])
        if int(leader_state["watermark"])!=frontier:
            raise RuntimeError(
                "cannot promote shared followers from divergent leader")
        for binding in bindings:
            follower_task=aggregate_task_catalog.task_info(
                con,binding["follower_task_id"])
            if follower_task["status"] in {"retired","failed"}:
                continue
            _copy_through(
                con,leader_task["consumer_id"],
                follower_task["consumer_id"],frontier)
            try:
                private=aggregate_state.state_info(
                    con,follower_task["state_id"])
            except KeyError:
                private=aggregate_state.clone_complete_state(
                    con,leader_task["state_id"],
                    follower_task["state_id"],
                    aggregate_state.validate_spec(
                        leader_state["spec"]),
                    follower_task["ir_id"],frontier)
            if int(private["watermark"])!=frontier:
                raise RuntimeError(
                    "promoted aggregate follower private state has wrong W")
            aggregate_physical_state.sync_instance(
                con,follower_task["state_id"],
                follower_task["ir"],generation=1)

            stream=aggregate_outbox.stream_info(
                con,follower_task["consumer_id"])
            if stream["state_id"]!=leader_task["state_id"]:
                raise RuntimeError(
                    "aggregate shared follower stream already detached")
            con.execute("""
                UPDATE aggregate_output_streams
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
                owner="aggregate:"+follower_task["consumer_id"],
                metadata=aggregate_log_consumer.consumer_metadata(
                    follower_task["source_relation"],
                    follower_task["plan_version"],
                    follower_task["ir"],
                    follower_task["state_id"],
                    follower_task["generation_id"]))
            physical_state_catalog.release_state(
                con,aggregate_physical_state.instance_id(
                    leader_task["state_id"]),
                follower_task["task_id"],"dependency")
            con.execute("""
                DELETE FROM aggregate_shared_followers
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
            con,aggregate_physical_state.instance_id(
                binding["shared_state_id"]),
            binding["follower_task_id"],"dependency")
    except KeyError:
        # The backing physical row may already have been reclaimed only after
        # the dependency ref was absent, so retry cleanup is idempotent.
        pass
    return binding


def release_binding(con,task_id):
    binding=release_dependency(
        con,task_id)
    if binding is None:
        return None
    con.execute("""
        DELETE FROM aggregate_shared_followers
        WHERE follower_task_id=?
    """,(binding["follower_task_id"],))
    return binding


def gc_retired_followers(con,limit=64):
    """Delete detached follower outboxes after retirement and writer drain."""
    limit=max(1,int(limit))
    rows=con.execute("""
        SELECT f.follower_task_id
        FROM aggregate_shared_followers f
        JOIN aggregate_task_descriptors d
          ON d.task_id=f.follower_task_id
        WHERE d.status IN ('retired','failed')
        ORDER BY d.updated,d.task_id
        LIMIT ?
    """,(limit,)).fetchall()
    removed=[]
    for row in rows:
        task=aggregate_task_catalog.task_info(
            con,row[0])
        consumer=task["consumer_id"]
        active=con.execute("""
            SELECT 1
            FROM aggregate_job_links l
            LEFT JOIN retired_jobs r ON r.job_id=l.job_id
            WHERE l.consumer_id=? AND r.job_id IS NULL
            LIMIT 1
        """,(consumer,)).fetchone()
        invisible=con.execute("""
            SELECT 1 FROM aggregate_output_commits
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
            DELETE FROM aggregate_output_streams
            WHERE consumer_id=?
        """,(consumer,))
        removed.append(binding)
    return removed
