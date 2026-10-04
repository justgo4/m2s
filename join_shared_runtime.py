#!/usr/bin/env python3
"""Durable sharing for exact and projection-subview INNER JOIN tasks."""
import time
import functools
import threading
import contextlib

import join_ir
import join_frozen_state
import join_job_bridge
import join_log_consumer
import join_outbox
import join_output_build
import join_physical_state
import join_state
import join_task_catalog
import physical_state_catalog
import source_state
import stateful_share_policy
import task_generation


# One daemon owns a state file. Locks survive until the final holder/waiter
# exits; no hash collisions can couple unrelated owner/follower handoffs.
_locks={}
_lock_guard=threading.Lock()


@contextlib.contextmanager
def task_guard(con,task):
    database=con.execute('PRAGMA database_list').fetchone()[2]
    key=(database,task['task_id'])
    with _lock_guard:
        entry=_locks.setdefault(key,[threading.RLock(),0])
        entry[1]+=1
    try:
        with entry[0]:
            yield
    finally:
        with _lock_guard:
            entry[1]-=1
            if entry[1]==0:
                del _locks[key]


def _serialized(function):
    @functools.wraps(function)
    def call(con,task,*args,**kwargs):
        with task_guard(con,task):
            return function(con,task,*args,**kwargs)
    return call


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
        CREATE TABLE IF NOT EXISTS join_shared_builds(
            follower_task_id TEXT PRIMARY KEY,
            snapshot_state_id TEXT NOT NULL,
            pin_owner TEXT NOT NULL,
            phase TEXT NOT NULL CHECK(phase IN ('copy','output','cleanup','done','abandoned')));
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


@_serialized
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

    # Existing private builds keep their ownership while outputs catch up.
    # Binding creation still rechecks state after acquiring the writer below.
    try:
        join_state.state_info(con,task["state_id"])
    except KeyError:
        pass
    else:
        return None

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
        owner='join-follower-build:'+task['task_id']
        snapshot='join-follower-snapshot:'+task['task_id']
        join_frozen_state.pin(con,owner,leader['state_id'],fixed_w)
        con.execute('INSERT INTO join_shared_builds VALUES(?,?,?,?)',
                    (task['task_id'],snapshot,owner,'copy'))
        physical_state_catalog.retain_state(
            con,physical["instance_id"],
            task["task_id"],"dependency")
    return binding_info(
        con,task["task_id"])


@_serialized
def _build_step(con,task,binding,cfg,limit=1000):
    row=con.execute('''SELECT snapshot_state_id,pin_owner,phase FROM join_shared_builds
        WHERE follower_task_id=?''',(task['task_id'],)).fetchone()
    if row is None:
        output=join_outbox.commit_info(con,task['consumer_id'],binding['fixed_w'])
        if not output['sealed'] or output['kind']!='bootstrap':
            raise RuntimeError('legacy JOIN follower is missing sealed bootstrap')
        return True
    if row[2]=='done':
        return True
    snapshot,owner,phase=row
    if phase=='abandoned':
        raise RuntimeError('JOIN follower build is abandoned')
    budget=cfg.get('batch_bytes',16*1024**2)
    maximum=cfg.get('max_row_bytes',64*1024**2)
    if phase=='copy':
        copied=join_frozen_state.copy_step(con,owner,snapshot,join_ir.state_spec(task['ir']),
                    limit=limit,byte_limit=budget,max_row_bytes=maximum)
        # A small left side can complete without spending the next call only
        # discovering the right side. Each side still owns its own bounded txn.
        if not copied['done'] and join_state.state_info(con,snapshot)['left_complete']:
            copied=join_frozen_state.copy_step(con,owner,snapshot,join_ir.state_spec(task['ir']),
                        limit=limit,byte_limit=budget,max_row_bytes=maximum)
        if not copied['done']:
            return False
        with join_state.transaction(con):
            join_frozen_state.release(con,owner)
            con.execute("UPDATE join_shared_builds SET phase='output' WHERE follower_task_id=? AND phase='copy'",
                        (task['task_id'],))
        phase='output'
    if phase=='output':
        built=join_output_build.step(con,task['consumer_id'],snapshot,task['plan_version'],
                    task['generation_id'],binding['fixed_w'],row_limit=limit,
                    byte_limit=budget,max_row_bytes=maximum,stream_state_id=binding['shared_state_id'])
        if not built['done']:
            return False
        con.execute("UPDATE join_shared_builds SET phase='cleanup' WHERE follower_task_id=? AND phase='output'",
                    (task['task_id'],))
    if not join_frozen_state.discard_step(con,snapshot,limit=limit,byte_limit=budget):
        return False
    with join_state.transaction(con):
        # Removing the manifest precedes deleting its frozen backing FK.
        con.execute('DELETE FROM join_output_builds WHERE consumer_id=? AND source_seq=?',
                    (task['consumer_id'],binding['fixed_w']))
        con.execute('DELETE FROM join_states WHERE state_id=?',(snapshot,))
        con.execute("UPDATE join_shared_builds SET phase='done' WHERE follower_task_id=?",(task['task_id'],))
    return True


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


@_serialized
def step(con,task,mapping,cfg,bootstrap_limit=1000):
    if con.execute('SELECT 1 FROM join_frozen_pins WHERE owner=?',
                   ('join-promotion:'+task['task_id'],)).fetchone():
        return dict(generation=task_generation.info(con,task['sink_key'],task['plan_version']),
                    consumer=source_state.consumer_info(con,task['consumer_id']),
                    phase='waiting_promotion',waiting_shared_leader=True)
    binding=binding_info(con,task['task_id'])
    if not _build_step(con,task,binding,cfg,limit=bootstrap_limit):
        return dict(generation=task_generation.info(con,task['sink_key'],task['plan_version']),
                    consumer=source_state.consumer_info(con,task['consumer_id']),
                    source_applied=source_state.base_applied_seq(con),visible_frontier=None,
                    phase='bootstrap',shared_physical=True,
                    shared_leader_task_id=binding['leader_task_id'],shared_state_id=binding['shared_state_id'])
    with source_state.read_snapshot(con):
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
        copied_sequences=copied_sequences,unchanged_interval=1)
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
        with join_state.transaction(con):
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
    if con.in_transaction:
        raise RuntimeError('JOIN promotion requires independent chunk transactions')
    for binding in bindings:
        follower_task=join_task_catalog.task_info(con,binding['follower_task_id'])
        if follower_task['status'] in {'retired','failed'}:
            continue
        result=_promote_one(con,follower_task,leader_task,binding)
        if result is not None:
            promoted.append(result)
    return promoted


@_serialized
def _promote_one(con,follower_task,leader_task,binding):
    follower_task=join_task_catalog.task_info(con,follower_task['task_id'])
    if follower_task['status'] in {'retired','failed'}:
        return None
    if maybe_binding(con,follower_task['task_id']) is None:
        return None
    reuse=join_ir.reuse_plan(leader_task['ir'],follower_task['ir'])
    if reuse is None:
        raise RuntimeError('cannot promote JOIN follower after reuse semantics changed')
    owner='join-promotion:'+follower_task['task_id']
    with join_state.transaction(con):
        pinned=con.execute('SELECT watermark FROM join_frozen_pins WHERE owner=?',(owner,)).fetchone()
        leader_state=join_state.state_info(con,leader_task['state_id'])
        leader_consumer=source_state.consumer_info(con,leader_task['consumer_id'])
        frontier=leader_consumer['watermark'] if pinned is None else int(pinned[0])
        if leader_state['watermark']!=frontier or leader_consumer['watermark']!=frontier:
            raise RuntimeError('JOIN promotion requires a stopped leader at its frozen frontier')
        join_frozen_state.pin(con,owner,leader_task['state_id'],frontier)
    # The persistent promotion pin parks the follower worker after a
    # process restart until owner retirement resumes this handoff.
    while not _build_step(con,follower_task,binding,{}):
        pass
    _copy_through(con,leader_task['consumer_id'],follower_task['consumer_id'],frontier,
                  reuse,follower_task['ir'])
    while not join_frozen_state.copy_step(con,owner,follower_task['state_id'],
                join_ir.state_spec(follower_task['ir']))['done']:
        pass
    with join_state.transaction(con):
        # The final handoff touches only metadata; copied backing bytes
        # and output are already durable, complete and at the same cut.
        join_frozen_state.info(con,owner)
        private=join_state.state_info(con,follower_task['state_id'])
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
        join_frozen_state.release(con,owner)
        con.execute('DELETE FROM join_shared_builds WHERE follower_task_id=?',(follower_task['task_id'],))
        return dict(
            follower_task_id=follower_task["task_id"],
            state_id=follower_task["state_id"],
            frontier=frontier,
        )


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


@_serialized
def abandon_build(con,task):
    row=con.execute('''SELECT snapshot_state_id,pin_owner,phase FROM join_shared_builds
        WHERE follower_task_id=?''',(task['task_id'],)).fetchone()
    if row is None or row[2]=='done':
        return False
    snapshot,owner,_=row
    with join_state.transaction(con):
        con.execute("UPDATE join_shared_builds SET phase='abandoned' WHERE follower_task_id=?",
                    (task['task_id'],))
    binding=binding_info(con,task['task_id'])
    # This candidate has never staged jobs. A durable retirement intent fences
    # the worker before deleting even an already sealed but unpublished seed.
    join_output_build.discard_unactivated(con,task['consumer_id'],binding['fixed_w'],
                                         shared_candidate=True)
    while not join_frozen_state.discard_step(con,snapshot):
        pass
    with join_state.transaction(con):
        join_frozen_state.release(con,owner)
        con.execute('DELETE FROM join_states WHERE state_id=?',(snapshot,))
        con.execute('DELETE FROM join_output_streams WHERE consumer_id=?',(task['consumer_id'],))
        con.execute("UPDATE join_shared_builds SET phase='done' WHERE follower_task_id=?",(task['task_id'],))
    return True


@_serialized
def abandon_promotion(con,task):
    owner='join-promotion:'+task['task_id']
    if not con.execute('SELECT 1 FROM join_frozen_pins WHERE owner=?',(owner,)).fetchone():
        return False
    if maybe_binding(con,task['task_id']) is None:
        raise RuntimeError('cannot discard detached JOIN promotion')
    while not join_frozen_state.discard_step(con,task['state_id']):
        pass
    with join_state.transaction(con):
        con.execute('DELETE FROM join_states WHERE state_id=?',(task['state_id'],))
        join_frozen_state.release(con,owner)
    return True


def release_binding(con,task_id):
    binding=release_dependency(
        con,task_id)
    if binding is None:
        return None
    con.execute("""
        DELETE FROM join_shared_followers
        WHERE follower_task_id=?
    """,(binding["follower_task_id"],))
    con.execute('DELETE FROM join_shared_builds WHERE follower_task_id=?',(binding['follower_task_id'],))
    return binding


def gc_retired_followers(con,limit=64):
    join_frozen_state.gc_step(con)
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
