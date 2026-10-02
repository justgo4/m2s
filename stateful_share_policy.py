#!/usr/bin/env python3
"""Durable admission policy and explainability for shared stateful compute.

The correctness layer already proves whether a leader can cover a follower.
This module decides whether to use that safe sharing opportunity.  Default
"compatible" preserves the current deterministic behavior.  "off" disables
sharing for controlled A/B tests.  "adaptive" adds bounded lag/fanout/surplus
admission and records the measured decision without changing semantics.
"""
import json

import aggregate_ir
import join_ir
import time

import source_state


MODES={"compatible","off","adaptive"}


def install(con):
    size_table_exists=con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='stateful_state_sizes'
    """).fetchone() is not None
    con.executescript("""
        CREATE TABLE IF NOT EXISTS stateful_state_sizes(
            kind TEXT NOT NULL,
            state_id TEXT NOT NULL,
            rows INTEGER NOT NULL CHECK(rows>=0),
            payload_bytes INTEGER NOT NULL CHECK(payload_bytes>=0),
            PRIMARY KEY(kind,state_id));

        CREATE TRIGGER IF NOT EXISTS aggregate_state_size_insert
        AFTER INSERT ON aggregate_groups
        BEGIN
            INSERT INTO stateful_state_sizes(
                kind,state_id,rows,payload_bytes)
            VALUES(
                'aggregate',NEW.state_id,1,
                length(NEW.key_blob)+length(NEW.key_payload)
                +length(NEW.accum_payload))
            ON CONFLICT(kind,state_id) DO UPDATE SET
                rows=stateful_state_sizes.rows+1,
                payload_bytes=stateful_state_sizes.payload_bytes
                    +excluded.payload_bytes;
        END;
        CREATE TRIGGER IF NOT EXISTS aggregate_state_size_delete
        AFTER DELETE ON aggregate_groups
        BEGIN
            UPDATE stateful_state_sizes
            SET rows=rows-1,
                payload_bytes=payload_bytes-(
                    length(OLD.key_blob)+length(OLD.key_payload)
                    +length(OLD.accum_payload))
            WHERE kind='aggregate' AND state_id=OLD.state_id;
            DELETE FROM stateful_state_sizes
            WHERE kind='aggregate' AND state_id=OLD.state_id
              AND rows=0;
        END;
        CREATE TRIGGER IF NOT EXISTS aggregate_state_size_update
        AFTER UPDATE ON aggregate_groups
        BEGIN
            UPDATE stateful_state_sizes
            SET rows=rows-1,
                payload_bytes=payload_bytes-(
                    length(OLD.key_blob)+length(OLD.key_payload)
                    +length(OLD.accum_payload))
            WHERE kind='aggregate' AND state_id=OLD.state_id;
            DELETE FROM stateful_state_sizes
            WHERE kind='aggregate' AND state_id=OLD.state_id
              AND rows=0;
            INSERT INTO stateful_state_sizes(
                kind,state_id,rows,payload_bytes)
            VALUES(
                'aggregate',NEW.state_id,1,
                length(NEW.key_blob)+length(NEW.key_payload)
                +length(NEW.accum_payload))
            ON CONFLICT(kind,state_id) DO UPDATE SET
                rows=stateful_state_sizes.rows+1,
                payload_bytes=stateful_state_sizes.payload_bytes
                    +excluded.payload_bytes;
        END;

        CREATE TRIGGER IF NOT EXISTS join_state_size_insert
        AFTER INSERT ON join_rows
        BEGIN
            INSERT INTO stateful_state_sizes(
                kind,state_id,rows,payload_bytes)
            VALUES(
                'inner_join',NEW.state_id,1,
                length(NEW.pk_blob)+COALESCE(length(NEW.join_blob),0)
                +length(NEW.row_payload))
            ON CONFLICT(kind,state_id) DO UPDATE SET
                rows=stateful_state_sizes.rows+1,
                payload_bytes=stateful_state_sizes.payload_bytes
                    +excluded.payload_bytes;
        END;
        CREATE TRIGGER IF NOT EXISTS join_state_size_delete
        AFTER DELETE ON join_rows
        BEGIN
            UPDATE stateful_state_sizes
            SET rows=rows-1,
                payload_bytes=payload_bytes-(
                    length(OLD.pk_blob)+COALESCE(length(OLD.join_blob),0)
                    +length(OLD.row_payload))
            WHERE kind='inner_join' AND state_id=OLD.state_id;
            DELETE FROM stateful_state_sizes
            WHERE kind='inner_join' AND state_id=OLD.state_id
              AND rows=0;
        END;
        CREATE TRIGGER IF NOT EXISTS join_state_size_update
        AFTER UPDATE ON join_rows
        BEGIN
            UPDATE stateful_state_sizes
            SET rows=rows-1,
                payload_bytes=payload_bytes-(
                    length(OLD.pk_blob)+COALESCE(length(OLD.join_blob),0)
                    +length(OLD.row_payload))
            WHERE kind='inner_join' AND state_id=OLD.state_id;
            DELETE FROM stateful_state_sizes
            WHERE kind='inner_join' AND state_id=OLD.state_id
              AND rows=0;
            INSERT INTO stateful_state_sizes(
                kind,state_id,rows,payload_bytes)
            VALUES(
                'inner_join',NEW.state_id,1,
                length(NEW.pk_blob)+COALESCE(length(NEW.join_blob),0)
                +length(NEW.row_payload))
            ON CONFLICT(kind,state_id) DO UPDATE SET
                rows=stateful_state_sizes.rows+1,
                payload_bytes=stateful_state_sizes.payload_bytes
                    +excluded.payload_bytes;
        END;

        CREATE TABLE IF NOT EXISTS stateful_share_decisions(
            task_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            mode TEXT NOT NULL,
            selected_leader_task_id TEXT,
            reuse_mode TEXT,
            reason TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_share_decisions_kind
            ON stateful_share_decisions(kind,mode,updated);

        CREATE TABLE IF NOT EXISTS stateful_share_observations(
            task_id TEXT PRIMARY KEY,
            samples INTEGER NOT NULL,
            max_leader_lag INTEGER NOT NULL,
            max_source_lag INTEGER NOT NULL,
            max_visible_lag INTEGER NOT NULL,
            copied_sequences INTEGER NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);

        CREATE TABLE IF NOT EXISTS stateful_share_preferences(
            task_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            preferred_leader_task_id TEXT NOT NULL,
            reuse_mode TEXT NOT NULL,
            reason TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_share_preferences_leader
            ON stateful_share_preferences(
                preferred_leader_task_id,task_id);
    """)
    if not size_table_exists:
        con.execute("""
            INSERT INTO stateful_state_sizes(
                kind,state_id,rows,payload_bytes)
            SELECT 'aggregate',state_id,COUNT(*),
                   COALESCE(SUM(
                       length(key_blob)+length(key_payload)
                       +length(accum_payload)),0)
            FROM aggregate_groups
            GROUP BY state_id
        """)
        con.execute("""
            INSERT INTO stateful_state_sizes(
                kind,state_id,rows,payload_bytes)
            SELECT 'inner_join',state_id,COUNT(*),
                   COALESCE(SUM(
                       length(pk_blob)
                       +COALESCE(length(join_blob),0)
                       +length(row_payload)),0)
            FROM join_rows
            GROUP BY state_id
        """)


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def policy_mode(cfg=None):
    value=str((cfg or {}).get(
        "stateful_share_mode","compatible")).strip().lower()
    if value not in MODES:
        raise ValueError(
            "stateful_share_mode must be one of "
            +",".join(sorted(MODES)))
    return value


def _limits(cfg=None):
    cfg=cfg or {}
    return dict(
        max_lag=max(
            0,int(cfg.get("stateful_share_max_lag",10000))),
        max_followers=max(
            1,int(cfg.get("stateful_share_max_followers",1000))),
        max_surplus=max(
            0,int(cfg.get("stateful_share_max_surplus",64))),
        max_observed_visible_lag=max(
            0,int(cfg.get(
                "stateful_share_max_observed_visible_lag",10000))),
        max_state_rows=max(
            0,int(cfg.get(
                "stateful_share_max_state_rows",0))),
        max_state_bytes=max(
            0,int(cfg.get(
                "stateful_share_max_state_bytes",0))),
    )


def _follower_count(con,kind,leader_task_id):
    table=(
        "aggregate_shared_followers"
        if kind=="aggregate"
        else "join_shared_followers"
        if kind=="inner_join"
        else None
    )
    if table is None:
        raise ValueError(
            "unsupported stateful sharing kind: "+str(kind))
    return int(con.execute(
        "SELECT COUNT(*) FROM "+table+" WHERE leader_task_id=?",
        (str(leader_task_id),)
    ).fetchone()[0])


def _observed_visible_lag(con,kind,leader_task_id):
    table=(
        "aggregate_shared_followers"
        if kind=="aggregate"
        else "join_shared_followers"
        if kind=="inner_join"
        else None
    )
    if table is None:
        raise ValueError(
            "unsupported stateful sharing kind: "+str(kind))
    row=con.execute(
        """
        SELECT COALESCE(MAX(o.max_visible_lag),0)
        FROM %s f
        LEFT JOIN stateful_share_observations o
          ON o.task_id=f.follower_task_id
        WHERE f.leader_task_id=?
        """ % table,
        (str(leader_task_id),)
    ).fetchone()
    return int(row[0] or 0)


def _state_stats(con,kind,state_id):
    kind=str(kind)
    state_id=str(state_id)
    if kind not in {"aggregate","inner_join"}:
        raise ValueError(
            "unsupported stateful sharing kind: "+kind)
    row=con.execute("""
        SELECT rows,payload_bytes
        FROM stateful_state_sizes
        WHERE kind=? AND state_id=?
    """,(kind,state_id)).fetchone()
    if row is None:
        return dict(rows=0,payload_bytes=0)
    return dict(
        rows=int(row[0]),
        payload_bytes=int(row[1]),
    )


def candidate_metrics(con,kind,candidate,measure_state=False):
    leader,state,consumer,physical,reuse=candidate
    applied=int(source_state.base_applied_seq(con))
    watermark=int(consumer["watermark"])
    surplus=int(
        reuse.get("surplus_aggregates",
        reuse.get("surplus_projections",0)))
    state_stats=(
        _state_stats(con,kind,leader["state_id"])
        if measure_state
        else dict(rows=0,payload_bytes=0)
    )
    return dict(
        leader_task_id=str(leader["task_id"]),
        state_id=str(leader["state_id"]),
        reuse_mode=str(reuse["mode"]),
        surplus=surplus,
        lag=max(0,applied-watermark),
        followers=_follower_count(
            con,kind,leader["task_id"]),
        observed_visible_lag=_observed_visible_lag(
            con,kind,leader["task_id"]),
        state_rows=state_stats["rows"],
        state_payload_bytes=state_stats["payload_bytes"],
        physical_health=str(physical["health"]),
        watermark=watermark,
        source_applied=applied,
    )


def _record(
        con,task_id,kind,mode,reason,metrics,
        selected=None,reuse_mode=None
):
    task_id=_text(task_id,"task_id")
    now=time.time()
    payload=json.dumps(
        metrics or {},sort_keys=True,
        separators=(",",":"))
    con.execute("""
        INSERT INTO stateful_share_decisions(
            task_id,kind,mode,selected_leader_task_id,reuse_mode,
            reason,metrics_json,created,updated)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(task_id) DO UPDATE SET
            kind=excluded.kind,
            mode=excluded.mode,
            selected_leader_task_id=excluded.selected_leader_task_id,
            reuse_mode=excluded.reuse_mode,
            reason=excluded.reason,
            metrics_json=excluded.metrics_json,
            updated=excluded.updated
    """,(
        task_id,str(kind),str(mode),
        None if selected is None else str(selected),
        None if reuse_mode is None else str(reuse_mode),
        str(reason),payload,now,now,
    ))


def decision_info(con,task_id):
    row=con.execute("""
        SELECT kind,mode,selected_leader_task_id,reuse_mode,
               reason,metrics_json,created,updated
        FROM stateful_share_decisions
        WHERE task_id=?
    """,(_text(task_id,"task_id"),)).fetchone()
    if row is None:
        raise KeyError(
            "stateful share decision does not exist")
    return dict(
        task_id=str(task_id),
        kind=str(row[0]),
        mode=str(row[1]),
        selected_leader_task_id=(
            None if row[2] is None else str(row[2])),
        reuse_mode=(
            None if row[3] is None else str(row[3])),
        reason=str(row[4]),
        metrics=json.loads(row[5]),
        created=float(row[6]),
        updated=float(row[7]),
    )


def preference_info(con,task_id):
    row=con.execute("""
        SELECT kind,preferred_leader_task_id,reuse_mode,
               reason,created,updated
        FROM stateful_share_preferences
        WHERE task_id=?
    """,(_text(task_id,"task_id"),)).fetchone()
    if row is None:
        raise KeyError(
            "stateful share preference does not exist")
    return dict(
        task_id=str(task_id),
        kind=str(row[0]),
        preferred_leader_task_id=str(row[1]),
        reuse_mode=str(row[2]),
        reason=str(row[3]),
        created=float(row[4]),
        updated=float(row[5]),
    )


def maybe_preference(con,task_id):
    try:
        return preference_info(
            con,task_id)
    except KeyError:
        return None


def _durable_status(con,kind,task_id):
    table=(
        "aggregate_task_descriptors"
        if kind=="aggregate"
        else "join_task_descriptors"
        if kind=="inner_join"
        else None
    )
    if table is None:
        raise ValueError(
            "unsupported stateful sharing kind: "+str(kind))
    row=con.execute(
        "SELECT status,sink_key,plan_version FROM "
        +table+" WHERE task_id=?",
        (str(task_id),)
    ).fetchone()
    if row is None:
        return None
    return dict(
        status=str(row[0]),
        sink_key=str(row[1]),
        plan_version=int(row[2]),
    )


def _task_with_status(con,item):
    kind=str(item["kind"])
    task=dict(item["task"])
    durable=_durable_status(
        con,kind,task["task_id"])
    if durable is None:
        return None
    task["status"]=durable["status"]
    return dict(kind=kind,task=task)


def _reuse(kind,leader,follower):
    if kind=="aggregate":
        return aggregate_ir.reuse_plan(
            leader["ir"],follower["ir"])
    if kind=="inner_join":
        return join_ir.reuse_plan(
            leader["ir"],follower["ir"])
    raise ValueError(
        "unsupported stateful sharing kind: "+str(kind))


def _width(kind,task):
    if kind=="aggregate":
        return len(task["ir"]["aggregates"])
    if kind=="inner_join":
        return len(task["ir"]["projections"])
    raise ValueError(
        "unsupported stateful sharing kind: "+str(kind))


def _surplus(reuse):
    return int(
        reuse.get(
            "surplus_aggregates",
            reuse.get("surplus_projections",0)))


def plan_graph(con,compiled,cfg=None):
    """Persist deterministic leader preferences for a task set.

    This removes startup-order dependence without creating follower chains.
    Existing active compute owners outrank candidates; otherwise wider tasks
    that cover more peers become roots, with task_id as the final stable tie
    breaker.  Preferences are hints only: correctness is revalidated by the
    aggregate/JOIN shared runtime before binding.
    """
    mode=policy_mode(cfg)
    refreshed=[
        value for value in (
            _task_with_status(con,item)
            for item in (compiled or ())
        )
        if value is not None
    ]
    candidate_ids=[
        item["task"]["task_id"]
        for item in refreshed
        if item["task"]["status"]=="candidate"
    ]
    if candidate_ids:
        marks=",".join("?" for _ in candidate_ids)
        con.execute(
            "DELETE FROM stateful_share_preferences "
            "WHERE task_id IN ("+marks+")",
            candidate_ids)
    if mode=="off" or not refreshed:
        return []

    coverage={}
    widths={}
    for leader_item in refreshed:
        kind=leader_item["kind"]
        leader=leader_item["task"]
        if leader["status"] not in {"candidate","active"}:
            continue
        count=0
        for follower_item in refreshed:
            follower=follower_item["task"]
            if (
                follower_item["kind"]!=kind
                or follower["task_id"]==leader["task_id"]
                or follower["status"]!="candidate"
            ):
                continue
            if _reuse(kind,leader,follower) is not None:
                count+=1
        coverage[leader["task_id"]]=count
        widths[leader["task_id"]]=_width(kind,leader)

    def rank(item):
        task=item["task"]
        return (
            0 if task["status"]=="active" else 1,
            -int(coverage.get(task["task_id"],0)),
            -int(widths.get(task["task_id"],0)),
            str(task["task_id"]),
        )

    ranks={
        item["task"]["task_id"]:rank(item)
        for item in refreshed
        if item["task"]["status"] in {"candidate","active"}
    }
    limits=_limits(cfg)
    planned_counts={}
    assigned_followers=set()
    planned_leaders=set()
    now=time.time()
    planned=[]
    for follower_item in refreshed:
        kind=follower_item["kind"]
        follower=follower_item["task"]
        if follower["status"]!="candidate":
            continue
        if follower["task_id"] in planned_leaders:
            # Once a candidate has been selected as a root for another task,
            # keep it a root for this planning pass. This makes no-chain
            # placement independent of catalog task iteration order.
            continue
        follower_rank=ranks.get(
            follower["task_id"])
        choices=[]
        for leader_item in refreshed:
            if leader_item["kind"]!=kind:
                continue
            leader=leader_item["task"]
            if (
                leader["task_id"]==follower["task_id"]
                or leader["status"] not in {"candidate","active"}
                or leader["task_id"] in assigned_followers
            ):
                continue
            leader_rank=ranks.get(
                leader["task_id"])
            if leader_rank is None or not (
                leader_rank<follower_rank
            ):
                continue
            reuse=_reuse(
                kind,leader,follower)
            if reuse is None:
                continue
            surplus=_surplus(reuse)
            if mode=="adaptive":
                existing_followers=_follower_count(
                    con,kind,leader["task_id"])
                projected_followers=(
                    existing_followers
                    +int(planned_counts.get(
                        leader["task_id"],0)))
                if projected_followers>=limits["max_followers"]:
                    continue
                if (
                    reuse["mode"]!="exact"
                    and surplus>limits["max_surplus"]
                ):
                    continue
                if (
                    leader["status"]=="active"
                    and _observed_visible_lag(
                        con,kind,leader["task_id"])
                    >limits["max_observed_visible_lag"]
                ):
                    continue
            score=(
                leader_rank,
                0 if reuse["mode"]=="exact" else 1,
                surplus,
                str(leader["task_id"]),
            )
            choices.append(
                (score,leader,reuse))
        if not choices:
            continue
        choices.sort(key=lambda item:item[0])
        _,leader,reuse=choices[0]
        reason=(
            "graph_active_owner"
            if leader["status"]=="active"
            else "graph_candidate_owner")
        planned_counts[leader["task_id"]]=(
            int(planned_counts.get(leader["task_id"],0))+1)
        planned_leaders.add(
            leader["task_id"])
        assigned_followers.add(
            follower["task_id"])
        con.execute("""
            INSERT INTO stateful_share_preferences(
                task_id,kind,preferred_leader_task_id,
                reuse_mode,reason,created,updated)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(task_id) DO UPDATE SET
                kind=excluded.kind,
                preferred_leader_task_id=excluded.preferred_leader_task_id,
                reuse_mode=excluded.reuse_mode,
                reason=excluded.reason,
                updated=excluded.updated
        """,(
            follower["task_id"],kind,
            leader["task_id"],reuse["mode"],
            reason,now,now))
        planned.append(
            preference_info(
                con,follower["task_id"]))
    return planned


def preference_pending(con,kind,task_id,cfg=None):
    if policy_mode(cfg)=="off":
        return False
    preference=maybe_preference(
        con,task_id)
    if preference is None:
        return False
    if preference["kind"]!=str(kind):
        raise RuntimeError(
            "stateful share preference kind changed")
    leader=_durable_status(
        con,kind,
        preference["preferred_leader_task_id"])
    if leader is None or leader["status"] in {
        "retired","failed"
    }:
        con.execute(
            "DELETE FROM stateful_share_preferences "
            "WHERE task_id=?",
            (str(task_id),))
        return False
    # Adaptive admission is allowed to override an earlier graph preference.
    # A follower that was correctness-compatible at planning time must not
    # wait forever if current lag/fanout/surplus/observed-lag budgets reject
    # that placement at bind time. choose() records this outcome immediately
    # before the runner asks whether the preference still fences bootstrap.
    decision=con.execute("""
        SELECT mode,reason
        FROM stateful_share_decisions
        WHERE task_id=?
    """,(str(task_id),)).fetchone()
    if (
        decision is not None
        and str(decision[0])=="adaptive"
        and str(decision[1])=="adaptive_rejected_all"
    ):
        con.execute(
            "DELETE FROM stateful_share_preferences "
            "WHERE task_id=?",
            (str(task_id),))
        return False
    # A durable graph preference is otherwise a placement fence, not a hint
    # that disappears as soon as the owner generation reaches ready. The
    # shared runtime revalidates physical compatibility on every bind attempt.
    # Keep the follower out of private bootstrap while the preferred owner
    # remains non-terminal; this closes the race between task activation and
    # publishing its physical-state catalog row.
    return leader["status"] in {"candidate","active"}


def choose(con,kind,task,candidates,cfg=None):
    """Return one correctness-approved candidate or None."""
    mode=policy_mode(cfg)
    candidates=list(candidates or ())
    preference=maybe_preference(
        con,task["task_id"])
    if preference is not None and mode!="off":
        if preference["kind"]!=str(kind):
            raise RuntimeError(
                "stateful share preference kind changed")
        candidates=[
            candidate for candidate in candidates
            if str(candidate[0]["task_id"])
            ==preference["preferred_leader_task_id"]
        ]
        if not candidates:
            _record(
                con,task["task_id"],kind,mode,
                "preferred_leader_not_ready",
                dict(
                    preferred_leader_task_id=
                    preference["preferred_leader_task_id"]))
            return None
    if mode=="off":
        _record(
            con,task["task_id"],kind,mode,
            "sharing_disabled",dict(candidate_count=len(candidates)))
        return None
    if not candidates:
        _record(
            con,task["task_id"],kind,mode,
            "no_compatible_leader",dict(candidate_count=0))
        return None

    limits=_limits(cfg)
    measure_state=(
        mode=="adaptive"
        and (
            limits["max_state_rows"]>0
            or limits["max_state_bytes"]>0
        )
    )
    measured=[
        (
            candidate,
            candidate_metrics(
                con,kind,candidate,
                measure_state=measure_state),
        )
        for candidate in candidates
    ]
    if mode=="compatible":
        chosen,metrics=measured[0]
        _record(
            con,task["task_id"],kind,mode,
            "first_compatible_leader",metrics,
            selected=metrics["leader_task_id"],
            reuse_mode=metrics["reuse_mode"])
        return chosen

    admitted=[]
    rejected=[]
    for candidate,metrics in measured:
        reasons=[]
        if metrics["lag"]>limits["max_lag"]:
            reasons.append("lag")
        if metrics["followers"]>=limits["max_followers"]:
            reasons.append("followers")
        if (
            metrics["reuse_mode"]!="exact"
            and metrics["surplus"]>limits["max_surplus"]
        ):
            reasons.append("surplus")
        if (
            metrics["observed_visible_lag"]
            >limits["max_observed_visible_lag"]
        ):
            reasons.append("observed_visible_lag")
        if (
            limits["max_state_rows"]>0
            and metrics["state_rows"]>limits["max_state_rows"]
        ):
            reasons.append("state_rows")
        if (
            limits["max_state_bytes"]>0
            and metrics["state_payload_bytes"]>limits["max_state_bytes"]
        ):
            reasons.append("state_bytes")
        if reasons:
            rejected.append(dict(
                leader_task_id=metrics["leader_task_id"],
                reasons=reasons,metrics=metrics))
            continue
        score=(
            0 if metrics["reuse_mode"]=="exact" else 1,
            metrics["surplus"],
            metrics["lag"],
            metrics["observed_visible_lag"],
            metrics["state_payload_bytes"],
            metrics["state_rows"],
            metrics["followers"],
            metrics["leader_task_id"],
        )
        admitted.append((score,candidate,metrics))
    if not admitted:
        _record(
            con,task["task_id"],kind,mode,
            "adaptive_rejected_all",
            dict(limits=limits,rejected=rejected))
        return None
    admitted.sort(key=lambda item:item[0])
    _,chosen,metrics=admitted[0]
    metrics=dict(metrics)
    metrics["limits"]=limits
    metrics["candidate_count"]=len(candidates)
    metrics["rejected_count"]=len(rejected)
    _record(
        con,task["task_id"],kind,mode,
        "adaptive_selected",metrics,
        selected=metrics["leader_task_id"],
        reuse_mode=metrics["reuse_mode"])
    return chosen



def observe(
        con,task_id,leader_watermark,follower_watermark,
        source_applied,visible_frontier,copied_sequences=0
):
    task_id=_text(task_id,"task_id")
    leader_watermark=int(leader_watermark)
    follower_watermark=int(follower_watermark)
    source_applied=int(source_applied)
    visible_frontier=int(visible_frontier)
    copied_sequences=max(0,int(copied_sequences))
    leader_lag=max(0,leader_watermark-follower_watermark)
    source_lag=max(0,source_applied-follower_watermark)
    visible_lag=max(0,follower_watermark-visible_frontier)
    now=time.time()
    con.execute("""
        INSERT INTO stateful_share_observations(
            task_id,samples,max_leader_lag,max_source_lag,
            max_visible_lag,copied_sequences,created,updated)
        VALUES(?,1,?,?,?,?,?,?)
        ON CONFLICT(task_id) DO UPDATE SET
            samples=stateful_share_observations.samples+1,
            max_leader_lag=MAX(
                stateful_share_observations.max_leader_lag,
                excluded.max_leader_lag),
            max_source_lag=MAX(
                stateful_share_observations.max_source_lag,
                excluded.max_source_lag),
            max_visible_lag=MAX(
                stateful_share_observations.max_visible_lag,
                excluded.max_visible_lag),
            copied_sequences=(
                stateful_share_observations.copied_sequences
                +excluded.copied_sequences),
            updated=excluded.updated
    """,(
        task_id,leader_lag,source_lag,visible_lag,
        copied_sequences,now,now))
    return observation_info(con,task_id)


def observation_info(con,task_id):
    row=con.execute("""
        SELECT samples,max_leader_lag,max_source_lag,
               max_visible_lag,copied_sequences,created,updated
        FROM stateful_share_observations
        WHERE task_id=?
    """,(_text(task_id,"task_id"),)).fetchone()
    if row is None:
        raise KeyError(
            "stateful share observation does not exist")
    return dict(
        task_id=str(task_id),
        samples=int(row[0]),
        max_leader_lag=int(row[1]),
        max_source_lag=int(row[2]),
        max_visible_lag=int(row[3]),
        copied_sequences=int(row[4]),
        created=float(row[5]),
        updated=float(row[6]),
    )


def status(con):
    decisions=int(con.execute(
        "SELECT COUNT(*) FROM stateful_share_decisions"
    ).fetchone()[0])
    selected=int(con.execute("""
        SELECT COUNT(*) FROM stateful_share_decisions
        WHERE selected_leader_task_id IS NOT NULL
    """).fetchone()[0])
    observed=con.execute("""
        SELECT COALESCE(SUM(samples),0),
               COALESCE(MAX(max_leader_lag),0),
               COALESCE(MAX(max_source_lag),0),
               COALESCE(MAX(max_visible_lag),0),
               COALESCE(SUM(copied_sequences),0)
        FROM stateful_share_observations
    """).fetchone()
    preferences=int(con.execute(
        "SELECT COUNT(*) FROM stateful_share_preferences"
    ).fetchone()[0])
    return dict(
        decisions=decisions,
        selected=selected,
        preferences=preferences,
        samples=int(observed[0]),
        max_leader_lag=int(observed[1]),
        max_source_lag=int(observed[2]),
        max_visible_lag=int(observed[3]),
        copied_sequences=int(observed[4]),
    )
