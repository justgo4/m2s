#!/usr/bin/env python3
"""Durable admission policy and explainability for shared stateful compute.

The correctness layer already proves whether a leader can cover a follower.
This module decides whether to use that safe sharing opportunity.  Default
"compatible" preserves the current deterministic behavior.  "off" disables
sharing for controlled A/B tests.  "adaptive" adds bounded lag/fanout/surplus
admission and records the measured decision without changing semantics.
"""
import json
import time

import source_state


MODES={"compatible","off","adaptive"}


def install(con):
    con.executescript("""
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


def _state_rows(con,kind,state_id):
    if kind=="aggregate":
        table="aggregate_groups"
    elif kind=="inner_join":
        table="join_rows"
    else:
        raise ValueError(
            "unsupported stateful sharing kind: "+str(kind))
    return int(con.execute(
        "SELECT COUNT(*) FROM "+table+" WHERE state_id=?",
        (str(state_id),)
    ).fetchone()[0])


def candidate_metrics(con,kind,candidate):
    leader,state,consumer,physical,reuse=candidate
    applied=int(source_state.base_applied_seq(con))
    watermark=int(consumer["watermark"])
    surplus=int(
        reuse.get("surplus_aggregates",
        reuse.get("surplus_projections",0)))
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
        state_rows=_state_rows(
            con,kind,leader["state_id"]),
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


def choose(con,kind,task,candidates,cfg=None):
    """Return one correctness-approved candidate or None."""
    mode=policy_mode(cfg)
    candidates=list(candidates or ())
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

    measured=[
        (candidate,candidate_metrics(con,kind,candidate))
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

    limits=_limits(cfg)
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
            -metrics["state_rows"],
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
    return dict(
        decisions=decisions,
        selected=selected,
        samples=int(observed[0]),
        max_leader_lag=int(observed[1]),
        max_source_lag=int(observed[2]),
        max_visible_lag=int(observed[3]),
        copied_sequences=int(observed[4]),
    )
