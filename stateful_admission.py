#!/usr/bin/env python3
"""Fail-closed resource admission for new stateful task generations.

The gate intentionally uses durable, cheap-to-read counters only. It does not
pretend to predict future operator cardinality. Limits default to zero (off),
so existing deployments keep their current behavior until a budget is set.
"""
import json
import time


_LIMIT_KEYS = {
    "max_tasks":"stateful_admission_max_tasks",
    "max_building":"stateful_admission_max_building",
    "max_state_bytes":"stateful_admission_max_state_bytes",
    "max_pending_bytes":"stateful_admission_max_pending_bytes",
    "max_source_lag":"stateful_admission_max_source_lag",
    "reserve_state_bytes":"stateful_admission_reserve_state_bytes",
}


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS stateful_admission_decisions(
            task_id TEXT PRIMARY KEY,
            sink_key TEXT NOT NULL,
            admitted INTEGER NOT NULL CHECK(admitted IN (0,1)),
            reason TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            limits_json TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_admission_decisions_admitted
            ON stateful_admission_decisions(admitted,updated,task_id);
    """)


def _table_exists(con,name):
    return con.execute("""
        SELECT 1 FROM sqlite_master
        WHERE type IN ('table','view') AND name=?
    """,(str(name),)).fetchone() is not None


def _limits(cfg):
    cfg=cfg or {}
    result={}
    for name,key in _LIMIT_KEYS.items():
        value=int(cfg.get(key,0) or 0)
        if value<0:
            raise ValueError(key+" cannot be negative")
        result[name]=value
    return result


def _descriptor_rows(con,table):
    if not _table_exists(con,table):
        return []
    return [
        (str(task_id),str(status))
        for task_id,status in con.execute(
            "SELECT task_id,status FROM "+table
        ).fetchall()
    ]


def _existing_tasks(con):
    rows=(
        _descriptor_rows(con,"aggregate_task_descriptors")
        +_descriptor_rows(con,"join_task_descriptors")
    )
    return {
        task_id:status
        for task_id,status in rows
        if status not in {"retired","failed"}
    }


def _state_bytes(con):
    if not _table_exists(con,"stateful_state_sizes"):
        return 0
    return int(con.execute("""
        SELECT COALESCE(SUM(payload_bytes),0)
        FROM stateful_state_sizes
    """).fetchone()[0] or 0)


def _pending_bytes(con):
    if _table_exists(con,"active_jobs"):
        return int(con.execute("""
            SELECT COALESCE(SUM(logical_bytes),0)
            FROM active_jobs
        """).fetchone()[0] or 0)
    if not _table_exists(con,"jobs"):
        return 0
    if _table_exists(con,"retired_jobs"):
        return int(con.execute("""
            SELECT COALESCE(SUM(j.logical_bytes),0)
            FROM jobs j
            LEFT JOIN retired_jobs r ON r.job_id=j.id
            WHERE r.job_id IS NULL
        """).fetchone()[0] or 0)
    return int(con.execute("""
        SELECT COALESCE(SUM(logical_bytes),0) FROM jobs
    """).fetchone()[0] or 0)


def _source_seq(con,key):
    if not _table_exists(con,"source_state_meta"):
        return 0
    row=con.execute(
        "SELECT value FROM source_state_meta WHERE key=?",
        (str(key),)
    ).fetchone()
    return 0 if row is None else int(row[0])


def snapshot(con,additions=()):
    existing=_existing_tasks(con)
    requested=[]
    sinks={}
    for item in additions or ():
        task=dict(item.get("task") or {})
        task_id=str(task.get("task_id") or "").strip()
        if not task_id:
            raise ValueError("stateful admission task_id must be non-empty")
        sink=str(task.get("sink_key") or "").strip()
        if not sink:
            raise ValueError("stateful admission sink_key must be non-empty")
        sinks[task_id]=sink
        if task_id not in existing and task_id not in requested:
            requested.append(task_id)
    durable=_source_seq(con,"log_durable_seq")
    applied=_source_seq(con,"base_applied_seq")
    return dict(
        current_tasks=len(existing),
        current_building=sum(
            1 for status in existing.values()
            if status=="candidate"),
        requested_tasks=len(requested),
        requested_task_ids=sorted(requested),
        requested_sinks={
            task_id:sinks[task_id]
            for task_id in sorted(requested)
        },
        state_bytes=_state_bytes(con),
        pending_bytes=_pending_bytes(con),
        source_log_durable_seq=durable,
        source_base_applied_seq=applied,
        source_lag=max(0,durable-applied),
    )


def _reasons(metrics,limits):
    reasons=[]
    requested=int(metrics["requested_tasks"])
    projected_state=(
        int(metrics["state_bytes"])
        +requested*int(limits["reserve_state_bytes"])
    )
    # Admission controls creation of new durable work. Existing generations
    # must always be allowed to recover after a crash, even when the current
    # machine is already above a configured soft capacity boundary.
    if requested==0:
        return reasons,projected_state
    if (
        limits["max_tasks"]>0
        and metrics["current_tasks"]+requested
            >limits["max_tasks"]
    ):
        reasons.append("max_tasks")
    if (
        limits["max_building"]>0
        and metrics["current_building"]+requested
            >limits["max_building"]
    ):
        reasons.append("max_building")
    if (
        limits["max_state_bytes"]>0
        and projected_state>limits["max_state_bytes"]
    ):
        reasons.append("max_state_bytes")
    if (
        limits["max_pending_bytes"]>0
        and metrics["pending_bytes"]>limits["max_pending_bytes"]
    ):
        reasons.append("max_pending_bytes")
    if (
        limits["max_source_lag"]>0
        and metrics["source_lag"]>limits["max_source_lag"]
    ):
        reasons.append("max_source_lag")
    return reasons,projected_state


def _record(con,task_id,sink,ok,reason,metrics,limits):
    now=time.time()
    con.execute("""
        INSERT INTO stateful_admission_decisions(
            task_id,sink_key,admitted,reason,
            metrics_json,limits_json,created,updated)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(task_id) DO UPDATE SET
            sink_key=excluded.sink_key,
            admitted=excluded.admitted,
            reason=excluded.reason,
            metrics_json=excluded.metrics_json,
            limits_json=excluded.limits_json,
            updated=excluded.updated
    """,(
        str(task_id),str(sink),1 if ok else 0,str(reason),
        json.dumps(metrics,sort_keys=True,separators=(",",":")),
        json.dumps(limits,sort_keys=True,separators=(",",":")),
        now,now,
    ))


def admit(con,additions,cfg=None):
    install(con)
    additions=list(additions or ())
    limits=_limits(cfg)
    metrics=snapshot(con,additions)
    reasons,projected_state=_reasons(metrics,limits)
    metrics=dict(metrics)
    metrics["projected_state_bytes"]=projected_state
    ok=not reasons
    reason="admitted" if ok else ",".join(reasons)
    requested=set(metrics["requested_task_ids"])
    for item in additions:
        task=dict(item.get("task") or {})
        task_id=str(task.get("task_id") or "")
        if task_id not in requested:
            continue
        _record(
            con,task_id,task["sink_key"],
            ok,reason,metrics,limits)
    return dict(
        ok=ok,
        reason=reason,
        reasons=list(reasons),
        metrics=metrics,
        limits=limits,
    )


def admit_or_raise(con,additions,cfg=None):
    result=admit(con,additions,cfg)
    if result["ok"]:
        return result
    raise RuntimeError(
        "stateful resource admission rejected: "
        +result["reason"]
        +" metrics="
        +json.dumps(
            result["metrics"],
            sort_keys=True,separators=(",",":"))
        +" limits="
        +json.dumps(
            result["limits"],
            sort_keys=True,separators=(",",":"))
    )


def decision_info(con,task_id):
    if not _table_exists(con,"stateful_admission_decisions"):
        raise KeyError("stateful admission decision does not exist")
    row=con.execute("""
        SELECT sink_key,admitted,reason,
               metrics_json,limits_json,created,updated
        FROM stateful_admission_decisions
        WHERE task_id=?
    """,(str(task_id),)).fetchone()
    if row is None:
        raise KeyError("stateful admission decision does not exist")
    return dict(
        task_id=str(task_id),
        sink_key=str(row[0]),
        admitted=bool(row[1]),
        reason=str(row[2]),
        metrics=json.loads(row[3]),
        limits=json.loads(row[4]),
        created=float(row[5]),
        updated=float(row[6]),
    )


def status(con):
    if not _table_exists(con,"stateful_admission_decisions"):
        return dict(decisions=0,admitted=0,rejected=0)
    total,admitted,rejected=con.execute("""
        SELECT COUNT(*),
               COALESCE(SUM(admitted),0),
               COALESCE(SUM(CASE WHEN admitted=0 THEN 1 ELSE 0 END),0)
        FROM stateful_admission_decisions
    """).fetchone()
    return dict(
        decisions=int(total),
        admitted=int(admitted),
        rejected=int(rejected),
    )
