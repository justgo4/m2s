#!/usr/bin/env python3
"""Fail-closed resource admission for new stateful task generations.

The gate intentionally uses durable, cheap-to-read counters only. It does not
pretend to predict future operator cardinality. Limits default to zero (off),
so existing deployments keep their current behavior until a budget is set.
"""
import contextlib
import json
import time


class AdmissionDeferred(RuntimeError):
    """Resource admission was rejected but can be retried without side effects."""

    def __init__(self,result,plan_version):
        self.result=dict(result or {})
        self.plan_version=int(plan_version)
        super().__init__(
            "stateful resource admission deferred: "
            +str(self.result.get("reason","rejected"))
            +" plan_version="+str(self.plan_version))


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
            reserved_state_bytes INTEGER NOT NULL DEFAULT 0
                CHECK(reserved_state_bytes>=0),
            reason TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            limits_json TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_admission_decisions_admitted
            ON stateful_admission_decisions(admitted,updated,task_id);
        CREATE TABLE IF NOT EXISTS stateful_admission_waiting(
            task_id TEXT PRIMARY KEY,
            plan_version INTEGER NOT NULL CHECK(plan_version>0),
            sink_key TEXT NOT NULL,
            reason TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0 CHECK(retry_count>=0),
            next_retry REAL NOT NULL DEFAULT 0,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_admission_waiting_due
            ON stateful_admission_waiting(next_retry,plan_version,task_id);
        CREATE INDEX IF NOT EXISTS stateful_admission_waiting_plan
            ON stateful_admission_waiting(plan_version,task_id);
    """)
    columns={
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(stateful_admission_decisions)"
        ).fetchall()
    }
    if "reserved_state_bytes" not in columns:
        con.execute("""
            ALTER TABLE stateful_admission_decisions
            ADD COLUMN reserved_state_bytes INTEGER NOT NULL DEFAULT 0
        """)


@contextlib.contextmanager
def _write_transaction(con):
    own=not con.in_transaction
    if own:
        con.execute("BEGIN IMMEDIATE")
    try:
        yield
        if own:
            con.execute("COMMIT")
    except BaseException:
        if own and con.in_transaction:
            con.execute("ROLLBACK")
        raise


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


def _task_statuses(con):
    rows=(
        _descriptor_rows(con,"aggregate_task_descriptors")
        +_descriptor_rows(con,"join_task_descriptors")
    )
    return {
        task_id:status
        for task_id,status in rows
    }


def _existing_tasks(con):
    return {
        task_id:status
        for task_id,status in _task_statuses(con).items()
        if status not in {"retired","failed"}
    }


def _pending_reservations(con,statuses=None):
    if not _table_exists(
        con,"stateful_admission_decisions"
    ):
        return {}
    statuses=(
        _task_statuses(con)
        if statuses is None else dict(statuses))
    rows=con.execute("""
        SELECT task_id,reserved_state_bytes
        FROM stateful_admission_decisions
        WHERE admitted=1 AND reserved_state_bytes>0
        ORDER BY task_id
    """).fetchall()
    result={}
    for task_id,reserved in rows:
        task_id=str(task_id)
        status=statuses.get(task_id)
        if status is None or status=="candidate":
            result[task_id]=int(reserved)
    return result


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
    statuses=_task_statuses(con)
    existing={
        task_id:status
        for task_id,status in statuses.items()
        if status not in {"retired","failed"}
    }
    reservations=_pending_reservations(
        con,statuses=statuses)
    reserved_missing={
        task_id:reserved
        for task_id,reserved in reservations.items()
        if task_id not in statuses
    }
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
        if (
            task_id not in existing
            and task_id not in reservations
            and task_id not in requested
        ):
            requested.append(task_id)
    durable=_source_seq(con,"log_durable_seq")
    applied=_source_seq(con,"base_applied_seq")
    return dict(
        current_tasks=(
            len(existing)+len(reserved_missing)),
        current_building=(
            sum(
                1 for status in existing.values()
                if status=="candidate")
            +len(reserved_missing)
        ),
        reserved_pending_tasks=len(reservations),
        reserved_state_bytes=sum(
            int(value)
            for value in reservations.values()),
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
        +int(metrics.get("reserved_state_bytes",0))
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


def _record(
        con,task_id,sink,ok,reason,metrics,limits,
        reserved_state_bytes=0
):
    now=time.time()
    con.execute("""
        INSERT INTO stateful_admission_decisions(
            task_id,sink_key,admitted,reserved_state_bytes,reason,
            metrics_json,limits_json,created,updated)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(task_id) DO UPDATE SET
            sink_key=excluded.sink_key,
            admitted=excluded.admitted,
            reserved_state_bytes=excluded.reserved_state_bytes,
            reason=excluded.reason,
            metrics_json=excluded.metrics_json,
            limits_json=excluded.limits_json,
            updated=excluded.updated
    """,(
        str(task_id),str(sink),1 if ok else 0,
        int(reserved_state_bytes),str(reason),
        json.dumps(metrics,sort_keys=True,separators=(",",":")),
        json.dumps(limits,sort_keys=True,separators=(",",":")),
        now,now,
    ))


def admit(con,additions,cfg=None):
    install(con)
    additions=list(additions or ())
    limits=_limits(cfg)
    with _write_transaction(con):
        metrics=snapshot(con,additions)
        reasons,projected_state=_reasons(
            metrics,limits)
        metrics=dict(metrics)
        metrics["projected_state_bytes"]=projected_state
        ok=not reasons
        reason=(
            "already_admitted"
            if ok and not metrics["requested_tasks"]
            else "admitted"
            if ok
            else ",".join(reasons)
        )
        requested=set(metrics["requested_task_ids"])
        for item in additions:
            task=dict(item.get("task") or {})
            task_id=str(task.get("task_id") or "")
            if task_id not in requested:
                continue
            _record(
                con,task_id,task["sink_key"],
                ok,reason,metrics,limits,
                reserved_state_bytes=(
                    limits["reserve_state_bytes"]
                    if ok else 0))
        if ok and requested and _table_exists(
            con,"stateful_admission_waiting"
        ):
            marks=",".join("?" for _ in requested)
            con.execute(
                "DELETE FROM stateful_admission_waiting "
                "WHERE task_id IN ("+marks+")",
                tuple(sorted(requested)))
        return dict(
            ok=ok,
            reason=reason,
            reasons=list(reasons),
            metrics=metrics,
            limits=limits,
        )


def admit_or_defer(
        con,additions,plan_version,cfg=None,
        retry_seconds=5.0
):
    """Admit now or durably queue the exact published plan for retry."""
    result=admit(con,additions,cfg)
    if result["ok"]:
        return result
    queue_wait(
        con,additions,plan_version,
        reason=result["reason"],
        retry_seconds=retry_seconds)
    raise AdmissionDeferred(result,plan_version)


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


def release_unregistered(
        con,additions,reason="registration_failed"
):
    install(con)
    task_ids=[
        str((item.get("task") or {}).get("task_id") or "")
        for item in (additions or ())
    ]
    released=[]
    with _write_transaction(con):
        statuses=_task_statuses(con)
        for task_id in task_ids:
            if not task_id or task_id in statuses:
                continue
            row=con.execute("""
                SELECT admitted,reserved_state_bytes
                FROM stateful_admission_decisions
                WHERE task_id=?
            """,(task_id,)).fetchone()
            if (
                row is None
                or not int(row[0])
                or int(row[1])<=0
            ):
                continue
            con.execute("""
                UPDATE stateful_admission_decisions
                SET reserved_state_bytes=0,
                    reason=?,
                    updated=?
                WHERE task_id=?
            """,(
                "released:"+str(reason),
                time.time(),task_id))
            released.append(task_id)
    return sorted(released)


def queue_wait(
        con,additions,plan_version,reason,
        retry_seconds=5.0
):
    install(con)
    additions=list(additions or ())
    plan_version=int(plan_version)
    if plan_version<=0:
        raise ValueError(
            "stateful admission wait plan_version must be positive")
    retry_seconds=max(0.0,float(retry_seconds))
    now=time.time()
    next_retry=now+retry_seconds
    queued=[]
    with _write_transaction(con):
        for item in additions:
            task=dict(item.get("task") or {})
            task_id=str(task.get("task_id") or "").strip()
            sink=str(task.get("sink_key") or "").strip()
            if not task_id or not sink:
                raise ValueError(
                    "stateful admission waiting task identity is incomplete")
            con.execute("""
                INSERT INTO stateful_admission_waiting(
                    task_id,plan_version,sink_key,reason,
                    retry_count,next_retry,created,updated)
                VALUES(?,?,?,?,0,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    plan_version=excluded.plan_version,
                    sink_key=excluded.sink_key,
                    reason=excluded.reason,
                    retry_count=stateful_admission_waiting.retry_count+1,
                    next_retry=excluded.next_retry,
                    updated=excluded.updated
            """,(
                task_id,plan_version,sink,str(reason),
                next_retry,now,now))
            queued.append(task_id)
    return sorted(queued)


def waiting_plans(con,now=None,due_only=False):
    if not _table_exists(
        con,"stateful_admission_waiting"
    ):
        return []
    now=time.time() if now is None else float(now)
    where=(
        " WHERE next_retry<=?"
        if due_only else "")
    params=(now,) if due_only else ()
    rows=con.execute("""
        SELECT plan_version,COUNT(*),MIN(next_retry),
               MAX(retry_count),MIN(created),MAX(updated)
        FROM stateful_admission_waiting
    """+where+"""
        GROUP BY plan_version
        ORDER BY MIN(next_retry),plan_version
    """,params).fetchall()
    return [
        dict(
            plan_version=int(row[0]),
            tasks=int(row[1]),
            next_retry=float(row[2]),
            retry_count=int(row[3]),
            created=float(row[4]),
            updated=float(row[5]),
        )
        for row in rows
    ]


def waiting_tasks(con,plan_version=None):
    if not _table_exists(
        con,"stateful_admission_waiting"
    ):
        return []
    sql="""
        SELECT task_id,plan_version,sink_key,reason,
               retry_count,next_retry,created,updated
        FROM stateful_admission_waiting
    """
    params=()
    if plan_version is not None:
        sql+=" WHERE plan_version=?"
        params=(int(plan_version),)
    sql+=" ORDER BY next_retry,plan_version,task_id"
    return [
        dict(
            task_id=str(row[0]),
            plan_version=int(row[1]),
            sink_key=str(row[2]),
            reason=str(row[3]),
            retry_count=int(row[4]),
            next_retry=float(row[5]),
            created=float(row[6]),
            updated=float(row[7]),
        )
        for row in con.execute(sql,params).fetchall()
    ]


def clear_wait(
        con,additions=None,task_ids=None,
        plan_version=None
):
    install(con)
    ids={
        str(value).strip()
        for value in (task_ids or ())
        if str(value).strip()
    }
    for item in additions or ():
        task=dict(item.get("task") or {})
        task_id=str(task.get("task_id") or "").strip()
        if task_id:
            ids.add(task_id)
    with _write_transaction(con):
        if ids:
            marks=",".join("?" for _ in ids)
            params=list(sorted(ids))
            sql=(
                "DELETE FROM stateful_admission_waiting "
                "WHERE task_id IN ("+marks+")")
            if plan_version is not None:
                sql+=" AND plan_version=?"
                params.append(int(plan_version))
            cur=con.execute(sql,tuple(params))
        elif plan_version is not None:
            cur=con.execute(
                "DELETE FROM stateful_admission_waiting "
                "WHERE plan_version=?",
                (int(plan_version),))
        else:
            return 0
    return int(cur.rowcount)


def decision_info(con,task_id):
    if not _table_exists(con,"stateful_admission_decisions"):
        raise KeyError("stateful admission decision does not exist")
    row=con.execute("""
        SELECT sink_key,admitted,reserved_state_bytes,reason,
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
        reserved_state_bytes=int(row[2]),
        reason=str(row[3]),
        metrics=json.loads(row[4]),
        limits=json.loads(row[5]),
        created=float(row[6]),
        updated=float(row[7]),
    )


def status(con):
    if not _table_exists(con,"stateful_admission_decisions"):
        waiting=waiting_plans(con)
        return dict(
            decisions=0,admitted=0,rejected=0,
            reserved_pending_tasks=0,
            reserved_state_bytes=0,
            waiting_tasks=sum(
                int(item["tasks"]) for item in waiting),
            waiting_plans=len(waiting),
            next_retry=(
                None if not waiting
                else min(
                    float(item["next_retry"])
                    for item in waiting)))
    total,admitted,rejected=con.execute("""
        SELECT COUNT(*),
               COALESCE(SUM(admitted),0),
               COALESCE(SUM(CASE WHEN admitted=0 THEN 1 ELSE 0 END),0)
        FROM stateful_admission_decisions
    """).fetchone()
    columns={
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(stateful_admission_decisions)"
        ).fetchall()
    }
    pending=(
        _pending_reservations(con)
        if "reserved_state_bytes" in columns
        else {}
    )
    waiting=waiting_plans(con)
    return dict(
        decisions=int(total),
        admitted=int(admitted),
        rejected=int(rejected),
        reserved_pending_tasks=len(pending),
        reserved_state_bytes=sum(
            int(value) for value in pending.values()),
        waiting_tasks=sum(
            int(item["tasks"]) for item in waiting),
        waiting_plans=len(waiting),
        next_retry=(
            None if not waiting
            else min(
                float(item["next_retry"])
                for item in waiting)),
    )
