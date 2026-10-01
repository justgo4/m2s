#!/usr/bin/env python3
"""Durable lifecycle for one sink bootstrap generation.

A generation records the fixed-W source cut used by historical bootstrap. It is
not a global target watermark: target visibility remains per ordered lane.
"""
import contextlib
import time


STATUSES = {"building", "history_staged", "ready", "failed", "retired"}


@contextlib.contextmanager
def transaction(con):
    if con.in_transaction:
        yield
        return
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS task_generations(
            generation_id TEXT PRIMARY KEY,
            sink_key TEXT NOT NULL,
            plan_version INTEGER NOT NULL,
            source_relation TEXT NOT NULL,
            fixed_w INTEGER,
            source_pin_id TEXT,
            source_pin_released INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            imported INTEGER NOT NULL DEFAULT 0,
            created REAL NOT NULL,
            updated REAL NOT NULL,
            history_staged_at REAL,
            ready_at REAL,
            UNIQUE(sink_key,plan_version));
        CREATE INDEX IF NOT EXISTS task_generations_status
            ON task_generations(status,updated);
    """)


def generation_id(sink_key, plan_version):
    sink_key = str(sink_key or "").strip()
    if not sink_key:
        raise ValueError("sink_key is required")
    plan_version = int(plan_version)
    if plan_version < 0:
        raise ValueError("plan_version cannot be negative")
    return "sink:%s:plan:%d" % (sink_key, plan_version)


def _row(row):
    if not row:
        raise KeyError("task generation does not exist")
    return dict(
        generation_id=str(row[0]),
        sink_key=str(row[1]),
        plan_version=int(row[2]),
        source_relation=str(row[3]),
        fixed_w=None if row[4] is None else int(row[4]),
        source_pin_id=None if row[5] is None else str(row[5]),
        source_pin_released=bool(row[6]),
        status=str(row[7]),
        imported=bool(row[8]),
        created=float(row[9]),
        updated=float(row[10]),
        history_staged_at=None if row[11] is None else float(row[11]),
        ready_at=None if row[12] is None else float(row[12]),
    )


def info(con, sink_key, plan_version):
    row = con.execute("""
        SELECT generation_id,sink_key,plan_version,source_relation,fixed_w,
               source_pin_id,source_pin_released,status,imported,created,
               updated,history_staged_at,ready_at
        FROM task_generations
        WHERE sink_key=? AND plan_version=?
    """, (str(sink_key),int(plan_version))).fetchone()
    return _row(row)


def maybe_info(con, sink_key, plan_version):
    try:
        return info(con,sink_key,plan_version)
    except KeyError:
        return None


def ensure_build(
        con, sink_key, plan_version, source_relation, fixed_w, source_pin_id
):
    gid = generation_id(sink_key,plan_version)
    source_relation = str(source_relation or "").strip()
    source_pin_id = str(source_pin_id or "").strip()
    fixed_w = int(fixed_w)
    if not source_relation or not source_pin_id:
        raise ValueError("source_relation and source_pin_id are required")
    if fixed_w < 0:
        raise ValueError("fixed_w cannot be negative")
    existing = maybe_info(con,sink_key,plan_version)
    if existing is not None:
        expected = (
            source_relation,fixed_w,source_pin_id
        )
        actual = (
            existing["source_relation"],
            existing["fixed_w"],
            existing["source_pin_id"],
        )
        if existing["imported"] or actual != expected:
            raise RuntimeError(
                "task generation fixed-W/source pin changed across restart "
                "expected=%r actual=%r" % (actual,expected)
            )
        if existing["status"] not in {"building","history_staged","ready"}:
            raise RuntimeError(
                "task generation cannot resume from status "
                + existing["status"])
        return existing
    now = time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO task_generations(
                generation_id,sink_key,plan_version,source_relation,fixed_w,
                source_pin_id,source_pin_released,status,imported,
                created,updated)
            VALUES(?,?,?,?,?,?,0,'building',0,?,?)
        """, (
            gid,str(sink_key),int(plan_version),source_relation,fixed_w,
            source_pin_id,now,now,
        ))
    return info(con,sink_key,plan_version)


def import_existing(
        con, sink_key, plan_version, source_relation, status
):
    status = str(status)
    if status not in {"history_staged","ready"}:
        raise ValueError("imported generation must already have staged history")
    existing = maybe_info(con,sink_key,plan_version)
    if existing is not None:
        return existing
    now = time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO task_generations(
                generation_id,sink_key,plan_version,source_relation,fixed_w,
                source_pin_id,source_pin_released,status,imported,
                created,updated,history_staged_at,ready_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            generation_id(sink_key,plan_version),
            str(sink_key),int(plan_version),str(source_relation),
            None,None,1,status,1,now,now,now,
            now if status == "ready" else None,
        ))
    return info(con,sink_key,plan_version)


def mark_history_staged(con, sink_key, plan_version):
    current = info(con,sink_key,plan_version)
    if current["status"] == "ready":
        return current
    if current["status"] == "history_staged":
        return current
    if current["status"] != "building":
        raise RuntimeError(
            "cannot stage history from generation status "
            + current["status"])
    now = time.time()
    with transaction(con):
        con.execute("""
            UPDATE task_generations
            SET status='history_staged',history_staged_at=?,updated=?
            WHERE sink_key=? AND plan_version=?
        """, (now,now,str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def mark_pin_released(con, sink_key, plan_version):
    current = info(con,sink_key,plan_version)
    if current["status"] == "building":
        raise RuntimeError(
            "cannot release fixed-W pin before history is durably staged")
    with transaction(con):
        con.execute("""
            UPDATE task_generations
            SET source_pin_released=1,updated=?
            WHERE sink_key=? AND plan_version=?
        """, (time.time(),str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def mark_ready_if_exists(con, sink_key, plan_version):
    current = maybe_info(con,sink_key,plan_version)
    if current is None:
        return None
    if current["status"] == "ready":
        return current
    if current["status"] not in {"building","history_staged"}:
        raise RuntimeError(
            "cannot publish generation from status " + current["status"])
    now = time.time()
    with transaction(con):
        con.execute("""
            UPDATE task_generations
            SET status='ready',
                history_staged_at=COALESCE(history_staged_at,?),
                ready_at=?,updated=?
            WHERE sink_key=? AND plan_version=?
        """, (now,now,now,str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def set_terminal(con, sink_key, plan_version, status):
    status = str(status)
    if status not in {"failed","retired"}:
        raise ValueError("terminal generation status must be failed or retired")
    current = info(con,sink_key,plan_version)
    if current["status"] == status:
        return current
    with transaction(con):
        con.execute("""
            UPDATE task_generations SET status=?,updated=?
            WHERE sink_key=? AND plan_version=?
        """, (status,time.time(),str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def list_generations(con, sink_key=None):
    if sink_key is None:
        rows = con.execute("""
            SELECT sink_key,plan_version FROM task_generations
            ORDER BY sink_key,plan_version
        """).fetchall()
    else:
        rows = con.execute("""
            SELECT sink_key,plan_version FROM task_generations
            WHERE sink_key=? ORDER BY plan_version
        """, (str(sink_key),)).fetchall()
    return [info(con,row[0],row[1]) for row in rows]
