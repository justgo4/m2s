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
        CREATE TABLE IF NOT EXISTS task_generation_sources(
            generation_id TEXT NOT NULL
                REFERENCES task_generations(generation_id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            source_relation TEXT NOT NULL,
            PRIMARY KEY(generation_id,ordinal),
            UNIQUE(generation_id,source_relation));
        CREATE INDEX IF NOT EXISTS task_generation_sources_relation
            ON task_generation_sources(source_relation,generation_id);
        INSERT OR IGNORE INTO task_generation_sources(
            generation_id,ordinal,source_relation)
        SELECT generation_id,0,source_relation
        FROM task_generations;
    """)


def normalize_source_relations(source_relations):
    values = [
        str(value or "").strip()
        for value in source_relations or ()
    ]
    if not values or any(not value for value in values):
        raise ValueError("source_relations must be non-empty")
    if len(values) != len(set(values)):
        raise ValueError("source_relations must be unique and ordered")
    return values


def source_relations(con, sink_key, plan_version):
    current = info(con,sink_key,plan_version)
    rows = con.execute("""
        SELECT source_relation
        FROM task_generation_sources
        WHERE generation_id=?
        ORDER BY ordinal
    """,(current["generation_id"],)).fetchall()
    values = [str(row[0]) for row in rows]
    if not values:
        values = [current["source_relation"]]
    if values[0] != current["source_relation"]:
        raise RuntimeError(
            "task generation primary source differs from source set")
    if len(values) != len(set(values)):
        raise RuntimeError(
            "task generation source set is not unique")
    return values


def _bind_sources_locked(con, generation_id_value, source_relations_value):
    expected = normalize_source_relations(source_relations_value)
    rows = con.execute("""
        SELECT source_relation
        FROM task_generation_sources
        WHERE generation_id=?
        ORDER BY ordinal
    """,(str(generation_id_value),)).fetchall()
    actual = [str(row[0]) for row in rows]
    if actual:
        if actual != expected:
            raise RuntimeError(
                "task generation source set changed across restart "
                "expected=%r actual=%r" % (actual,expected))
        return actual
    for ordinal,relation in enumerate(expected):
        con.execute("""
            INSERT INTO task_generation_sources(
                generation_id,ordinal,source_relation)
            VALUES(?,?,?)
        """,(str(generation_id_value),int(ordinal),relation))
    return expected


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


def ensure_build_multi(
        con, sink_key, plan_version, source_relations_value, fixed_w, source_pin_id
):
    gid = generation_id(sink_key,plan_version)
    relations = normalize_source_relations(source_relations_value)
    source_pin_id = str(source_pin_id or "").strip()
    fixed_w = int(fixed_w)
    if not source_pin_id:
        raise ValueError("source_pin_id is required")
    if fixed_w < 0:
        raise ValueError("fixed_w cannot be negative")
    existing = maybe_info(con,sink_key,plan_version)
    if existing is not None:
        actual_relations = source_relations(
            con,sink_key,plan_version)
        expected = (
            relations,fixed_w,source_pin_id
        )
        actual = (
            actual_relations,
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
            gid,str(sink_key),int(plan_version),relations[0],fixed_w,
            source_pin_id,now,now,
        ))
        _bind_sources_locked(con,gid,relations)
    return info(con,sink_key,plan_version)


def ensure_build(
        con, sink_key, plan_version, source_relation, fixed_w, source_pin_id
):
    source_relation = str(source_relation or "").strip()
    if not source_relation:
        raise ValueError("source_relation is required")
    return ensure_build_multi(
        con,sink_key,plan_version,[source_relation],
        fixed_w,source_pin_id)


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


def finalize_history_and_release_pin(con, sink_key, plan_version):
    """Atomically reconcile durable staged history and release fixed-W retention.

    The caller must invoke this only after the sink's final historical snapshot
    chunk is durably staged.  Deleting source_pins and changing the generation
    lifecycle happen in the same SQLite transaction, eliminating a crash window
    where a restart could see a missing pin but a still-building generation.
    """
    current = info(con,sink_key,plan_version)
    if current["imported"]:
        if current["status"] not in {"history_staged","ready"}:
            raise RuntimeError("imported generation has invalid staged status")
        return current
    if current["status"] == "ready" and current["source_pin_released"]:
        return current
    if current["status"] not in {"building","history_staged","ready"}:
        raise RuntimeError(
            "cannot finalize history from generation status "
            + current["status"])
    pin_id = current["source_pin_id"]
    if not pin_id:
        raise RuntimeError("fixed-W generation has no durable source pin")
    now = time.time()
    with transaction(con):
        pin = con.execute("""
            SELECT watermark FROM source_pins WHERE pin_id=?
        """, (pin_id,)).fetchone()
        if pin is not None and int(pin[0]) != int(current["fixed_w"]):
            raise RuntimeError(
                "fixed-W source pin watermark changed before release")
        # A missing pin is acceptable only when a prior atomic finalize already
        # committed. If metadata still says unreleased, fail closed rather than
        # silently acquiring a different W.
        if pin is None and not current["source_pin_released"]:
            raise RuntimeError(
                "fixed-W source pin disappeared before atomic generation finalize")
        con.execute("DELETE FROM source_pins WHERE pin_id=?", (pin_id,))
        con.execute("""
            UPDATE task_generations
            SET status=CASE
                    WHEN status='building' THEN 'history_staged'
                    ELSE status
                END,
                history_staged_at=COALESCE(history_staged_at,?),
                source_pin_released=1,
                updated=?
            WHERE sink_key=? AND plan_version=?
        """, (now,now,str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def mark_ready_if_exists(con, sink_key, plan_version):
    current = maybe_info(con,sink_key,plan_version)
    if current is None:
        return None
    if current["status"] == "ready":
        if not current["source_pin_released"]:
            raise RuntimeError(
                "ready generation still retains a fixed-W source pin")
        return current
    if current["status"] != "history_staged":
        raise RuntimeError(
            "cannot publish generation before durable history_staged; status="
            + current["status"])
    if not current["source_pin_released"]:
        raise RuntimeError(
            "cannot publish generation before fixed-W source pin is released")
    if current["history_staged_at"] is None:
        raise RuntimeError(
            "history_staged generation lacks durable staged timestamp")
    now = time.time()
    with transaction(con):
        con.execute("""
            UPDATE task_generations
            SET status='ready',ready_at=?,updated=?
            WHERE sink_key=? AND plan_version=?
        """, (now,now,str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def abandon(con, sink_key, plan_version, status="failed"):
    """Explicitly give up recovery and release this generation's fixed-W pin.

    Normal crash/restart must never call this: an unfinished generation keeps
    its original pin so it can resume the same W.  Cancellation/removal may call
    abandon only when the caller intentionally discards that generation.
    """
    status = str(status)
    if status not in {"failed","retired"}:
        raise ValueError("abandoned generation status must be failed or retired")
    current = info(con,sink_key,plan_version)
    if current["status"] in {"failed","retired"}:
        return current
    now = time.time()
    with transaction(con):
        if not current["source_pin_released"] and not current["imported"]:
            pin_id = current["source_pin_id"]
            if not pin_id:
                raise RuntimeError(
                    "unreleased fixed-W generation has no source pin id")
            pin = con.execute("""
                SELECT watermark FROM source_pins WHERE pin_id=?
            """, (pin_id,)).fetchone()
            if pin is None:
                raise RuntimeError(
                    "fixed-W source pin disappeared before explicit abandon")
            if int(pin[0]) != int(current["fixed_w"]):
                raise RuntimeError(
                    "fixed-W source pin watermark changed before abandon")
            con.execute("DELETE FROM source_pins WHERE pin_id=?", (pin_id,))
        con.execute("""
            UPDATE task_generations
            SET status=?,source_pin_released=1,updated=?
            WHERE sink_key=? AND plan_version=?
        """, (status,now,str(sink_key),int(plan_version)))
    return info(con,sink_key,plan_version)


def set_terminal(con, sink_key, plan_version, status):
    status = str(status)
    if status not in {"failed","retired"}:
        raise ValueError("terminal generation status must be failed or retired")
    current = info(con,sink_key,plan_version)
    if current["status"] == status:
        return current
    if not current["source_pin_released"]:
        raise RuntimeError(
            "cannot terminate a generation with an unreleased fixed-W pin; "
            "resume it or explicitly abandon it")
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
