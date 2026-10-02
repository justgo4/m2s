#!/usr/bin/env python3
"""Durable cohort protocol for coordinated stateful semantic rebuilds.

A cohort groups multiple independent sink rebuilds that belong to one catalog
plan.  Every member must be ready before one common source frontier can be
frozen.  All member rebuild rows are durably advanced to ready_to_swap before
any caller may begin remote swaps.  Remote swaps can still be individually
visible because StarRocks does not provide a cross-table atomic swap; the
cohort phase makes partial progress explicit and restart-recoverable.
"""
import hashlib
import time

import stateful_rebuild


PHASES={
    "building",
    "fencing",
    "ready_to_swap",
    "swapping",
    "cleanup",
    "complete",
    "failed",
}
TERMINAL={"complete","failed"}


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS stateful_rebuild_cohorts(
            cohort_id TEXT PRIMARY KEY,
            plan_version INTEGER NOT NULL,
            frontier INTEGER,
            phase TEXT NOT NULL,
            error TEXT NOT NULL DEFAULT '',
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS stateful_rebuild_cohort_members(
            cohort_id TEXT NOT NULL,
            sink_key TEXT NOT NULL UNIQUE,
            ordinal INTEGER NOT NULL,
            ready INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(cohort_id,sink_key),
            FOREIGN KEY(cohort_id)
                REFERENCES stateful_rebuild_cohorts(cohort_id)
                ON DELETE CASCADE);
        CREATE INDEX IF NOT EXISTS stateful_rebuild_cohort_phase
            ON stateful_rebuild_cohorts(phase,updated);
        CREATE INDEX IF NOT EXISTS stateful_rebuild_cohort_member_order
            ON stateful_rebuild_cohort_members(cohort_id,ordinal);
    """)


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def identity(plan_version,sink_keys):
    plan_version=int(plan_version)
    if plan_version<0:
        raise ValueError(
            "stateful rebuild cohort plan_version cannot be negative")
    sinks=sorted({
        _text(sink,"sink_key")
        for sink in sink_keys
    })
    if not sinks:
        raise ValueError(
            "stateful rebuild cohort requires at least one sink")
    payload=(
        str(plan_version)+"\n"+"\n".join(sinks)
    ).encode("utf-8")
    return "rebuild-cohort-"+hashlib.sha256(
        payload).hexdigest()[:24]


def _row(row):
    if row is None:
        raise KeyError(
            "stateful rebuild cohort does not exist")
    value=dict(
        cohort_id=str(row[0]),
        plan_version=int(row[1]),
        frontier=(
            None if row[2] is None
            else int(row[2])),
        phase=str(row[3]),
        error=str(row[4] or ""),
        created=float(row[5]),
        updated=float(row[6]),
    )
    if value["phase"] not in PHASES:
        raise RuntimeError(
            "stateful rebuild cohort has invalid durable phase")
    if (
        value["frontier"] is not None
        and value["frontier"]<0
    ):
        raise RuntimeError(
            "stateful rebuild cohort has negative frontier")
    return value


def info(con,cohort_id):
    cohort_id=_text(
        cohort_id,"cohort_id")
    row=con.execute("""
        SELECT cohort_id,plan_version,frontier,
               phase,error,created,updated
        FROM stateful_rebuild_cohorts
        WHERE cohort_id=?
    """,(cohort_id,)).fetchone()
    value=_row(row)
    value["members"]=[
        dict(
            sink_key=str(member[0]),
            ordinal=int(member[1]),
            ready=bool(member[2]),
        )
        for member in con.execute("""
            SELECT sink_key,ordinal,ready
            FROM stateful_rebuild_cohort_members
            WHERE cohort_id=?
            ORDER BY ordinal,sink_key
        """,(cohort_id,)).fetchall()
    ]
    return value


def maybe_info(con,cohort_id):
    try:
        return info(con,cohort_id)
    except KeyError:
        return None


def begin(con,plan_version,sink_keys):
    plan_version=int(plan_version)
    sinks=sorted({
        _text(sink,"sink_key")
        for sink in sink_keys
    })
    cohort_id=identity(
        plan_version,sinks)
    current=maybe_info(
        con,cohort_id)
    if current is not None:
        actual=[
            item["sink_key"]
            for item in current["members"]
        ]
        if (
            current["plan_version"]!=plan_version
            or actual!=sinks
        ):
            raise RuntimeError(
                "stateful rebuild cohort identity changed across retry")
        if current["phase"]=="failed":
            raise RuntimeError(
                "failed stateful rebuild cohort cannot be revived")
        return current

    now=time.time()
    con.execute("""
        INSERT OR IGNORE INTO stateful_rebuild_cohorts(
            cohort_id,plan_version,frontier,
            phase,error,created,updated)
        VALUES(?,?,NULL,'building','',?,?)
    """,(cohort_id,plan_version,now,now))
    for ordinal,sink in enumerate(sinks):
        con.execute("""
            INSERT OR IGNORE INTO stateful_rebuild_cohort_members(
                cohort_id,sink_key,ordinal,ready)
            VALUES(?,?,?,0)
        """,(cohort_id,sink,ordinal))
        owner=con.execute("""
            SELECT cohort_id
            FROM stateful_rebuild_cohort_members
            WHERE sink_key=?
        """,(sink,)).fetchone()
        if (
            owner is None
            or str(owner[0])!=cohort_id
        ):
            raise RuntimeError(
                "stateful rebuild sink already belongs to another cohort: "
                +sink)

    current=info(
        con,cohort_id)
    actual=[
        item["sink_key"]
        for item in current["members"]
    ]
    if actual!=sinks:
        raise RuntimeError(
            "stateful rebuild cohort member set is incomplete")
    return current


def for_sink(con,sink_key):
    row=con.execute("""
        SELECT m.cohort_id
        FROM stateful_rebuild_cohort_members m
        JOIN stateful_rebuild_cohorts c
          ON c.cohort_id=m.cohort_id
        WHERE m.sink_key=?
          AND c.phase NOT IN ('complete','failed')
        LIMIT 2
    """,(_text(sink_key,"sink_key"),)).fetchall()
    if not row:
        return None
    if len(row)>1:
        raise RuntimeError(
            "stateful rebuild sink has multiple active cohorts")
    return info(con,row[0][0])


def mark_member_ready(con,cohort_id,sink_key):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]!="building":
        raise RuntimeError(
            "stateful rebuild cohort member readiness is closed in phase "
            +cohort["phase"])
    sink_key=_text(
        sink_key,"sink_key")
    changed=con.execute("""
        UPDATE stateful_rebuild_cohort_members
        SET ready=1
        WHERE cohort_id=? AND sink_key=?
    """,(cohort["cohort_id"],sink_key)).rowcount
    if not changed:
        raise KeyError(
            "stateful rebuild cohort member does not exist")
    return info(
        con,cohort["cohort_id"])


def ready(con,cohort_id):
    cohort=info(
        con,cohort_id)
    return bool(
        cohort["members"]
        and all(
            item["ready"]
            for item in cohort["members"]))


def _savepoint(con,name,fn):
    con.execute("SAVEPOINT "+name)
    try:
        value=fn()
        con.execute("RELEASE "+name)
        return value
    except BaseException:
        con.execute("ROLLBACK TO "+name)
        con.execute("RELEASE "+name)
        raise


def freeze(con,cohort_id,frontier):
    frontier=int(frontier)
    if frontier<0:
        raise ValueError(
            "stateful rebuild cohort frontier cannot be negative")
    cohort=info(
        con,cohort_id)
    if cohort["frontier"] is not None:
        if cohort["frontier"]!=frontier:
            raise RuntimeError(
                "stateful rebuild cohort frontier changed across retry")
        return cohort
    if cohort["phase"]!="building":
        raise RuntimeError(
            "stateful rebuild cohort can only freeze from building")
    if not ready(
        con,cohort["cohort_id"]):
        raise RuntimeError(
            "stateful rebuild cohort cannot freeze before every member is ready")

    def apply():
        for member in cohort["members"]:
            stateful_rebuild.freeze_frontier(
                con,member["sink_key"],frontier)
        changed=con.execute("""
            UPDATE stateful_rebuild_cohorts
            SET frontier=?,phase='fencing',updated=?
            WHERE cohort_id=?
              AND frontier IS NULL
              AND phase='building'
        """,(
            frontier,time.time(),
            cohort["cohort_id"])).rowcount
        durable=info(
            con,cohort["cohort_id"])
        if (
            durable["frontier"]==frontier
            and durable["phase"]=="fencing"
        ):
            return durable
        if not changed:
            raise RuntimeError(
                "stateful rebuild cohort changed concurrently while freezing")
        raise RuntimeError(
            "stateful rebuild cohort frontier freeze was not durable")

    return _savepoint(
        con,"stateful_rebuild_cohort_freeze",apply)


def mark_ready_to_swap(con,cohort_id):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]=="ready_to_swap":
        return cohort
    if (
        cohort["phase"]!="fencing"
        or cohort["frontier"] is None
    ):
        raise RuntimeError(
            "stateful rebuild cohort is not fenced")

    def apply():
        for member in cohort["members"]:
            rebuild=stateful_rebuild.info(
                con,member["sink_key"])
            if rebuild["frontier"]!=cohort["frontier"]:
                raise RuntimeError(
                    "stateful rebuild member frontier differs from cohort")
            if rebuild["phase"]=="fencing":
                stateful_rebuild.mark_ready_to_swap(
                    con,member["sink_key"])
            elif rebuild["phase"]!="ready_to_swap":
                raise RuntimeError(
                    "stateful rebuild member is not ready to swap: "
                    +member["sink_key"])
        changed=con.execute("""
            UPDATE stateful_rebuild_cohorts
            SET phase='ready_to_swap',updated=?
            WHERE cohort_id=? AND phase='fencing'
        """,(
            time.time(),cohort["cohort_id"])).rowcount
        durable=info(
            con,cohort["cohort_id"])
        if durable["phase"]=="ready_to_swap":
            return durable
        if not changed:
            raise RuntimeError(
                "stateful rebuild cohort changed concurrently before swap")
        raise RuntimeError(
            "stateful rebuild cohort ready transition was not durable")

    return _savepoint(
        con,"stateful_rebuild_cohort_ready",apply)


def begin_swap(con,cohort_id):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]=="swapping":
        return cohort
    if cohort["phase"]!="ready_to_swap":
        raise RuntimeError(
            "stateful rebuild cohort cannot swap from phase "
            +cohort["phase"])
    for member in cohort["members"]:
        rebuild=stateful_rebuild.info(
            con,member["sink_key"])
        if (
            rebuild["phase"]!="ready_to_swap"
            or rebuild["frontier"]!=cohort["frontier"]
        ):
            raise RuntimeError(
                "stateful rebuild cohort member lost swap readiness: "
                +member["sink_key"])
    con.execute("""
        UPDATE stateful_rebuild_cohorts
        SET phase='swapping',updated=?
        WHERE cohort_id=? AND phase='ready_to_swap'
    """,(time.time(),cohort["cohort_id"]))
    durable=info(
        con,cohort["cohort_id"])
    if durable["phase"]!="swapping":
        raise RuntimeError(
            "stateful rebuild cohort swap gate was not durable")
    return durable


def mark_member_swapped(con,cohort_id,sink_key):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]!="swapping":
        raise RuntimeError(
            "stateful rebuild cohort is not swapping")
    sink_key=_text(
        sink_key,"sink_key")
    if sink_key not in {
        item["sink_key"]
        for item in cohort["members"]
    }:
        raise KeyError(
            "stateful rebuild cohort member does not exist")
    rebuild=stateful_rebuild.info(
        con,sink_key)
    if rebuild["phase"]=="ready_to_swap":
        return stateful_rebuild.mark_swapped(
            con,sink_key)
    if rebuild["phase"]=="swapped":
        return rebuild
    raise RuntimeError(
        "stateful rebuild member cannot mark swapped from phase "
        +rebuild["phase"])


def all_swapped(con,cohort_id):
    cohort=info(
        con,cohort_id)
    return all(
        stateful_rebuild.info(
            con,item["sink_key"])["phase"]
        in {"swapped","cleanup","complete"}
        for item in cohort["members"]
    )


def mark_cleanup(con,cohort_id):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]=="cleanup":
        return cohort
    if cohort["phase"]!="swapping":
        raise RuntimeError(
            "stateful rebuild cohort cannot cleanup from phase "
            +cohort["phase"])
    if not all_swapped(
        con,cohort["cohort_id"]):
        raise RuntimeError(
            "stateful rebuild cohort cannot cleanup before all swaps")

    def apply():
        for member in cohort["members"]:
            rebuild=stateful_rebuild.info(
                con,member["sink_key"])
            if rebuild["phase"]=="swapped":
                stateful_rebuild.mark_cleanup(
                    con,member["sink_key"])
            elif rebuild["phase"] not in {
                "cleanup","complete"
            }:
                raise RuntimeError(
                    "stateful rebuild member cannot enter cleanup: "
                    +member["sink_key"])
        con.execute("""
            UPDATE stateful_rebuild_cohorts
            SET phase='cleanup',updated=?
            WHERE cohort_id=? AND phase='swapping'
        """,(
            time.time(),cohort["cohort_id"]))
        durable=info(
            con,cohort["cohort_id"])
        if durable["phase"]!="cleanup":
            raise RuntimeError(
                "stateful rebuild cohort cleanup transition was not durable")
        return durable

    return _savepoint(
        con,"stateful_rebuild_cohort_cleanup",apply)


def mark_complete(con,cohort_id):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]=="complete":
        return cohort
    if cohort["phase"]!="cleanup":
        raise RuntimeError(
            "stateful rebuild cohort cannot complete from phase "
            +cohort["phase"])

    def apply():
        for member in cohort["members"]:
            rebuild=stateful_rebuild.info(
                con,member["sink_key"])
            if rebuild["phase"]=="cleanup":
                stateful_rebuild.mark_complete(
                    con,member["sink_key"])
            elif rebuild["phase"]!="complete":
                raise RuntimeError(
                    "stateful rebuild member cannot complete: "
                    +member["sink_key"])
        con.execute("""
            UPDATE stateful_rebuild_cohorts
            SET phase='complete',updated=?
            WHERE cohort_id=? AND phase='cleanup'
        """,(
            time.time(),cohort["cohort_id"]))
        durable=info(
            con,cohort["cohort_id"])
        if durable["phase"]!="complete":
            raise RuntimeError(
                "stateful rebuild cohort completion was not durable")
        return durable

    return _savepoint(
        con,"stateful_rebuild_cohort_complete",apply)


def fail(con,cohort_id,error):
    cohort=info(
        con,cohort_id)
    if cohort["phase"]=="complete":
        raise RuntimeError(
            "completed stateful rebuild cohort cannot fail")
    error=_text(
        error,"error")
    if cohort["phase"]=="failed":
        if cohort["error"]!=error:
            raise RuntimeError(
                "failed rebuild cohort error changed across retry")
        return cohort
    con.execute("""
        UPDATE stateful_rebuild_cohorts
        SET phase='failed',error=?,updated=?
        WHERE cohort_id=? AND phase=?
    """,(
        error,time.time(),cohort["cohort_id"],
        cohort["phase"]))
    durable=info(
        con,cohort["cohort_id"])
    if durable["phase"]!="failed":
        raise RuntimeError(
            "stateful rebuild cohort failure transition was not durable")
    if durable["error"]!=error:
        raise RuntimeError(
            "stateful rebuild cohort failed with a different error")
    return durable


def active(con):
    rows=con.execute("""
        SELECT cohort_id
        FROM stateful_rebuild_cohorts
        WHERE phase NOT IN ('complete','failed')
        ORDER BY created,cohort_id
    """).fetchall()
    return [
        info(con,row[0])
        for row in rows
    ]
