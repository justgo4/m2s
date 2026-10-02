#!/usr/bin/env python3
"""Durable protocol for online replacement of one stateful sink generation.

A replacement never writes into the live logical StarRocks table while it is
building.  The new generation writes to a deterministic shadow table.  After a
single durable source frontier F is frozen, both old and new generations are
fenced at F, both targets must be visible through F, and only then may the
control plane atomically SWAP logical/shadow tables.

This module owns durable intent/phase validation only.  j4 owns StarRocks DDL,
writer routing and task lifecycle.
"""
import hashlib
import re
import time


PHASES={
    "building_shadow",
    "fencing",
    "ready_to_swap",
    "swapped",
    "cleanup",
    "complete",
    "failed",
}
TERMINAL={"complete","failed"}


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS stateful_rebuilds(
            sink_key TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            old_task_id TEXT NOT NULL,
            new_task_id TEXT NOT NULL UNIQUE,
            logical_target TEXT NOT NULL,
            shadow_target TEXT NOT NULL UNIQUE,
            original_comment TEXT NOT NULL DEFAULT '',
            frontier INTEGER,
            phase TEXT NOT NULL,
            error TEXT NOT NULL DEFAULT '',
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS stateful_rebuilds_phase
            ON stateful_rebuilds(phase,updated);
    """)
    columns={
        str(row[1])
        for row in con.execute(
            "PRAGMA table_info(stateful_rebuilds)")
    }
    if "original_comment" not in columns:
        con.execute(
            "ALTER TABLE stateful_rebuilds "
            "ADD COLUMN original_comment TEXT NOT NULL DEFAULT ''")


def _text(value,name):
    value=str(value or "").strip()
    if not value:
        raise ValueError(name+" must be non-empty")
    return value


def remote_marker(new_task_id):
    new_task_id=_text(
        new_task_id,"new_task_id")
    return "j4-rebuild-"+hashlib.sha256(
        new_task_id.encode("utf-8")).hexdigest()[:24]


def shadow_target(logical_target,new_task_id):
    logical_target=_text(
        logical_target,"logical_target")
    new_task_id=_text(
        new_task_id,"new_task_id")
    digest=hashlib.sha256(
        new_task_id.encode("utf-8")).hexdigest()[:16]
    # StarRocks table identifiers can be much longer, but keep the generated
    # name compact and deterministic so crash recovery never guesses.
    prefix=re.sub(
        r"[^0-9A-Za-z_]+","_",
        logical_target).strip("_") or "stateful"
    prefix=prefix[:40]
    return "__j4_rebuild_"+prefix+"_"+digest


def _row(row):
    if row is None:
        raise KeyError(
            "stateful rebuild intent does not exist")
    value=dict(
        sink_key=str(row[0]),
        kind=str(row[1]),
        old_task_id=str(row[2]),
        new_task_id=str(row[3]),
        logical_target=str(row[4]),
        shadow_target=str(row[5]),
        original_comment=str(row[6] or ""),
        frontier=(
            None if row[7] is None
            else int(row[7])),
        phase=str(row[8]),
        error=str(row[9] or ""),
        created=float(row[10]),
        updated=float(row[11]),
    )
    if value["phase"] not in PHASES:
        raise RuntimeError(
            "stateful rebuild has invalid durable phase")
    if value["frontier"] is not None and value["frontier"]<0:
        raise RuntimeError(
            "stateful rebuild has negative durable frontier")
    if (
        value["old_task_id"]==value["new_task_id"]
        or value["logical_target"]==value["shadow_target"]
    ):
        raise RuntimeError(
            "stateful rebuild durable identity is corrupt")
    return value


def info(con,sink_key):
    row=con.execute("""
        SELECT sink_key,kind,old_task_id,new_task_id,
               logical_target,shadow_target,original_comment,
               frontier,phase,error,created,updated
        FROM stateful_rebuilds
        WHERE sink_key=?
    """,(_text(sink_key,"sink_key"),)).fetchone()
    return _row(row)


def maybe_info(con,sink_key):
    try:
        return info(con,sink_key)
    except KeyError:
        return None


def begin(
        con,kind,sink_key,old_task_id,new_task_id,
        logical_target,shadow=None,original_comment=""
):
    kind=_text(kind,"kind")
    if kind not in {"aggregate","inner_join"}:
        raise ValueError(
            "unsupported stateful rebuild kind: "+kind)
    sink_key=_text(sink_key,"sink_key")
    old_task_id=_text(
        old_task_id,"old_task_id")
    new_task_id=_text(
        new_task_id,"new_task_id")
    logical_target=_text(
        logical_target,"logical_target")
    shadow=(
        shadow_target(logical_target,new_task_id)
        if shadow is None
        else _text(shadow,"shadow_target"))
    original_comment=str(
        original_comment or "")
    if old_task_id==new_task_id:
        raise ValueError(
            "stateful rebuild requires a new task generation")
    if logical_target==shadow:
        raise ValueError(
            "stateful rebuild shadow target must differ")
    current=maybe_info(con,sink_key)
    expected=(
        kind,old_task_id,new_task_id,
        logical_target,shadow,original_comment)
    if current is not None:
        actual=(
            current["kind"],current["old_task_id"],
            current["new_task_id"],
            current["logical_target"],
            current["shadow_target"],
            current["original_comment"])
        if actual!=expected:
            raise RuntimeError(
                "stateful rebuild identity changed across restart "
                "actual=%r expected=%r" % (
                    actual,expected))
        if current["phase"]=="failed":
            raise RuntimeError(
                "failed stateful rebuild must be explicitly cleared")
        return current
    now=time.time()
    con.execute("""
        INSERT INTO stateful_rebuilds(
            sink_key,kind,old_task_id,new_task_id,
            logical_target,shadow_target,original_comment,
            frontier,phase,error,created,updated)
        VALUES(?,?,?,?,?,?,?,NULL,'building_shadow','',?,?)
    """,(
        sink_key,kind,old_task_id,new_task_id,
        logical_target,shadow,original_comment,now,now))
    return info(con,sink_key)


def freeze_frontier(con,sink_key,frontier):
    frontier=int(frontier)
    if frontier<0:
        raise ValueError(
            "stateful rebuild frontier cannot be negative")
    current=info(con,sink_key)
    if current["phase"] in TERMINAL:
        raise RuntimeError(
            "terminal stateful rebuild cannot freeze a frontier")
    if current["frontier"] is not None:
        if int(current["frontier"])!=frontier:
            raise RuntimeError(
                "stateful rebuild frontier changed across retry/restart")
        return current
    if current["phase"]!="building_shadow":
        raise RuntimeError(
            "stateful rebuild frontier can only be frozen from building")
    con.execute("""
        UPDATE stateful_rebuilds
        SET frontier=?,phase='fencing',updated=?
        WHERE sink_key=? AND frontier IS NULL
          AND phase='building_shadow'
    """,(frontier,time.time(),current["sink_key"]))
    return info(con,sink_key)


def _advance(con,sink_key,expected,new_phase):
    current=info(con,sink_key)
    if current["phase"]==new_phase:
        return current
    if current["phase"]!=expected:
        raise RuntimeError(
            "invalid stateful rebuild phase transition "
            +current["phase"]+" -> "+new_phase)
    con.execute("""
        UPDATE stateful_rebuilds
        SET phase=?,updated=?
        WHERE sink_key=? AND phase=?
    """,(
        new_phase,time.time(),
        current["sink_key"],expected))
    return info(con,sink_key)


def mark_ready_to_swap(con,sink_key):
    current=info(con,sink_key)
    if current["frontier"] is None:
        raise RuntimeError(
            "stateful rebuild cannot swap before frontier freeze")
    return _advance(
        con,sink_key,"fencing","ready_to_swap")


def mark_swapped(con,sink_key):
    return _advance(
        con,sink_key,"ready_to_swap","swapped")


def mark_cleanup(con,sink_key):
    return _advance(
        con,sink_key,"swapped","cleanup")


def mark_complete(con,sink_key):
    return _advance(
        con,sink_key,"cleanup","complete")


def fail(con,sink_key,error):
    current=info(con,sink_key)
    if current["phase"]=="complete":
        raise RuntimeError(
            "completed stateful rebuild cannot fail")
    error=_text(error,"error")
    con.execute("""
        UPDATE stateful_rebuilds
        SET phase='failed',error=?,updated=?
        WHERE sink_key=?
    """,(error,time.time(),current["sink_key"]))
    return info(con,sink_key)


def for_task(con,task_id):
    task_id=_text(task_id,"task_id")
    row=con.execute("""
        SELECT sink_key FROM stateful_rebuilds
        WHERE (old_task_id=? OR new_task_id=?)
          AND phase NOT IN ('complete','failed')
        ORDER BY created DESC
        LIMIT 1
    """,(task_id,task_id)).fetchone()
    if row is None:
        return None
    return info(con,row[0])


def active(con):
    rows=con.execute("""
        SELECT sink_key FROM stateful_rebuilds
        WHERE phase NOT IN ('complete','failed')
        ORDER BY created,sink_key
    """).fetchall()
    return [
        info(con,row[0])
        for row in rows
    ]


def clear_terminal(con,sink_key):
    current=info(con,sink_key)
    if current["phase"] not in TERMINAL:
        raise RuntimeError(
            "non-terminal stateful rebuild cannot be cleared")
    con.execute(
        "DELETE FROM stateful_rebuilds WHERE sink_key=?",
        (current["sink_key"],))
    return True


def frontier_reached(
        rebuild,old_watermark,new_watermark,
        old_visible,new_visible
):
    """Pure readiness predicate used before remote atomic SWAP."""
    frontier=rebuild.get("frontier")
    if frontier is None:
        return False
    frontier=int(frontier)
    return (
        int(old_watermark)==frontier
        and int(new_watermark)==frontier
        and int(old_visible)>=frontier
        and int(new_visible)>=frontier
    )
