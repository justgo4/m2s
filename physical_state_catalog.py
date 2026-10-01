#!/usr/bin/env python3
"""Durable P6C catalog for reusable physical state.

The catalog separates semantic compatibility, version readability and physical
reuse.  It deliberately does not choose query plans; it records the facts a
future planner must prove before sharing state.
"""
import contextlib
import json
import time
import uuid

import incremental_contract as contract


HEALTH_STATES = {"building", "ready", "degraded", "failed", "retired"}
REF_ROLES = {"owner", "consumer", "dependency"}


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
        CREATE TABLE IF NOT EXISTS physical_states(
            instance_id TEXT PRIMARY KEY,
            semantic_id TEXT NOT NULL,
            spec_json TEXT NOT NULL,
            backend TEXT NOT NULL,
            format_tag TEXT NOT NULL,
            generation INTEGER NOT NULL,
            min_readable_watermark INTEGER NOT NULL,
            watermark INTEGER NOT NULL,
            health TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created REAL NOT NULL,
            updated REAL NOT NULL);

        CREATE INDEX IF NOT EXISTS physical_states_semantic
            ON physical_states(semantic_id,health,watermark);

        CREATE TABLE IF NOT EXISTS physical_state_refs(
            instance_id TEXT NOT NULL
                REFERENCES physical_states(instance_id) ON DELETE CASCADE,
            owner_id TEXT NOT NULL,
            role TEXT NOT NULL,
            created REAL NOT NULL,
            PRIMARY KEY(instance_id,owner_id,role));

        CREATE TABLE IF NOT EXISTS physical_state_pins(
            pin_id TEXT PRIMARY KEY,
            instance_id TEXT NOT NULL
                REFERENCES physical_states(instance_id) ON DELETE CASCADE,
            owner TEXT NOT NULL,
            watermark INTEGER NOT NULL,
            created REAL NOT NULL);

        CREATE INDEX IF NOT EXISTS physical_state_pins_instance
            ON physical_state_pins(instance_id,watermark);
        CREATE UNIQUE INDEX IF NOT EXISTS physical_state_pins_owner
            ON physical_state_pins(instance_id,owner);
    """)


def _text(value, name):
    value = str(value or "").strip()
    if not value:
        raise ValueError(name + " must be non-empty")
    return value


def _json(value):
    return json.dumps(
        value or {}, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"))


def _row_info(row):
    spec = json.loads(row[2])
    semantic_id = contract.state_identity(spec)
    if semantic_id != str(row[1]):
        raise RuntimeError(
            "physical state semantic identity does not match persisted spec")
    return dict(
        instance_id=str(row[0]),
        semantic_id=semantic_id,
        spec=spec,
        backend=str(row[3]),
        format_tag=str(row[4]),
        generation=int(row[5]),
        min_readable_watermark=int(row[6]),
        watermark=int(row[7]),
        health=str(row[8]),
        metadata=json.loads(row[9]),
        created=float(row[10]),
        updated=float(row[11]),
    )


def state_info(con, instance_id):
    row = con.execute("""
        SELECT instance_id,semantic_id,spec_json,backend,format_tag,generation,
               min_readable_watermark,watermark,health,metadata_json,
               created,updated
        FROM physical_states WHERE instance_id=?
    """, (str(instance_id),)).fetchone()
    if not row:
        raise KeyError("physical state does not exist")
    return _row_info(row)


def register_state(
        con, spec, backend, format_tag, watermark,
        min_readable_watermark=None, generation=1,
        health="building", metadata=None, instance_id=None
):
    spec = contract.validate_state_spec(spec)
    semantic_id = contract.state_identity(spec)
    backend = _text(backend, "backend")
    format_tag = _text(format_tag, "format_tag")
    watermark = int(watermark)
    minimum = (
        watermark
        if min_readable_watermark is None
        else int(min_readable_watermark)
    )
    generation = int(generation)
    health = _text(health, "health")
    if health not in HEALTH_STATES:
        raise ValueError("unsupported physical state health: " + health)
    if watermark < 0 or minimum < 0 or minimum > watermark:
        raise ValueError("invalid readable watermark interval")
    if generation < 1:
        raise ValueError("generation must be >= 1")
    instance_id = _text(instance_id or uuid.uuid4().hex, "instance_id")
    now = time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO physical_states(
                instance_id,semantic_id,spec_json,backend,format_tag,generation,
                min_readable_watermark,watermark,health,metadata_json,
                created,updated)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            instance_id,semantic_id,
            contract.canonical_bytes(spec).decode("utf-8"),
            backend,format_tag,generation,minimum,watermark,health,
            _json(metadata),now,now,
        ))
    return state_info(con, instance_id)


def ensure_state(
        con, spec, backend, format_tag, watermark,
        min_readable_watermark=None, generation=1,
        health="building", metadata=None, instance_id=None
):
    instance_id = _text(instance_id, "instance_id")
    try:
        existing = state_info(con, instance_id)
    except KeyError:
        return register_state(
            con,spec,backend,format_tag,watermark,
            min_readable_watermark=min_readable_watermark,
            generation=generation,health=health,metadata=metadata,
            instance_id=instance_id)
    expected_id = contract.state_identity(spec)
    if existing["semantic_id"] != expected_id:
        raise RuntimeError("physical state instance id was reused for new semantics")
    if existing["backend"] != str(backend):
        raise RuntimeError("physical state backend changed in place")
    if existing["format_tag"] != str(format_tag):
        raise RuntimeError("physical state format changed in place")
    if existing["generation"] != int(generation):
        raise RuntimeError("physical state generation changed in place")
    if metadata is not None and existing["metadata"] != dict(metadata):
        raise RuntimeError("physical state metadata changed in place")
    return existing


def semantic_compatible(state, requested_spec):
    try:
        requested_id = contract.state_identity(requested_spec)
        actual = contract.state_identity(state["spec"])
        return actual == state["semantic_id"] == requested_id
    except (KeyError, TypeError, ValueError):
        return False


def version_readable(state, watermark):
    try:
        watermark = int(watermark)
        return (
            int(state["min_readable_watermark"]) <= watermark
            <= int(state["watermark"])
        )
    except (KeyError, TypeError, ValueError):
        return False


def physically_reusable(state, backend=None, format_tag=None):
    try:
        if state["health"] != "ready":
            return False
        if backend is not None and str(state["backend"]) != str(backend):
            return False
        if format_tag is not None and str(state["format_tag"]) != str(format_tag):
            return False
        return True
    except (KeyError, TypeError, ValueError):
        return False


def find_semantic(con, requested_spec):
    semantic_id = contract.state_identity(requested_spec)
    rows = con.execute("""
        SELECT instance_id FROM physical_states
        WHERE semantic_id=?
        ORDER BY
            CASE health
                WHEN 'ready' THEN 0
                WHEN 'degraded' THEN 1
                WHEN 'building' THEN 2
                ELSE 3
            END,
            watermark DESC,generation DESC,instance_id
    """, (semantic_id,)).fetchall()
    return [state_info(con,row[0]) for row in rows]


def set_health(con, instance_id, health):
    health = _text(health, "health")
    if health not in HEALTH_STATES:
        raise ValueError("unsupported physical state health: " + health)
    with transaction(con):
        if not con.execute(
            "SELECT 1 FROM physical_states WHERE instance_id=?",
            (str(instance_id),)
        ).fetchone():
            raise KeyError("physical state does not exist")
        con.execute("""
            UPDATE physical_states SET health=?,updated=?
            WHERE instance_id=?
        """, (health,time.time(),str(instance_id)))
    return state_info(con, instance_id)


def advance_state(
        con, instance_id, watermark, min_readable_watermark=None
):
    instance = state_info(con, instance_id)
    watermark = int(watermark)
    minimum = (
        instance["min_readable_watermark"]
        if min_readable_watermark is None
        else int(min_readable_watermark)
    )
    if watermark < instance["watermark"]:
        raise ValueError("physical state watermark cannot move backwards")
    if minimum < instance["min_readable_watermark"]:
        raise ValueError(
            "physical state minimum readable watermark cannot move backwards; "
            "create a new generation if older history is rebuilt")
    if minimum > watermark:
        raise ValueError("minimum readable watermark exceeds state watermark")
    pin = con.execute("""
        SELECT MIN(watermark) FROM physical_state_pins
        WHERE instance_id=?
    """, (str(instance_id),)).fetchone()[0]
    if pin is not None and minimum > int(pin):
        raise RuntimeError(
            "physical state compaction would cross an active fixed-W pin")
    with transaction(con):
        con.execute("""
            UPDATE physical_states
            SET min_readable_watermark=?,watermark=?,updated=?
            WHERE instance_id=?
        """, (minimum,watermark,time.time(),str(instance_id)))
    return state_info(con, instance_id)


def retain_state(con, instance_id, owner_id, role="consumer"):
    owner_id = _text(owner_id, "owner_id")
    role = _text(role, "role")
    if role not in REF_ROLES:
        raise ValueError("unsupported physical state ref role: " + role)
    state_info(con, instance_id)
    with transaction(con):
        con.execute("""
            INSERT OR IGNORE INTO physical_state_refs(
                instance_id,owner_id,role,created)
            VALUES(?,?,?,?)
        """, (str(instance_id),owner_id,role,time.time()))


def release_state(con, instance_id, owner_id, role="consumer"):
    with transaction(con):
        con.execute("""
            DELETE FROM physical_state_refs
            WHERE instance_id=? AND owner_id=? AND role=?
        """, (str(instance_id),str(owner_id),str(role)))


def state_refs(con, instance_id):
    state_info(con, instance_id)
    return [
        dict(owner_id=row[0], role=row[1], created=float(row[2]))
        for row in con.execute("""
            SELECT owner_id,role,created
            FROM physical_state_refs
            WHERE instance_id=?
            ORDER BY owner_id,role
        """, (str(instance_id),))
    ]


def pin_state(con, instance_id, owner, watermark):
    state = state_info(con, instance_id)
    instance_id = str(instance_id)
    owner = _text(owner, "owner")
    watermark = int(watermark)
    if not version_readable(state, watermark):
        raise ValueError("requested fixed-W is not readable from physical state")
    with transaction(con):
        existing = con.execute("""
            SELECT pin_id,watermark FROM physical_state_pins
            WHERE instance_id=? AND owner=?
        """, (instance_id,owner)).fetchone()
        if existing is not None:
            if int(existing[1]) != watermark:
                raise RuntimeError(
                    "physical state pin owner attempted to change fixed-W "
                    "across retry/restart")
            return dict(
                pin_id=str(existing[0]),instance_id=instance_id,
                owner=owner,watermark=watermark)
        pin_id = uuid.uuid4().hex
        con.execute("""
            INSERT INTO physical_state_pins(
                pin_id,instance_id,owner,watermark,created)
            VALUES(?,?,?,?,?)
        """, (
            pin_id,instance_id,owner,watermark,time.time(),
        ))
    return dict(
        pin_id=pin_id, instance_id=instance_id,
        owner=owner, watermark=watermark)


def release_pin(con, pin_id):
    with transaction(con):
        con.execute(
            "DELETE FROM physical_state_pins WHERE pin_id=?",
            (str(pin_id),))


def state_pins(con, instance_id):
    state_info(con, instance_id)
    return [
        dict(
            pin_id=row[0], owner=row[1],
            watermark=int(row[2]), created=float(row[3]))
        for row in con.execute("""
            SELECT pin_id,owner,watermark,created
            FROM physical_state_pins
            WHERE instance_id=?
            ORDER BY created,pin_id
        """, (str(instance_id),))
    ]


def gc_eligible(con, instance_id):
    state = state_info(con, instance_id)
    refs = con.execute("""
        SELECT COUNT(*) FROM physical_state_refs WHERE instance_id=?
    """, (str(instance_id),)).fetchone()[0]
    pins = con.execute("""
        SELECT COUNT(*) FROM physical_state_pins WHERE instance_id=?
    """, (str(instance_id),)).fetchone()[0]
    return (
        state["health"] in {"failed", "retired"}
        and int(refs) == 0
        and int(pins) == 0
    )


def delete_state(con, instance_id):
    if not gc_eligible(con, instance_id):
        raise RuntimeError("physical state is not eligible for GC")
    with transaction(con):
        con.execute(
            "DELETE FROM physical_states WHERE instance_id=?",
            (str(instance_id),))


def status(con):
    rows = con.execute("""
        SELECT instance_id FROM physical_states
        ORDER BY semantic_id,generation,instance_id
    """).fetchall()
    result = []
    for row in rows:
        info = state_info(con,row[0])
        info["refs"] = state_refs(con,row[0])
        info["pins"] = state_pins(con,row[0])
        info["gc_eligible"] = gc_eligible(con,row[0])
        result.append(info)
    return result
