#!/usr/bin/env python3
"""Add a nullable column on PostgreSQL and refuse a silent drop."""
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pg_cdc
import stream_connector


def main():
    if not os.environ.get("PGPASSWORD"):
        raise SystemExit("PGPASSWORD is required")
    slot = "m2s_evolve_slot"
    conn = pg_cdc.connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS evolve")
            cur.execute("CREATE TABLE evolve (id integer PRIMARY KEY, v integer)")
            cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s", (slot,))
            if cur.fetchone():
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')",
                (slot,))
            cur.execute("ALTER TABLE evolve ADD COLUMN note text")
            cur.execute("INSERT INTO evolve (id, v, note) VALUES (3, 1, 'east')")
            cur.execute("SELECT id, v, note FROM evolve")
            source = cur.fetchall()
        lines = pg_cdc.read_slot(conn, slot)
        state = pg_cdc.apply_changes({}, lines)
        if state.get(3) != {"id": 3, "v": 1, "note": "east"}:
            raise AssertionError(dict(state=state, lines=lines, source=source))
        decision = stream_connector.plan_ddl(dict(action="drop", column="note"))
        if decision["propagate"] or not decision["rebuild"] or decision["drop_rows"]:
            raise AssertionError(decision)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM evolve")
            if cur.fetchone()[0] != 1:
                raise AssertionError("planning a drop removed a row")
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
    finally:
        conn.close()
    print("pg_ddl_test ok added=note rows=1 rebuild_on_drop=1", flush=True)


if __name__ == "__main__":
    main()
