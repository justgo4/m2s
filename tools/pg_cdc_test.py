#!/usr/bin/env python3
"""PostgreSQL WAL insert, update, and delete match the source table."""
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pg_cdc


def main():
    if not os.environ.get("PGPASSWORD"):
        raise SystemExit("PGPASSWORD is required for the local CDC role")
    slot = "m2s_items_slot"
    conn = pg_cdc.connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS items")
            cur.execute("CREATE TABLE items (id integer PRIMARY KEY, v integer)")
            cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s", (slot,))
            if cur.fetchone():
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')",
                (slot,))
            cur.execute("INSERT INTO items (id, v) VALUES (1, 5), (2, 7)")
            cur.execute("UPDATE items SET v = 9 WHERE id = 1")
            cur.execute("DELETE FROM items WHERE id = 2")
            cur.execute("SELECT id, v FROM items ORDER BY id")
            source = {row[0]: row[1] for row in cur.fetchall()}
        lines = pg_cdc.read_slot(conn, slot)
        state = pg_cdc.apply_changes({}, lines)
        image = {key: value["v"] for key, value in state.items()}
        if image != source or image != {1: 9}:
            raise AssertionError(dict(source=source, image=image, lines=lines))
        with conn.cursor() as cur:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
    finally:
        conn.close()
    print("pg_cdc_test ok source=%s wal_rows=%d" % (image, len(lines)), flush=True)


if __name__ == "__main__":
    main()
