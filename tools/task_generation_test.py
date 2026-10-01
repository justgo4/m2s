#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import task_generation as tg


def expect_error(function, error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected " + error.__name__)


def main():
    con = sqlite3.connect(":memory:", isolation_level=None)
    tg.install(con)

    build = tg.ensure_build(
        con,"sink-a",7,"db.orders",120,"pin-a")
    assert build["status"] == "building"
    assert build["fixed_w"] == 120
    assert not build["source_pin_released"]

    # Same durable build can resume, but W/pin drift is forbidden.
    assert tg.ensure_build(
        con,"sink-a",7,"db.orders",120,"pin-a"
    )["generation_id"] == build["generation_id"]
    expect_error(
        lambda: tg.ensure_build(
            con,"sink-a",7,"db.orders",121,"pin-b"),
        RuntimeError,
    )
    expect_error(
        lambda: tg.mark_pin_released(con,"sink-a",7),
        RuntimeError,
    )

    staged = tg.mark_history_staged(con,"sink-a",7)
    assert staged["status"] == "history_staged"
    assert staged["history_staged_at"] is not None
    con.execute(
        "CREATE TABLE source_pins("
        "pin_id TEXT PRIMARY KEY,watermark INTEGER NOT NULL,"
        "owner TEXT NOT NULL,created REAL NOT NULL)")
    con.execute(
        "INSERT INTO source_pins VALUES('pin-a',120,'sink:sink-a:plan:7',0)")
    staged = tg.finalize_history_and_release_pin(con,"sink-a",7)
    assert staged["source_pin_released"]
    assert con.execute(
        "SELECT 1 FROM source_pins WHERE pin_id='pin-a'").fetchone() is None

    ready = tg.mark_ready_if_exists(con,"sink-a",7)
    assert ready["status"] == "ready"
    assert ready["ready_at"] is not None
    assert tg.mark_history_staged(con,"sink-a",7)["status"] == "ready"

    imported = tg.import_existing(
        con,"sink-old",2,"db.orders","ready")
    assert imported["imported"]
    assert imported["fixed_w"] is None
    assert imported["source_pin_released"]
    assert imported["status"] == "ready"
    assert imported["history_staged_at"] is not None
    assert imported["ready_at"] is not None

    imported_staged = tg.import_existing(
        con,"sink-old-staged",3,"db.orders","history_staged")
    assert imported_staged["imported"]
    assert imported_staged["status"] == "history_staged"
    assert imported_staged["history_staged_at"] is not None
    assert imported_staged["ready_at"] is None

    # Durable sink state can be ahead of lifecycle metadata after a crash.
    # Reconciliation must finalize using the original pin/W, never acquire a
    # fresh watermark.
    recovery = tg.ensure_build(
        con,"sink-recover",4,"db.orders",150,"pin-recover")
    con.execute(
        "INSERT INTO source_pins VALUES("
        "'pin-recover',150,'sink:sink-recover:plan:4',0)")
    recovered = tg.finalize_history_and_release_pin(
        con,"sink-recover",4)
    assert recovered["status"] == "history_staged"
    assert recovered["fixed_w"] == 150
    assert recovered["source_pin_released"]
    assert len(tg.list_generations(con)) == 4

    retired = tg.set_terminal(con,"sink-a",7,"retired")
    assert retired["status"] == "retired"
    expect_error(
        lambda: tg.mark_ready_if_exists(con,"sink-a",7),
        RuntimeError,
    )

    # A crash-resumable build cannot be terminalled accidentally because that
    # would either leak or incorrectly release its fixed-W retention.
    pending = tg.ensure_build(
        con,"sink-cancel",8,"db.orders",180,"pin-cancel")
    con.execute(
        "INSERT INTO source_pins VALUES("
        "'pin-cancel',180,'sink:sink-cancel:plan:8',0)")
    expect_error(
        lambda: tg.set_terminal(con,"sink-cancel",8,"failed"),
        RuntimeError,
    )
    assert tg.info(con,"sink-cancel",8)["status"] == "building"
    assert con.execute(
        "SELECT 1 FROM source_pins WHERE pin_id='pin-cancel'").fetchone()

    abandoned = tg.abandon(con,"sink-cancel",8,"failed")
    assert abandoned["status"] == "failed"
    assert abandoned["source_pin_released"]
    assert con.execute(
        "SELECT 1 FROM source_pins WHERE pin_id='pin-cancel'").fetchone() is None
    expect_error(
        lambda: tg.ensure_build(
            con,"sink-cancel",8,"db.orders",180,"pin-cancel"),
        RuntimeError,
    )
    con.close()
    print("task_generation_test ok", flush=True)


if __name__ == "__main__":
    main()
