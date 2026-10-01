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
    staged = tg.mark_pin_released(con,"sink-a",7)
    assert staged["source_pin_released"]

    ready = tg.mark_ready_if_exists(con,"sink-a",7)
    assert ready["status"] == "ready"
    assert ready["ready_at"] is not None
    assert tg.mark_history_staged(con,"sink-a",7)["status"] == "ready"

    imported = tg.import_existing(
        con,"sink-old",2,"db.orders","ready")
    assert imported["imported"]
    assert imported["fixed_w"] is None
    assert imported["source_pin_released"]
    assert len(tg.list_generations(con)) == 2

    retired = tg.set_terminal(con,"sink-a",7,"retired")
    assert retired["status"] == "retired"
    expect_error(
        lambda: tg.mark_ready_if_exists(con,"sink-a",7),
        RuntimeError,
    )
    con.close()
    print("task_generation_test ok", flush=True)


if __name__ == "__main__":
    main()
