#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import incremental_contract as contract
import physical_state_catalog as catalog


def expect_error(function, error=Exception):
    try:
        function()
    except error:
        return
    raise AssertionError("expected " + error.__name__)


def main():
    con = sqlite3.connect(":memory:", isolation_level=None)
    con.execute("PRAGMA foreign_keys=ON")
    catalog.install(con)

    spec = contract.state_spec(
        "arrangement", ["mysql.orders"], [7],
        key_exprs=["customer_id"],
        value_exprs=["amount", "status"],
        predicate="status = 1",
        collation="utf8mb4_bin",
    )
    state = catalog.register_state(
        con,spec,"sqlite","arrow-v1",10,
        min_readable_watermark=0,
        generation=1,
        instance_id="state-a",
        metadata={"layout": "row-versioned"},
    )
    assert catalog.semantic_compatible(state,spec)
    assert catalog.version_readable(state,0)
    assert catalog.version_readable(state,10)
    assert not catalog.version_readable(state,11)
    assert not catalog.physically_reusable(state,"sqlite","arrow-v1")

    state = catalog.set_health(con,"state-a","ready")
    assert catalog.physically_reusable(state,"sqlite","arrow-v1")
    assert not catalog.physically_reusable(state,"rocksdb","arrow-v1")

    catalog.retain_state(con,"state-a","query-1","consumer")
    catalog.retain_state(con,"state-a","query-1","consumer")
    assert len(catalog.state_refs(con,"state-a")) == 1
    pin = catalog.pin_state(con,"state-a","build-1",5)
    resumed_pin = catalog.pin_state(con,"state-a","build-1",5)
    assert resumed_pin["pin_id"] == pin["pin_id"]
    assert len(catalog.state_pins(con,"state-a")) == 1
    expect_error(
        lambda: catalog.pin_state(con,"state-a","build-1",6),
        RuntimeError,
    )
    expect_error(
        lambda: catalog.advance_state(
            con,"state-a",20,min_readable_watermark=6),
        RuntimeError,
    )
    state = catalog.advance_state(
        con,"state-a",20,min_readable_watermark=5)
    assert state["watermark"] == 20
    assert state["min_readable_watermark"] == 5

    catalog.release_pin(con,pin["pin_id"])
    state = catalog.advance_state(
        con,"state-a",21,min_readable_watermark=6)
    assert not catalog.version_readable(state,5)
    assert catalog.version_readable(state,6)

    second = catalog.register_state(
        con,spec,"rocksdb","sst-v1",21,
        min_readable_watermark=10,
        generation=2,
        instance_id="state-b",
        health="ready",
    )
    found = catalog.find_semantic(con,spec)
    assert [item["instance_id"] for item in found] == ["state-b","state-a"]
    assert catalog.physically_reusable(second,"rocksdb","sst-v1")

    acquired = catalog.acquire_reusable_state(
        con,spec,"build-reuse",10,backend="rocksdb",format_tag="sst-v1")
    assert acquired["state"]["instance_id"] == "state-b"
    assert acquired["pin"]["watermark"] == 10
    assert acquired["ref"]["role"] == "consumer"
    resumed = catalog.acquire_reusable_state(
        con,spec,"build-reuse",10,backend="rocksdb",format_tag="sst-v1")
    assert resumed["pin"]["pin_id"] == acquired["pin"]["pin_id"]
    expect_error(
        lambda: catalog.acquire_reusable_state(
            con,spec,"build-reuse",11,backend="rocksdb",format_tag="sst-v1"),
        RuntimeError,
    )
    assert catalog.acquire_reusable_state(
        con,spec,"no-match",9,backend="sqlite",format_tag="sst-v1") is None
    assert catalog.ensure_state(
        con,spec,"rocksdb","sst-v1",21,
        min_readable_watermark=10,generation=2,
        health="ready",instance_id="state-b"
    )["instance_id"] == "state-b"
    expect_error(
        lambda: catalog.ensure_state(
            con,spec,"sqlite","sst-v1",21,
            min_readable_watermark=10,generation=2,
            health="ready",instance_id="state-b"),
        RuntimeError,
    )

    catalog.release_state(con,"state-a","query-1","consumer")
    catalog.set_health(con,"state-a","retired")
    assert catalog.gc_eligible(con,"state-a")
    catalog.delete_state(con,"state-a")
    expect_error(lambda: catalog.state_info(con,"state-a"), KeyError)

    # Pins and refs independently prevent collection of retired state.
    catalog.retain_state(con,"state-b","query-2","dependency")
    catalog.set_health(con,"state-b","retired")
    assert not catalog.gc_eligible(con,"state-b")
    catalog.release_state(con,"state-b","query-2","dependency")
    pin2 = catalog.pin_state(con,"state-b","build-2",10)
    assert not catalog.gc_eligible(con,"state-b")
    catalog.release_pin(con,pin2["pin_id"])
    assert catalog.gc_eligible(con,"state-b")

    malformed = dict(spec)
    malformed["extra"] = 1
    expect_error(lambda: contract.state_identity(malformed), ValueError)

    con.close()
    print("physical_state_catalog_test ok", flush=True)


if __name__ == "__main__":
    main()
