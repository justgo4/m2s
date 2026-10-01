#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4
import physical_state_catalog
import source_state


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("value",pa.large_string()),
    ])


def batch(rows):
    table = pa.Table.from_pylist(
        [dict(id=row[0],value=row[1]) for row in rows],
        schema=schema())
    return table.append_column(
        "_sync_op",pa.array([0]*len(rows),type=pa.int8())
    ).append_column(
        "_sync_order",pa.array(range(len(rows)),type=pa.int64()))


def main():
    with tempfile.TemporaryDirectory(prefix="m2s-base-catalog-") as td:
        con = j4.init_state(str(Path(td)/"state.sqlite3"))
        source_state.register_relation(
            con,"db.orders","source-1",schema(),["id"])

        building = j4.sync_source_base_catalog(con)[0]
        assert building["health"] == "building"
        assert building["watermark"] == 0
        assert not physical_state_catalog.physically_reusable(building)

        source_state.stage_snapshot_batch(
            con,"db.orders",batch([(1,"a"),(2,"b")]),
            cursor=(2,),is_last=True)
        ready = j4.sync_source_base_catalog(con)[0]
        assert ready["health"] == "ready"
        assert ready["min_readable_watermark"] == 0
        assert physical_state_catalog.version_readable(ready,0)
        assert physical_state_catalog.physically_reusable(
            ready,"sqlite-source-state","source-state-v1")

        part = source_state.prepare_part(
            "db.orders",batch([(1,"a2")]))
        assert source_state.log_commit(
            con,"source-1",("binlog.000001",100),None,[part]) == 1
        assert source_state.apply_pending(con) == 1
        ready = j4.sync_source_base_catalog(con)[0]
        assert ready["watermark"] == 1
        assert physical_state_catalog.version_readable(ready,0)
        assert physical_state_catalog.version_readable(ready,1)

        pin = source_state.acquire_pin(
            con,"build-q1",["db.orders"])
        assert pin["watermark"] == 1
        assert source_state.gc(con)["floor"] == 1
        ready = j4.sync_source_base_catalog(con)[0]
        assert ready["min_readable_watermark"] == 1
        assert ready["metadata"]["pin_authority"] == "source_state"
        source_state.release_pin(con,pin["pin_id"])

        rows = physical_state_catalog.status(con)
        assert len(rows) == 1
        assert rows[0]["refs"][0]["role"] == "owner"
        assert rows[0]["semantic_id"] == ready["semantic_id"]
        con.close()

    print("source_base_catalog_test ok", flush=True)


if __name__ == "__main__":
    main()
