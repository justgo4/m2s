#!/usr/bin/env python3
"""Contract for asynchronous durable source capture -> base apply."""
from pathlib import Path
import pickle
import sqlite3
import sys
import tempfile
import threading
import time

import pyarrow as pa

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import j4
import source_state


def schema():
    return pa.schema([
        pa.field("id",pa.int64()),
        pa.field("value",pa.string()),
    ])


def batch(start,count):
    ids=list(range(int(start),int(start)+int(count)))
    table=pa.table({
        "id":pa.array(ids,type=pa.int64()),
        "value":pa.array(
            ["v-%06d" % value for value in ids],
            type=pa.string()),
    },schema=schema())
    return table.append_column(
        "_sync_op",
        pa.array([0]*len(ids),type=pa.int8())
    ).append_column(
        "_sync_order",
        pa.array(range(len(ids)),type=pa.int64())
    )


def wait_applied(con,seq,timeout=10):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if source_state.base_applied_seq(con)>=int(seq):
            return
        time.sleep(.01)
    raise AssertionError(
        "source apply worker did not reach seq=%d "
        "durable=%d applied=%d pending_bytes=%d"
        % (
            int(seq),
            source_state.log_durable_seq(con),
            source_state.base_applied_seq(con),
            source_state.apply_pending_bytes(con),
        )
    )


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-source-apply-worker-"
    ) as directory:
        path=str(Path(directory)/"state.sqlite3")
        con=j4.init_state(path)
        source_state.register_relation(
            con,"db.events","epoch-async",
            schema(),["id"])
        source_state.stage_snapshot_batch(
            con,"db.events",batch(0,0),
            cursor=None,is_last=True)

        parts=[
            source_state.prepare_part(
                "db.events",batch(0,100)),
            source_state.prepare_part(
                "db.events",batch(100,100)),
        ]
        payload_bytes=sum(
            len(part["payload"]) for part in parts)
        assert source_state.log_commit(
            con,"epoch-async",
            ("binlog.000001",100),None,
            parts)==1
        assert source_state.base_applied_seq(con)==0
        assert source_state.apply_pending_bytes(
            con)==payload_bytes

        stop=threading.Event()
        wake=threading.Event()
        runtime=dict(
            stop=stop,
            source_apply_event=wake,
        )
        worker=threading.Thread(
            target=j4.source_state_apply_worker,
            args=(dict(state=path),runtime),
            name="source-apply-contract")
        worker.start()
        wake.set()
        wait_applied(con,1)
        assert source_state.apply_pending_bytes(con)==0
        assert source_state.status(con)[
            "apply_pending_bytes"]==0

        # Persist a second commit without waking the worker, then reconstruct
        # the byte counter exactly as an upgrade/restart from an older state.
        stop.set()
        wake.set()
        worker.join(5)
        assert not worker.is_alive()

        part=source_state.prepare_part(
            "db.events",batch(200,50))
        payload_bytes=len(part["payload"])
        assert source_state.log_commit(
            con,"epoch-async",
            ("binlog.000001",120),None,
            [part])==2
        assert source_state.apply_pending_bytes(
            con)==payload_bytes
        con.execute("""
            DELETE FROM source_state_meta
            WHERE key='apply_pending_bytes'
        """)
        source_state.install(con)
        assert source_state.apply_pending_bytes(
            con)==payload_bytes
        con.close()

        con=j4.open_state(path)
        stop=threading.Event()
        wake=threading.Event()
        runtime=dict(
            stop=stop,
            source_apply_event=wake,
        )
        worker=threading.Thread(
            target=j4.source_state_apply_worker,
            args=(dict(state=path),runtime),
            name="source-apply-restart-contract")
        worker.start()
        wake.set()
        wait_applied(con,2)
        assert source_state.apply_pending_bytes(con)==0
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE valid_to IS NULL
              AND deleted=0
        """).fetchone()[0]==250
        stop.set()
        wake.set()
        worker.join(5)
        assert not worker.is_alive()

        # Deterministic snapshot/apply interleaving: CDC is already durable,
        # but base apply is delayed while a stale historical page lands first.
        # The later apply must close that baseline version, and final complete
        # is allowed only after base_applied catches log_durable.
        source_state.register_relation(
            con,"db.race","epoch-async",
            schema(),["id"])
        cdc=source_state.prepare_part(
            "db.race",batch(1,1))
        cdc_table=source_state.decode_batch(
            cdc["payload"])
        value_index=cdc_table.schema.get_field_index(
            "value")
        cdc_table=cdc_table.set_column(
            value_index,"value",
            pa.array(["cdc-new"],type=pa.string()))
        cdc=source_state.prepare_part(
            "db.race",cdc_table)
        assert source_state.log_commit(
            con,"epoch-async",
            ("binlog.000001",140),None,
            [cdc])==3

        stale=batch(1,1)
        value_index=stale.schema.get_field_index(
            "value")
        stale=stale.set_column(
            value_index,"value",
            pa.array(["snapshot-old"],type=pa.string()))
        source_state.stage_snapshot_batch(
            con,"db.race",stale,
            cursor=(1,),is_last=False)
        before=con.execute("""
            SELECT row_payload,valid_from,valid_to
            FROM source_versions
            WHERE table_name='db.race'
              AND valid_to IS NULL
        """).fetchone()
        assert pickle.loads(before[0])==(
            1,"snapshot-old")
        assert int(before[1])==0
        assert before[2] is None

        stop=threading.Event()
        wake=threading.Event()
        runtime=dict(
            stop=stop,
            source_apply_event=wake,
        )
        worker=threading.Thread(
            target=j4.source_state_apply_worker,
            args=(dict(state=path),runtime),
            name="source-apply-snapshot-race")
        worker.start()
        wake.set()
        wait_applied(con,3)
        current=con.execute("""
            SELECT row_payload,valid_from
            FROM source_versions
            WHERE table_name='db.race'
              AND valid_to IS NULL
              AND deleted=0
        """).fetchone()
        assert pickle.loads(current[0])==(
            1,"cdc-new")
        assert int(current[1])==3
        assert con.execute("""
            SELECT valid_to
            FROM source_versions
            WHERE table_name='db.race'
              AND valid_from=0
        """).fetchone()[0]==3
        source_state.stage_snapshot_batch(
            con,"db.race",batch(0,0),
            cursor=(1,),is_last=True)
        assert source_state.relation_info(
            con,"db.race")["complete_seq"]==3
        assert con.execute("""
            SELECT COUNT(*)
            FROM source_versions
            WHERE table_name='db.race'
              AND valid_to IS NULL
        """).fetchone()[0]==1
        stop.set()
        wake.set()
        worker.join(5)
        assert not worker.is_alive()
        con.close()

        # Advisory SQLite/OSError handling remains inside state_gc_worker, but
        # an unexpected invariant failure must not silently kill only the GC
        # thread while the daemon continues without history reclamation.
        stop=threading.Event()
        runtime=dict(
            stop=stop,
            error_lock=threading.Lock(),
            errors=[],
        )
        original_sync=j4.sync_source_base_catalog
        def fail_source_gc_sync(_con):
            raise RuntimeError(
                "forced source GC invariant")
        j4.sync_source_base_catalog=fail_source_gc_sync
        try:
            worker=threading.Thread(
                target=j4.guarded_worker,
                args=(
                    j4.state_gc_worker,
                    runtime,
                    dict(
                        state=path,
                        shared_source_state=True,
                        detail_logs=False,
                    ),
                ),
                name="source-gc-fatal-contract")
            worker.start()
            worker.join(5)
            assert not worker.is_alive()
            assert stop.is_set()
            assert runtime["errors"]==[
                (
                    "state_gc_worker",
                    "forced source GC invariant",
                )
            ]
        finally:
            j4.sync_source_base_catalog=original_sync
            stop.set()

    print(
        "source_apply_worker_test ok durable_bytes "
        "async_drain restart_rebuild gc_fatal_guard",
        flush=True,
    )


if __name__=="__main__":
    main()
