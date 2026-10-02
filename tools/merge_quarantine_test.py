#!/usr/bin/env python3
"""Real SQLite markers + actual writer-loop isolation, no server/network.

Unknown HTTP responses are simulated at the process_merge_lane boundary. This
proves no replay and unrelated worker progress, not remote reconciliation.
"""
from contextlib import redirect_stdout
import io
from pathlib import Path
import sys
import tempfile
import threading
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import j4


def main():
    cfg=dict(
        load_timeout=30,
        merge_commit_interval_ms=100,
        merge_commit_parallel=1,
        compression="")
    sequence_mapping=dict(
        src_table="safe",sr_table="safe",
        _target_sequence=True,
        _output_columns=["id"])
    upsert=(
        b'{"id":1,"_cdc_seq":7,"__op":0}\n'
        b'{"id":2,"_cdc_seq":8,"__op":"upsert"}\n')
    profile=j4.merge_payload_profile(
        sequence_mapping,upsert)
    assert profile==dict(
        known=True,rows=2,has_delete=False,
        target_sequence=True,
        sequence_min=7,sequence_max=8,
        sequence_guarded_upsert=True,
        replay_safe=False)
    headers=j4.merge_commit_headers(
        sequence_mapping,cfg,profile)
    assert "merge_condition" not in headers
    compressed=j4.merge_payload_profile(
        sequence_mapping,
        j4.gzip.compress(upsert,mtime=0))
    assert compressed["sequence_guarded_upsert"]
    assert not compressed["replay_safe"]
    delete_profile=j4.merge_payload_profile(
        sequence_mapping,
        b'{"id":1,"_cdc_seq":9,"__op":1}\n')
    assert delete_profile["has_delete"]
    assert not delete_profile["replay_safe"]
    assert "merge_condition" not in j4.merge_commit_headers(
        sequence_mapping,cfg,delete_profile)
    no_sequence=j4.merge_payload_profile(
        dict(
            src_table="plain",sr_table="plain",
            _target_sequence=False,
            _output_columns=["id"]),
        b'{"id":1,"__op":0}\n')
    assert not no_sequence["replay_safe"]

    with tempfile.TemporaryDirectory(prefix='m2s-safe-replay-') as directory:
        safe_path=str(Path(directory)/'state.sqlite3')
        con=j4.init_state(safe_path)
        con.execute(
            "INSERT INTO deliveries(id,table_name,lane) "
            "VALUES('safe-delivery','safe',0)")
        con.execute(
            "INSERT INTO load_parts("
            "delivery_id,part,label,payload,nrows) "
            "VALUES('safe-delivery',0,'safe-label',?,2)",
            (upsert,))
        j4.begin_merge_request(
            con,sequence_mapping,
            'safe-delivery',0,'safe-label',
            upsert,profile)
        assert con.execute(
            "SELECT replay_safe FROM merge_uncertain"
        ).fetchone()==(0,)

        # Re-registering the exact same in-flight request is idempotent, but
        # durable UNKNOWN evidence is immutable. A changed payload/label/table
        # must never overwrite the only identity we have for a request whose
        # remote outcome is unresolved.
        j4.begin_merge_request(
            con,sequence_mapping,
            'safe-delivery',0,'safe-label',
            upsert,profile)
        durable=con.execute("""
            SELECT table_name,lane,label,payload_sha256
            FROM merge_uncertain
            WHERE delivery_id='safe-delivery' AND part=0
        """).fetchone()
        try:
            j4.begin_merge_request(
                con,sequence_mapping,
                'safe-delivery',0,'safe-label',
                upsert+b'{"id":3,"_cdc_seq":9,"__op":0}\n',
                profile)
            raise AssertionError(
                "uncertain Merge Commit payload identity was overwritten")
        except RuntimeError as exc:
            assert "identity changed" in str(exc)
        assert con.execute("""
            SELECT table_name,lane,label,payload_sha256
            FROM merge_uncertain
            WHERE delivery_id='safe-delivery' AND part=0
        """).fetchone()==durable
        restored=dict(
            stop=threading.Event(),
            control_lock=threading.Lock(),
            quarantined_tables={})
        with redirect_stdout(io.StringIO()):
            assert j4.quarantine_pending_merges(
                con,restored)==1
        assert j4.merge_table_quarantined(
            restored,'safe')
        con.close()

    with tempfile.TemporaryDirectory(prefix='m2s-quarantine-') as directory:
        path = str(Path(directory)/'state.sqlite3')
        con = j4.init_state(path)
        con.execute("INSERT INTO deliveries(id,table_name,lane) VALUES('unknown','bad',0)")
        con.execute("INSERT INTO load_parts(delivery_id,part,label,payload,nrows) VALUES('unknown',0,'synthetic',?,1)", (b'{}',))
        con.close()
        runtime = dict(stop=threading.Event(), control_lock=threading.Lock(),
                       quarantined_tables={}, version_recovery={},
                       active_writers=dict(bad=1, healthy=1),
                       load_events=dict(bad=threading.Event(), healthy=threading.Event()),
                       lane_locks={('bad',0):threading.Lock(), ('healthy',0):threading.Lock()})
        errors, calls = [], dict(bad=0, healthy=0)
        def process(con, engines, handle, table, lane, cfg, state):
            calls[table] += 1
            if table == 'bad':
                j4.begin_merge_request(con, dict(src_table='bad',sr_table='bad'), 'unknown', 0, 'synthetic', b'{}')
                raise RuntimeError('synthetic response lost before durable TxnId')
            state['stop'].set()  # Test ends only after unrelated worker progresses.
            return True
        def run(table):
            try:
                j4.merge_delivery_worker(dict(src_table=table,sr_table=table),0,dict(state=path),runtime)
            except BaseException as exc:
                errors.append((table,type(exc).__name__,str(exc)))
        with redirect_stdout(io.StringIO()), patch.object(j4,'merge_candidate_lanes',return_value=[0]), patch.object(j4,'process_merge_lane',side_effect=process):
            bad = threading.Thread(target=run,args=('bad',))
            bad.start()
            deadline = time.monotonic()+5
            while not j4.merge_table_quarantined(runtime,'bad') and time.monotonic()<deadline:
                time.sleep(.01)
            try:
                assert j4.merge_table_quarantined(runtime,'bad'), errors
                assert not runtime['stop'].is_set(), 'uncertain target stopped whole pipeline'
                healthy = threading.Thread(target=run,args=('healthy',))
                healthy.start()
                healthy.join(5)
                assert not healthy.is_alive()
            finally:
                runtime['stop'].set()
                j4.wake_loaders(runtime)
                bad.join(5)
            assert not bad.is_alive()
        assert not errors, errors
        assert calls == dict(bad=1, healthy=1), calls
        con = j4.init_state(path)
        try:
            assert len(j4.unresolved_merge_uncertain(con)) == 1
            assert con.execute('SELECT visible,txn_id,payload FROM load_parts').fetchone() == (0,None,b'{}')
            restored = dict(stop=threading.Event(), control_lock=threading.Lock(), quarantined_tables={})
            with redirect_stdout(io.StringIO()):
                assert j4.quarantine_pending_merges(con,restored) == 1
            assert j4.merge_table_quarantined(restored,'bad')
            assert not j4.merge_table_quarantined(restored,'healthy')
            assert not j4.process_merge_lane(con,{},None,'bad',0,{},restored)
            assert not j4.quarantine_merge_table(con,'healthy',restored,'ordinary error')
        finally:
            con.close()
    print(
        'MERGE QUARANTINE PASS sequence-guarded upsert remains fail-closed '
        'across future delete; immutable marker retained, no replay, unrelated worker progresses',
        flush=True)


if __name__=='__main__':
    main()
