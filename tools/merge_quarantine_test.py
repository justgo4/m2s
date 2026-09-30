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
    print('MERGE QUARANTINE PASS durable marker retained, no replay, unrelated worker progresses',flush=True)


if __name__=='__main__':
    main()
