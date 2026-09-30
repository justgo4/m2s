#!/usr/bin/env python3
"""Exercise the actual native capture recovery loop and SQLite commit boundary."""
import argparse
import contextlib
import json
from pathlib import Path
import struct
import sys
import tempfile
import threading
from unittest.mock import MagicMock, patch
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4
from binlog_contract import SPECS, header, mapping, row_event, table_map


def trace(binary, directory, case, group_size, gtid):
    specs = [SPECS[0], SPECS[1], SPECS[8]]
    prepared = mapping(specs)
    prepared.update(sql='SELECT * FROM arrow_batch', full_filter=None)
    j4.validate_mapping(prepared)
    path = str(directory / f'case_{case}.sqlite3')
    con = j4.init_state(path)
    sid = '3e11fa47-71ca-11e1-9e33-c80aa9429562'
    initial_gtid = sid + ':1-3' if gtid else None
    j4.bootstrap(con, 'synthetic', 'synthetic-source', ('binlog.000001', 4), ['events'], initial_gtid)
    con.close()
    cfg = dict(state=path, mysql=dict(database='synthetic'),
               native_binlog_path=str(binary.resolve()), query_timeout=10,
               native_event_group_events=group_size, key_partitions=4,
               batch_bytes=1024 ** 2, batch_rows=8192, max_row_bytes=1024 ** 2,
               max_backlog_bytes=64 * 1024 ** 2, txn_spool_max_bytes=64 * 1024 ** 2,
               min_free_bytes=0, retry_max=8, duckdb_memory='64MB', gtid_enabled=gtid)
    stop = threading.Event()
    plan = j4.runtime_plan_entry(1, [prepared])
    runtime = dict(stop=stop, plan_lock=threading.RLock(), active_plan_version=1,
                   plans={1: plan}, reader_ready=threading.Event(), heartbeat_count=0,
                   cdc_transactions=0, pending_plan=None)
    query_body = struct.pack('<IIBHH', 1, 0, 9, 0, 0) + b'synthetic\0BEGIN'
    start_event = header(33, b'\0' + uuid.UUID(sid).bytes + struct.pack('<Q', 4), 10) if gtid else header(2, query_body, 10)
    row = {'id': case, 'part': case % 7, 'i32': case * 10}
    raw = [start_event, table_map(specs), row_event('insert', [row], specs),
           table_map(specs), header(16, struct.pack('<Q', 1), 200)]
    opened, decoders, retries, commits = [], [], [], []
    injected = False
    real_start, real_commit = j4.native_start, j4.commit_spool

    def open_stream(config, position, durable_gtid):
        opened.append((position, durable_gtid))
        if len(opened) <= (12 if case % 17 == 0 else case % 3):
            raise j4.pymysql.err.OperationalError(2003, 'synthetic connect failure')
        return dict(log_file=position[0], log_pos=position[1], use_checksum=False, index=0)

    def start(config, maps):
        proc = real_start(config, maps)
        decoders.append(proc)
        return proc

    def read(stream):
        nonlocal injected
        index = stream['index']
        if index == 3 and not injected:
            injected = True
            if case % 2:
                decoders[-1].kill()
                decoders[-1].wait()
                # The following TABLE_MAP will detect EOF/BrokenPipe through real IPC.
            else:
                raise j4.pymysql.err.OperationalError(2013, 'synthetic mid-transaction disconnect')
        stream['index'] += 1
        packet = MagicMock()
        packet._data = b'\0' + raw[index]
        packet.is_eof_packet.return_value = False
        packet.is_ok_packet.return_value = True
        return packet

    def commit(*args, **kwargs):
        before = j4.meta_get(args[0], 'read_position')
        if before != ('binlog.000001', 4):
            raise AssertionError('uncommitted replay advanced durable cursor')
        changed = real_commit(*args, **kwargs)
        commits.append(changed)
        stop.set()
        return changed

    with contextlib.ExitStack() as stack:
        for name, replacement in [('replication_open_stream', open_stream),
                                  ('replication_read_packet', read),
                                  ('replication_close_stream', lambda _: None),
                                  ('native_start', start), ('commit_spool', commit),
                                  ('wake_loaders', lambda *args: None),
                                  ('log', lambda text: retries.append(text))]:
            stack.enter_context(patch.object(j4, name, replacement))
        stack.enter_context(patch.object(stop, 'wait', side_effect=lambda timeout: stop.is_set()))
        j4.capture_binlog_native(cfg, [prepared], runtime)
    con = j4.open_state(path)
    try:
        if j4.meta_get(con, 'read_position') != ('binlog.000001', 200):
            raise AssertionError('commit failed to advance exact durable position')
        if j4.meta_get(con, 'gtid_set') != (sid + ':1-4' if gtid else None):
            raise AssertionError('GTID checkpoint advanced incorrectly')
        if len(commits) != 1 or runtime['cdc_transactions'] != 1:
            raise AssertionError('transaction replay duplicated durable commit')
        jobs = con.execute('SELECT payload,nrows FROM jobs').fetchall()
        if sum(item[1] for item in jobs) != 1:
            raise AssertionError('transaction replay duplicated rows')
        actual = j4.arrow_job_table(prepared, jobs[0][0]).select(['id', 'part', 'i32']).to_pylist()
        if actual != [row]:
            raise AssertionError('recovered journal rows differ')
        if any(position != ('binlog.000001', 4) or checkpoint != initial_gtid for position, checkpoint in opened):
            raise AssertionError('reconnect used an in-memory cursor instead of durable state')
        if not injected or len(decoders) != 2:
            raise AssertionError('decoder was not recreated with CONFIG after disconnect')
    finally:
        con.close()
    return len(retries)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'build/native/mysql_arrow_reader')
    parser.add_argument('--cases', type=int, default=32)
    args = parser.parse_args()
    retries = 0
    with tempfile.TemporaryDirectory(prefix='m2s-recovery-') as directory:
        for case in range(args.cases):
            retries += trace(args.binary, Path(directory), case, 1 if case % 8 < 4 else 128, bool(case % 4 >= 2))
    print(json.dumps(dict(kind='actual_capture_loop_recovery', cases=args.cases,
                          source_cursor='durable_only', duplicate_commits=0,
                          decoder_restarts=args.cases, recovery_logs=retries)), flush=True)


if __name__ == '__main__':
    main()
