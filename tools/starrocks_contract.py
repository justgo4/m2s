#!/usr/bin/env python3
"""Isolated StarRocks 4.1.1 protocol probes; never use a production database."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4


def configuration():
    return dict(sr=dict(host=os.environ.get('M2S_TEST_SR_HOST', '127.0.0.1'),
                        port=int(os.environ.get('M2S_TEST_SR_QUERY_PORT', '9030')),
                        http_port=int(os.environ.get('M2S_TEST_SR_HTTP_PORT', '8030')),
                        user=os.environ.get('M2S_TEST_SR_USER', 'root'),
                        password=os.environ.get('M2S_TEST_SR_PASSWORD', ''),
                        database='m2s_contract'),
                load_timeout=30, query_timeout=10, compression='', retry_max=3,
                merge_commit_interval_ms=1000, merge_commit_parallel=2)


def connection(cfg):
    sr = cfg['sr']
    return j4.pymysql.connect(host=sr['host'], port=sr['port'], user=sr['user'],
                             password=sr['password'], autocommit=True,
                             read_timeout=10, write_timeout=10, connect_timeout=2)


def execute(cfg, sql):
    con = connection(cfg)
    try:
        with con.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall(), [item[0] for item in (cur.description or ())]
    finally:
        con.close()


def wait_ready(cfg):
    deadline = time.monotonic() + 240
    last_error = None
    probe_database = 'm2s_ready_probe'
    while time.monotonic() < deadline:
        try:
            rows, _ = execute(cfg, 'SELECT current_version()')
            version = str(rows[0][0])
            if not version.startswith('4.1.1'):
                raise ValueError('protocol contract requires exactly StarRocks 4.1.1')
            rows, names = execute(cfg, 'SHOW BACKENDS')
            alive = next(i for i, name in enumerate(names) if name.lower() == 'alive')
            if not any(str(row[alive]).lower() in ('true', '1') for row in rows):
                last_error = 'no alive backend'
                time.sleep(1)
                continue

            # FE connectivity and Alive=true can precede usable backend storage.
            # The protocol contracts need a backend that can actually allocate a
            # replication=1 tablet, so readiness is proven with the same physical
            # operation the tests require instead of guessing from SHOW BACKENDS.
            execute(cfg, 'CREATE DATABASE IF NOT EXISTS '+probe_database)
            execute(cfg, 'DROP TABLE IF EXISTS '+probe_database+'.ready')
            execute(cfg, '''CREATE TABLE '''+probe_database+'''.ready(
                    id BIGINT NOT NULL)
                    PRIMARY KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1")''')
            execute(cfg, 'DROP TABLE '+probe_database+'.ready')
            execute(cfg, 'DROP DATABASE '+probe_database)
            return version
        except j4.pymysql.MySQLError as exc:
            last_error = type(exc).__name__+': '+str(exc)[:500]
            try:
                execute(cfg, 'DROP TABLE IF EXISTS '+probe_database+'.ready')
            except j4.pymysql.MySQLError:
                pass
        time.sleep(1)
    raise RuntimeError('isolated StarRocks did not become ready: ' + str(last_error))


def request(cfg, operation, label, payload=None, extra=None):
    handle = j4.pycurl.Curl()
    try:
        headers = dict(db=cfg['sr']['database'], table='events', label=label,
                       timeout=str(cfg['load_timeout']), format='json',
                       read_json_by_line='true', columns='id,v',
                       max_filter_ratio='0', strict_mode='true', Expect='100-continue')
        headers.update(extra or {})
        return j4.curl_request(handle, cfg, j4.sr_http_base(cfg) + '/api/transaction/' + operation,
                              payload, headers, method='PUT' if operation == 'load' else 'POST')
    finally:
        handle.close()


def successful(response):
    status, detail = response
    if not 200 <= status < 300 or str(detail.get('Status', '')).upper() not in ('OK', 'SUCCESS'):
        # Status and errors are diagnostic; never emit authentication headers.
        raise RuntimeError('isolated protocol failed: ' + j4.load_result_text(detail))
    return detail


def row_count(cfg):
    rows, _ = execute(cfg, 'SELECT COUNT(*) FROM m2s_contract.events')
    return rows[0][0]


def confirm(cfg, txn_id):
    stop = threading.Event()
    timer = threading.Timer(120, stop.set)
    timer.start()
    try:
        state, _ = j4.wait_visible(cfg, txn_id, stop)
        if state != 'visible':
            raise AssertionError('isolated transaction failed to become VISIBLE')
    finally:
        timer.cancel()


def two_phase(cfg, dual=False):
    execute(cfg, 'TRUNCATE TABLE m2s_contract.events')
    label = 'contract_' + uuid.uuid4().hex
    merge = dict(enable_merge_commit='true', merge_commit_async='true',
                 merge_commit_interval_ms='1000', merge_commit_parallel='2') if dual else {}
    successful(request(cfg, 'begin', label))
    try:
        a = successful(request(cfg, 'load', label, b'{"id":1,"v":10}\n', merge))
        b = successful(request(cfg, 'load', label, b'{"id":2,"v":20}\n', merge))
        if row_count(cfg) != 0:
            raise AssertionError('2PC rows became visible before prepare/commit')
        successful(request(cfg, 'prepare', label))
        if row_count(cfg) != 0:
            raise AssertionError('2PC rows became visible before commit')
        commit = successful(request(cfg, 'commit', label))
        txn = commit.get('TxnId') or a.get('TxnId') or b.get('TxnId')
        if not txn:
            raise AssertionError('2PC response omitted transaction identity')
        confirm(cfg, int(txn))
        if row_count(cfg) != 2:
            raise AssertionError('2PC final rows differ')
        return dict(outcome='visible_after_explicit_commit', rows=2)
    finally:
        # An already committed transaction may reject rollback; never hide test errors.
        try:
            request(cfg, 'rollback', label)
        except Exception:
            pass


def merge_load(cfg, index, invalid=False):
    handle = j4.pycurl.Curl()
    try:
        headers = dict(format='json', read_json_by_line='true', columns='id,v',
                       enable_merge_commit='true', merge_commit_async='true',
                       merge_commit_interval_ms='invalid' if invalid else '1000',
                       merge_commit_parallel='2', Expect='100-continue')
        data = ('{"id":%d,"v":%d}\n' % (100 + index, index)).encode()
        return j4.curl_request(handle, cfg,
                              j4.sr_http_base(cfg) + '/api/m2s_contract/events/_stream_load',
                              data, headers)
    finally:
        handle.close()


def merged(cfg):
    execute(cfg, 'TRUNCATE TABLE m2s_contract.events')
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda i: merge_load(cfg, i), range(8)))
    details = [successful(item) for item in responses]
    txns = {int(item['TxnId']) for item in details}
    for txn in txns:
        confirm(cfg, txn)
    if row_count(cfg) != 8:
        raise AssertionError('merge async final rows differ')
    if len(txns) >= 8:
        raise AssertionError('merge was requested but no merging was observed')
    return dict(outcome='merged_and_visible', requests=8, distinct_transactions=len(txns), rows=8)


def dual_probe(cfg):
    try:
        positive = two_phase(cfg, dual=True)
    except (RuntimeError, AssertionError):
        return dict(outcome='rejected', simultaneous_mechanisms_proven=False)
    # An accepted header does not prove functionality. Compare an invalid integer
    # on both endpoints: if 2PC ignores it, this is ordinary 2PC, not combined mode.
    label = 'negative_' + uuid.uuid4().hex
    successful(request(cfg, 'begin', label))
    try:
        status, detail = request(cfg, 'load', label, b'{"id":9,"v":9}\n',
                                dict(enable_merge_commit='true', merge_commit_async='true',
                                     merge_commit_interval_ms='invalid', merge_commit_parallel='2'))
        ignored = 200 <= status < 300 and str(detail.get('Status', '')).upper() in ('OK', 'SUCCESS')
        plain_status, plain = merge_load(cfg, 9999, invalid=True)
        plain_rejects = not (200 <= plain_status < 300 and str(plain.get('Status', '')).upper() in ('OK', 'SUCCESS'))
        return dict(outcome='merge_headers_ignored_by_transaction_endpoint' if ignored and plain_rejects
                    else 'unproven', positive=positive, invalid_2pc_parameter_accepted=ignored,
                    invalid_stream_parameter_rejected=plain_rejects,
                    simultaneous_mechanisms_proven=False)
    finally:
        request(cfg, 'rollback', label)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('benchmark-results/starrocks-contract.json'))
    args = parser.parse_args()
    cfg = configuration()
    version = wait_ready(cfg)
    execute(cfg, 'CREATE DATABASE IF NOT EXISTS m2s_contract')
    execute(cfg, 'DROP TABLE IF EXISTS m2s_contract.events')
    execute(cfg, '''CREATE TABLE m2s_contract.events(id BIGINT NOT NULL,v BIGINT)
            PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")''')
    report = dict(format_version=1, kind='isolated_starrocks_protocol_contract', version=version,
                  server_configuration='image_defaults', transaction=two_phase(cfg),
                  merge_async=merged(cfg), combined=dual_probe(cfg))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
