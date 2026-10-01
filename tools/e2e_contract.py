#!/usr/bin/env python3
"""Disposable MySQL -> actual j4 daemon -> StarRocks correctness/restart probe.

Drops only m2s_e2e_contract on both isolated servers. Explicit --isolated required.
Runtime state/logs/credentials stay in a temporary directory and are not artifacts.
This is a short functional test, not a 50M/72h performance certification.
"""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4
from starrocks_contract import configuration, execute, wait_ready

DATABASE = 'm2s_e2e_contract'
RUNTIME_LOG = None
QUERY = 'SELECT id,part,v*2 AS v,trim(note) AS note,amount FROM mysql.events WHERE active=1'


def source_options():
    return dict(host=os.environ.get('M2S_TEST_MYSQL_HOST', '127.0.0.1'),
                port=int(os.environ.get('M2S_TEST_MYSQL_PORT', '3306')),
                user=os.environ.get('M2S_TEST_MYSQL_USER', 'root'),
                password=os.environ.get('M2S_TEST_MYSQL_PASSWORD', ''),
                charset='utf8mb4', autocommit=True, connect_timeout=3,
                read_timeout=15, write_timeout=15)


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def setup_catalog(directory, cfg, source, mode, group, shared_source_state=False):
    variables = dict(CDC_MYSQL_HOST=source['host'], CDC_MYSQL_PORT=source['port'],
                     CDC_MYSQL_USER=source['user'], CDC_MYSQL_PASSWORD=source['password'],
                     CDC_MYSQL_SCHEMA=DATABASE, CDC_SR_FE_HOST=cfg['sr']['host'],
                     CDC_SR_FE_PORT=cfg['sr']['http_port'], CDC_SR_QUERY_PORT=cfg['sr']['port'],
                     CDC_SR_USER=cfg['sr']['user'], CDC_SR_PASSWORD=cfg['sr']['password'],
                     CDC_SR_DB=DATABASE, CDC_SERVER_ID=188611,
                     CDC_STATE_FILE=str(directory / 'state.sqlite3'), CDC_LOAD_MODE=mode,
                     CDC_NATIVE_BINLOG_PATH=str(ROOT / 'build/native/mysql_arrow_reader'),
                     CDC_NATIVE_EVENT_GROUP_EVENTS=group, CDC_KEY_PARTITIONS=4,
                     CDC_WRITE_WORKERS_MIN=1, CDC_WRITE_WORKERS_INITIAL=2,
                     CDC_WRITE_WORKERS_MAX=2, CDC_SNAPSHOT_WORKERS=1,
                     CDC_SNAPSHOT_ROWS=64, CDC_SNAPSHOT_READ_AHEAD_GROUPS=1,
                     CDC_SNAPSHOT_BUNDLE_MAX_LANES=4, CDC_COMMIT_INTERVAL_MS=1000,
                     CDC_BATCH_MS=200, CDC_QUERY_TIMEOUT=15, CDC_LOAD_TIMEOUT=60,
                     CDC_STATUS_SECONDS=5, CDC_IDLE_STATUS_SECONDS=10,
                     CDC_COMPRESSION='', CDC_DETAIL_LOGS=True,
                     CDC_SHARED_SOURCE_STATE='true' if shared_source_state else 'false',
                     CDC_RESOURCE_MEMORY_MB=2048, CDC_DUCKDB_MEMORY='64MB')
    catalog = directory / 'catalog.sqlite3'
    commands = [f'SET VARIABLE {key} = {sql_literal(value)}' for key, value in variables.items()]
    commands.append('CREATE TABLE starrocks.events AS ' + QUERY)
    j4.cdc_catalog.execute_batch(str(catalog), commands)
    env = dict(os.environ, CDC_CATALOG_FILE=str(catalog), CDC_CATALOG_SOCKET=str(directory / 'control.sock'))
    return env


def state(directory):
    path = directory / 'state.sqlite3'
    if not path.exists():
        return None
    try:
        con = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True, timeout=2)
        try:
            rows = con.execute('SELECT snapshot_done FROM table_state').fetchall()
            pending = con.execute('SELECT COUNT(*) FROM active_jobs').fetchone()[0]
            deliveries = con.execute('SELECT COUNT(*) FROM deliveries').fetchone()[0]
            cursor = con.execute("SELECT value FROM meta WHERE key='read_position'").fetchone()
            source_ready = source_log = source_base = None
            source_pins = []
            try:
                relation_rows = con.execute(
                    'SELECT table_name,complete_seq FROM source_relations ORDER BY table_name'
                ).fetchall()
                source_ready = bool(relation_rows) and all(row[1] is not None for row in relation_rows)
                source_meta = dict(con.execute(
                    "SELECT key,value FROM source_state_meta "
                    "WHERE key IN ('log_durable_seq','base_applied_seq')"
                ).fetchall())
                source_log = int(source_meta.get('log_durable_seq', 0))
                source_base = int(source_meta.get('base_applied_seq', 0))
                source_pins = [
                    dict(owner=row[0], watermark=int(row[1]))
                    for row in con.execute(
                        'SELECT owner,watermark FROM source_pins ORDER BY owner'
                    ).fetchall()
                ]
            except sqlite3.OperationalError:
                pass
            return dict(done=bool(rows) and all(row[0] for row in rows),
                        pending=pending, deliveries=deliveries,
                        cursor=j4.unpack(cursor[0]) if cursor else None,
                        source_ready=source_ready, source_log=source_log,
                        source_base=source_base, source_pins=source_pins)
        finally:
            con.close()
    except sqlite3.Error:
        return None


def live_process(proc):
    if proc.poll() is not None:
        text = RUNTIME_LOG.read_text(errors='replace') if RUNTIME_LOG and RUNTIME_LOG.exists() else 'no log'
        detail = text[:3500] + '\n...\n' + text[-6000:]
        for name in ('M2S_TEST_MYSQL_PASSWORD', 'M2S_TEST_SR_PASSWORD'):
            secret = os.environ.get(name, '')
            if secret:
                detail = detail.replace(secret, '[redacted]')
        raise RuntimeError(f'actual daemon exited unexpectedly rc={proc.returncode}; synthetic diagnostics: {detail}')


def start(directory, env, run):
    global RUNTIME_LOG
    RUNTIME_LOG = directory / f'daemon-{run}.log'
    handle = RUNTIME_LOG.open('wb')
    proc = subprocess.Popen([sys.executable, str(ROOT / 'j4.py')], env=env,
                            stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    return proc, handle


def stop(proc, handle, kill=False):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGKILL if kill else signal.SIGTERM)
    try:
        rc = proc.wait(timeout=45)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        raise RuntimeError('daemon did not stop within 45 seconds')
    finally:
        handle.close()
    if not kill and rc != 0:
        raise RuntimeError(f'graceful daemon shutdown failed rc={rc}')


def wait_started(proc, directory):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        live_process(proc)
        current = state(directory)
        if current and current['cursor']:
            return current
        time.sleep(.1)
    raise RuntimeError('actual daemon did not initialize durable state')


def create_target(cfg):
    ddl = ('CREATE TABLE IF NOT EXISTS ' + DATABASE + '.events('
           'id BIGINT NOT NULL,part INT NOT NULL,v BIGINT,note VARCHAR(256),amount DECIMAL(19,4)) '
           'PRIMARY KEY(id,part) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES("replication_num"="1")')
    deadline = time.monotonic() + 90
    while True:
        try:
            execute(cfg, ddl)
            return
        except j4.pymysql.err.ProgrammingError as exc:
            # Alive may precede the first disk-capacity heartbeat. Only retry
            # this exact isolated boot condition; persistent full disk fails.
            if ('backends without enough disk space' not in str(exc)
                    or time.monotonic() >= deadline):
                raise
            time.sleep(1)


def normalize(rows):
    return [tuple(str(value) if isinstance(value, Decimal) else value for value in row) for row in rows]


def final_result(source, cfg, extra=False):
    with source.cursor() as cur:
        query = ('SELECT id,part,v,note,amount FROM ' + DATABASE + '.events WHERE active=1 AND v>=0 ORDER BY id,part'
                 if extra else 'SELECT id,part,v*2,trim(note),amount FROM ' + DATABASE + '.events WHERE active=1 ORDER BY id,part')
        cur.execute(query)
        expected = normalize(cur.fetchall())
    table = 'events_extra' if extra else 'events'
    actual, _ = execute(cfg, 'SELECT id,part,v,note,amount FROM ' + DATABASE + '.' + table + ' ORDER BY id,part')
    return expected, normalize(actual)


def wait_equal(proc, directory, source, cfg, extra=False):
    deadline = time.monotonic() + 240
    last = None
    while time.monotonic() < deadline:
        live_process(proc)
        current = state(directory)
        if current and current['done'] and not current['pending'] and not current['deliveries']:
            expected, actual = final_result(source, cfg)
            new_equal = True
            if extra:
                new_expected, new_actual = final_result(source, cfg, extra=True)
                new_equal = new_expected == new_actual
            if actual == expected and new_equal:
                return len(actual), current
            last = (len(expected), len(actual))
        time.sleep(.3)
    raise AssertionError(f'pipeline did not reach exact per-key result; counts={last} state={state(directory)}')


def disconnect_reader(source):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        with source.cursor() as cur:
            cur.execute('SHOW PROCESSLIST')
            names = [item[0].lower() for item in cur.description]
            connections = cur.fetchall()
            for row in connections:
                if str(row[names.index('command')]).lower().startswith('binlog dump'):
                    cur.execute('KILL CONNECTION ' + str(int(row[names.index('id')])))
                    return 1
        time.sleep(.1)
    raise AssertionError('no actual replication connection available for disconnect injection')


def change(source, sequence):
    """Key changes, delete/reinsert, filter flips, NULL/decimal and ordered markers."""
    key = sequence % 128
    source.begin()
    try:
        with source.cursor() as cur:
            cur.execute('UPDATE ' + DATABASE + '.events SET v=%s,active=%s,note=%s,amount=%s WHERE id=%s AND part=0',
                        (-sequence - 1 if sequence % 4 == 0 else sequence, int(sequence % 3 != 0), None if sequence % 5 == 0 else '  changed🙂  ',
                         None if sequence % 7 == 0 else Decimal('-0.0001'), key))
            cur.execute('DELETE FROM ' + DATABASE + '.events WHERE id=%s AND part=0', (256 + key,))
            cur.execute('INSERT INTO ' + DATABASE + '.events VALUES(%s,0,%s,%s,%s,1) ON DUPLICATE KEY UPDATE v=VALUES(v)',
                        (256 + key, sequence, ' reinsert ', Decimal('1.2345')))
            cur.execute('UPDATE ' + DATABASE + '.events SET id=id+100000 WHERE id=%s AND part=0', (512 + key,))
            cur.execute('INSERT INTO ' + DATABASE + '.events VALUES(%s,1,%s,%s,%s,1)',
                        (1000000 + sequence, sequence, f' marker_{sequence} ', Decimal('0.1234')))
        commit_started = time.perf_counter()
        source.commit()
    except BaseException:
        source.rollback()
        raise
    return commit_started


def sample_visible(proc, cfg, commits, samples, directory, extra=False):
    live_process(proc)
    table = 'events_extra' if extra else 'events'
    rows, _ = execute(cfg, 'SELECT id,v FROM ' + DATABASE + '.' + table + ' WHERE id>=1000000')
    observed = time.perf_counter()
    for key, value in rows:
        sequence = int(key) - 1000000
        if sequence in commits and sequence not in samples:
            if value != sequence * (1 if extra else 2):
                raise AssertionError('marker transformation differs')
            samples[sequence] = dict(seconds=observed - commits[sequence],
                                     during_backfill=not bool((state(directory) or {}).get('done')))


def wait_log_contains(proc, needle, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        live_process(proc)
        text = RUNTIME_LOG.read_text(errors='replace') if RUNTIME_LOG and RUNTIME_LOG.exists() else ''
        if needle in text:
            return text
        time.sleep(.05)
    raise AssertionError('daemon log did not contain expected marker: ' + needle)


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))] if ordered else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--isolated', action='store_true', required=True)
    parser.add_argument('--load-mode', choices=['merge_async', 'transaction'], default='merge_async')
    parser.add_argument('--group-size', type=int, choices=[1, 128], default=128)
    parser.add_argument('--shared-source-state', action='store_true')
    parser.add_argument('--rows', type=int, default=2048)
    parser.add_argument('--transactions', type=int, default=80)
    parser.add_argument('--output', type=Path, default=Path('benchmark-results/e2e-contract.json'))
    args = parser.parse_args()
    if not 1024 <= args.rows <= 100000 or not 40 <= args.transactions <= 1000:
        parser.error('rows=1024..100000 and transactions=40..1000')
    cfg, opts = configuration(), source_options()
    version = wait_ready(cfg)
    cfg['sr']['database'] = DATABASE
    execute(cfg, 'DROP DATABASE IF EXISTS ' + DATABASE)
    execute(cfg, 'CREATE DATABASE ' + DATABASE)
    create_target(cfg)
    source = j4.pymysql.connect(**opts)
    proc, handle = None, None
    try:
        with source.cursor() as cur:
            cur.execute('SELECT VERSION(),@@GLOBAL.gtid_mode')
            mysql_version, gtid_mode = cur.fetchone()
            cur.execute('DROP DATABASE IF EXISTS ' + DATABASE)
            cur.execute('CREATE DATABASE ' + DATABASE + ' CHARACTER SET utf8mb4')
            cur.execute('CREATE TABLE ' + DATABASE + '.events(id BIGINT NOT NULL,part INT NOT NULL,v BIGINT,note VARCHAR(64),amount DECIMAL(19,4),active TINYINT NOT NULL,PRIMARY KEY(id,part)) ENGINE=InnoDB')
            cur.executemany('INSERT INTO ' + DATABASE + '.events VALUES(%s,0,%s,%s,%s,1)',
                            [(i, i, '  initial🙂  ', Decimal('123.4567')) for i in range(args.rows)])
        with tempfile.TemporaryDirectory(prefix='m2s-e2e-') as temp:
            directory = Path(temp)
            env = setup_catalog(directory, cfg, opts, args.load_mode, args.group_size,
                                shared_source_state=args.shared_source_state)
            proc, handle = start(directory, env, 1)
            began = wait_started(proc, directory)
            if began['done']:
                raise AssertionError('fixture completed before concurrent backfill test started')
            commits, samples, disconnects = {}, {}, 0
            for seq in range(args.transactions):
                if seq == args.transactions // 2:
                    disconnects += disconnect_reader(source)
                commits[seq] = change(source, seq)
                if seq % 5 == 0:
                    sample_visible(proc, cfg, commits, samples, directory)
                time.sleep(.08)
            deadline = time.monotonic() + 90
            while len(samples) < len(commits) and time.monotonic() < deadline:
                sample_visible(proc, cfg, commits, samples, directory)
                time.sleep(.2)
            if len(samples) != len(commits) or not any(row['during_backfill'] for row in samples.values()):
                raise AssertionError('did not observe every new marker, including during backfill')
            wait_equal(proc, directory, source, cfg)
            before_dynamic = state(directory)
            if args.shared_source_state:
                if not before_dynamic or not before_dynamic.get('source_ready'):
                    raise AssertionError(
                        'shared source mirror was not complete before dynamic deployment: '
                        + repr(before_dynamic))
                if before_dynamic.get('source_log') != before_dynamic.get('source_base'):
                    raise AssertionError(
                        'dynamic deployment attempted while source base lagged durable log: '
                        + repr(before_dynamic))
            deployment = directory / 'deploy-extra.sql'
            deployment.write_text('CREATE TABLE starrocks.events_extra AS '
                                  'SELECT id,part,v,note,amount FROM mysql.events WHERE active=1 AND v>=0;\n')
            installed = subprocess.run([sys.executable, str(ROOT / 'j4.py'), 'sql', str(deployment)],
                                       env=env, capture_output=True, timeout=120)
            if installed.returncode:
                raise RuntimeError('actual synthetic SQL deployment failed; status=' + str(installed.returncode)
                                   + ' diagnostic=' + installed.stdout.decode(errors='replace')[-3000:])
            response = json.loads(installed.stdout.decode())
            activation = ((response.get('result') or {}).get('publish') or {}).get('activation') or {}
            if activation.get('status') != 'hot_pending':
                raise AssertionError('online synthetic SQL was not accepted for hot activation: ' +
                                     json.dumps(activation, sort_keys=True))
            # Publication becomes active only at the next safe MySQL
            # transaction boundary. Produce one explicit cutover transaction
            # before asserting which historical source the new sink selected.
            dynamic_commits, existing_samples, dynamic_samples = {}, {}, {}
            first_dynamic = args.transactions
            dynamic_commits[first_dynamic] = change(source, first_dynamic)
            if args.shared_source_state:
                activation_log = wait_log_contains(
                    proc, 'HOT ADD WORKERS sink=events_extra ', timeout=30)
                marker = 'HOT ADD WORKERS sink=events_extra source=events target=events_extra'
                line = next(
                    (row for row in activation_log.splitlines() if marker in row), '')
                if not line or 'snapshot=shared_fixed_w' not in line:
                    raise AssertionError(
                        'shared-state hot-add did not use fixed-W local history; '
                        'matching_log=' + repr(line))
                if 'snapshot=mysql' in line:
                    raise AssertionError('shared-state hot-add silently fell back to MySQL')
            # The new task is live before history is complete; existing output
            # must continue. Output schemas/filters deliberately differ.
            for seq in range(args.transactions + 1, args.transactions + 20):
                dynamic_commits[seq] = change(source, seq)
                time.sleep(.08)
            deadline = time.monotonic() + 90
            backfill_restart = None
            while (len(existing_samples) < 20 or len(dynamic_samples) < 20 or backfill_restart is None) and time.monotonic() < deadline:
                sample_visible(proc, cfg, dynamic_commits, existing_samples, directory)
                sample_visible(proc, cfg, dynamic_commits, dynamic_samples, directory, extra=True)
                current = state(directory)
                shared_pin = bool(
                    current and any(
                        pin['owner'].startswith('sink:events_extra:')
                        for pin in current.get('source_pins', ())
                    )
                )
                crash_ready = (
                    current and not current['done']
                    and (shared_pin if args.shared_source_state else bool(dynamic_samples))
                )
                if backfill_restart is None and crash_ready:
                    backfill_restart = dict(
                        pending_jobs=current['pending'],
                        deliveries=current['deliveries'],
                        source_pin=shared_pin,
                        source_pins=current.get('source_pins', []))
                    stop(proc, handle, kill=True)
                    proc, handle = None, None
                    proc, handle = start(directory, env, 2)
                    wait_started(proc, directory)
                time.sleep(.2)
            if backfill_restart is None:
                raise AssertionError('did not inject a crash during hot-added task historical construction')
            if not any(row['during_backfill'] for row in dynamic_samples.values()):
                raise AssertionError('new task emitted no fresh markers while history was incomplete')
            if len(existing_samples) != 20 or len(dynamic_samples) != 20:
                raise AssertionError('hot-added task or existing task stopped delivering CDC')
            before_count, before_state = wait_equal(proc, directory, source, cfg, extra=True)
            if args.shared_source_state:
                if before_state.get('source_pins'):
                    raise AssertionError(
                        'fixed-W source pin leaked after dynamic build: '
                        + repr(before_state['source_pins']))
                if before_state.get('source_log') != before_state.get('source_base'):
                    raise AssertionError(
                        'source base did not catch durable log after dynamic build')
            # Crash after a fully drained checkpoint. Separate from the uncertain-request test.
            stop(proc, handle, kill=True)
            proc, handle = None, None
            for seq in range(args.transactions + 20, args.transactions + 40):
                change(source, seq)
            proc, handle = start(directory, env, 3)
            wait_started(proc, directory)
            after_count, after_state = wait_equal(proc, directory, source, cfg, extra=True)
            stop(proc, handle)
            proc, handle = None, None
            latencies = [item['seconds'] for item in samples.values()]
            report = dict(format_version=1, kind='actual_daemon_mysql_starrocks_contract',
                          source_version=mysql_version, starrocks_version=version,
                          gtid_mode=gtid_mode, protocol=args.load_mode, event_group_events=args.group_size,
                          shared_source_state=bool(args.shared_source_state),
                          dynamic_history_source='shared_fixed_w' if args.shared_source_state else 'mysql_snapshot',
                          initial_rows=args.rows, transactions=args.transactions,
                          marker_samples=len(samples), actual_replication_disconnects=disconnects, visible_during_backfill=sum(item['during_backfill'] for item in samples.values()),
                          observation='client_commit_start_to_first_successful_target_poll_upper_bound_includes_commit_roundtrip',
                          observation_poll_schedule="every_5_transactions_during_writes_then_0.2s_sleep_plus_query_time",
                          latency_seconds=dict(p50=percentile(latencies,.5),p95=percentile(latencies,.95),p99=percentile(latencies,.99),max=max(latencies)),
                          before_restart_rows=before_count, after_restart_rows=after_count,
                          dynamic_sql_deployment=True, dynamic_sql_markers=20,
                          dynamic_markers_visible_during_backfill=sum(x['during_backfill'] for x in dynamic_samples.values()),
                          existing_task_markers_during_deploy=len(existing_samples),
                          dynamic_task_exact_final_result=True,
                          dynamic_task_latency_seconds=dict(p95=percentile([x['seconds'] for x in dynamic_samples.values()],.95),
                                                           p99=percentile([x['seconds'] for x in dynamic_samples.values()],.99)),
                          crash_boundary='active_new_task_backfill_then_drained_checkpoint; HTTP_boundary_not_controlled',
                          dynamic_backfill_crash=backfill_restart,
                          exact_final_result=True, durable_cursor_progressed=j4.position_ge(after_state['cursor'],before_state['cursor']),
                          server_configuration='image_defaults_except_test_table_replication_1',
                          scope='short_functional_probe_not_50M_or_72h', samples=samples,
                          dynamic_samples=dynamic_samples, existing_samples=existing_samples)
            if after_state['cursor'] == before_state['cursor']:
                raise AssertionError('restart did not capture new source transactions')
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + '\n')
            print(json.dumps({key:value for key,value in report.items() if key not in ('samples','dynamic_samples','existing_samples')}), flush=True)
    finally:
        if proc is not None:
            stop(proc, handle, kill=True)
        source.close()
        execute(cfg, 'DROP DATABASE IF EXISTS ' + DATABASE)


if __name__ == '__main__':
    main()
