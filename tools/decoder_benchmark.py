#!/usr/bin/env python3
"""Isolated Python/native event/native group A/B: decoder or durable local path.

This excludes MySQL socket reads and StarRocks, and makes no end-to-end claims.
Every measured sample runs in a fresh Python process. Raw fixture construction,
configuration, state creation and engine initialization are outside wall timing.
Native CPU includes child startup/shutdown, explicitly reported as such.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import random
import resource
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4
from binlog_contract import SPECS, decode_rows, mapping, parse_map, random_row, row_event, table_map


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def peak_rss(pid):
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith('VmHWM:'):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return None


def statistics_of(values):
    known = [value for value in values if value is not None]
    if not known:
        return dict(median=None, min=None, max=None, known_samples=0)
    return dict(median=statistics.median(known), min=min(known), max=max(known), known_samples=len(known))


def worker(args):
    rng = random.Random(args.seed)
    specs = [SPECS[i] for i in (0, 1, 12, 17)]
    if args.width:
        specs.append(SPECS[13])
    prepared = mapping(specs)
    prepared.update(sql='SELECT * FROM arrow_batch', full_filter=None)
    j4.validate_mapping(prepared)
    info = parse_map(table_map(specs))
    inputs, groups = [], []
    for event in range(args.events):
        rows = [random_row(rng, event * args.rows_per_event + i) for i in range(args.rows_per_event)]
        if args.width:
            for row in rows:
                row['blob'] = b'x' * args.width
        raw = row_event('insert', rows, specs)
        inputs.append(raw)
    for offset in range(0, len(inputs), args.events_per_transaction):
        groups.append(inputs[offset:offset + args.events_per_transaction])
    cfg = dict(mysql=dict(database='synthetic'), query_timeout=30,
               native_binlog_path=str(args.binary.resolve()), key_partitions=16,
               batch_rows=50000, batch_bytes=16 * 1024 ** 2, max_row_bytes=64 * 1024 ** 2,
               duckdb_memory='256MB')
    decoder, engine, con = None, None, None
    children_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    if args.worker != 'python':
        decoder = j4.native_start(cfg, [prepared])
        j4.native_decode_event(decoder, table_map(specs), 30)
    ipc_in, ipc_out = 0, 0
    real_writev, real_write, real_read = j4.native_writev_frame, j4.native_write_frame, j4.native_read_frame

    def writev(proc, kind, parts, payload_size, timeout=30):
        nonlocal ipc_in
        ipc_in += 5 + payload_size
        return real_writev(proc, kind, parts, payload_size, timeout)

    def write(proc, kind, payload=b'', timeout=30):
        nonlocal ipc_in
        ipc_in += 5 + len(payload)
        return real_write(proc, kind, payload, timeout)

    def read(proc, timeout):
        nonlocal ipc_out
        kind, payload = real_read(proc, timeout)
        ipc_out += 5 + len(payload)
        return kind, payload

    j4.native_writev_frame, j4.native_write_frame, j4.native_read_frame = writev, write, read
    latencies, rows, logical_bytes, sqlite_bytes, checksum = [], 0, 0, 0, 0
    with tempfile.TemporaryDirectory(prefix='m2s-decoder-bench-') as directory:
        if args.layer == 'local':
            path = str(Path(directory) / 'state.sqlite3')
            cfg['state'] = path
            con = j4.init_state(path)
            j4.bootstrap(con, 'synthetic', 'synthetic', ('binlog.000001', 4), ['events'])
            con.execute("UPDATE table_state SET snapshot_done=1 WHERE name='events'")
            engine = j4.transform_engine(cfg)
        wall, cpu = time.perf_counter(), time.process_time()
        try:
            for transaction, raw_events in enumerate(groups):
                started = time.perf_counter()
                if args.worker == 'python':
                    batches = [decode_rows(event, info, prepared) for event in raw_events]
                elif args.worker == 'native_event':
                    batches = [j4.native_decode_event(decoder, event, 30)[2] for event in raw_events]
                else:
                    batches = [item[2] for item in j4.native_decode_events(decoder, raw_events, 30)]
                for batch in batches:
                    rows += batch.num_rows
                    logical_bytes += batch.nbytes
                    checksum += j4.pc.sum(batch['id']).as_py() or 0
                if con is not None:
                    with tempfile.SpooledTemporaryFile() as spool:
                        pending = j4.transaction_batch_new()
                        for batch in batches:
                            j4.transaction_batch_add(pending, prepared, batch, cfg, engine, spool)
                        j4.transaction_batch_flush_all(pending, cfg, engine, spool)
                        j4.commit_spool(con, spool, ('binlog.000001', 100 + transaction), 1700000000, {'events': prepared})
                latencies.append(time.perf_counter() - started)
            seconds = time.perf_counter() - wall
            python_cpu = time.process_time() - cpu
            native_rss = peak_rss(decoder.pid) if decoder else 0
            if con is not None:
                sqlite_bytes = sum(Path(directory, name).stat().st_size for name in os.listdir(directory))
                expected_rows = args.events * args.rows_per_event
                if con.execute('SELECT SUM(nrows) FROM jobs').fetchone()[0] != expected_rows:
                    raise AssertionError('durable journal lost rows')
                if j4.meta_get(con, 'read_position') != ('binlog.000001', 99 + len(groups)):
                    raise AssertionError('durable local checkpoint differs')
        finally:
            if decoder:
                j4.native_stop(decoder)
            if engine:
                engine.close()
            if con:
                con.close()
            j4.native_writev_frame, j4.native_write_frame, j4.native_read_frame = real_writev, real_write, real_read
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    native_cpu = children.ru_utime + children.ru_stime - children_before.ru_utime - children_before.ru_stime
    expected_rows = args.events * args.rows_per_event
    if rows != expected_rows or checksum != expected_rows * (expected_rows - 1) // 2:
        raise AssertionError('decoded row count/order checksum differs')
    return dict(method=args.worker, layer=args.layer, wall_seconds=seconds,
                python_cpu_seconds=python_cpu, native_cpu_seconds=native_cpu,
                total_cpu_seconds=python_cpu + native_cpu, rows=rows,
                rows_per_second=rows / seconds, input_event_bytes=sum(map(len, inputs)),
                decoded_arrow_bytes=logical_bytes, ipc_input_bytes=ipc_in, ipc_output_bytes=ipc_out,
                sqlite_bytes=sqlite_bytes, durable_transactions=len(groups) if args.layer == 'local' else 0,
                durability='sqlite_FULL_WAL' if args.layer == 'local' else 'volatile_decoder',
                python_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                native_peak_rss_bytes=native_rss,
                transaction_latency_seconds={name: percentile(latencies, p) for name, p in [('p50', .5), ('p95', .95), ('p99', .99)]},
                checksum=checksum)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, default=ROOT / 'build/native/mysql_arrow_reader')
    parser.add_argument('--layer', choices=['decoder', 'local'], default='decoder')
    parser.add_argument('--events', type=int, default=1000)
    parser.add_argument('--rows-per-event', type=int, default=8)
    parser.add_argument('--events-per-transaction', type=int, default=64)
    parser.add_argument('--width', type=int, default=0)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--worker', choices=['python', 'native_event', 'native_group'])
    parser.add_argument('--output', type=Path, default=Path('benchmark-results/decoder.json'))
    args = parser.parse_args()
    if not 1 <= args.events <= 10000 or not 1 <= args.rows_per_event <= 1024 or not 1 <= args.events_per_transaction <= 4096 or not 0 <= args.width <= 65536:
        parser.error('bounded events/rows/group/width required')
    if not 1 <= args.repeats <= 25 or args.events * args.rows_per_event * (args.width + 512) > 256 * 1024 ** 2:
        parser.error('repeats=1..25 and estimated fixture memory <=256 MiB required')
    if args.worker:
        report = worker(args)
    else:
        samples = {key: [] for key in ['python', 'native_event', 'native_group']}
        rng = random.Random(args.seed)
        with tempfile.TemporaryDirectory(prefix='m2s-bench-results-') as directory:
            for repeat in range(args.repeats):
                order = list(samples)
                rng.shuffle(order)
                for method in order:
                    output = Path(directory) / f'{method}_{repeat}.json'
                    command = [sys.executable, __file__, '--worker', method, '--layer', args.layer,
                               '--binary', str(args.binary.resolve()), '--events', str(args.events),
                               '--rows-per-event', str(args.rows_per_event), '--events-per-transaction', str(args.events_per_transaction),
                               '--width', str(args.width), '--seed', str(args.seed), '--output', str(output)]
                    result = subprocess.run(command, capture_output=True, timeout=300)
                    if result.returncode:
                        raise RuntimeError('benchmark worker failed: ' + result.stderr.decode(errors='replace')[-2000:])
                    samples[method].append(json.loads(output.read_text()))
        metrics = ['wall_seconds', 'python_cpu_seconds', 'native_cpu_seconds', 'total_cpu_seconds',
                   'rows_per_second', 'ipc_input_bytes', 'ipc_output_bytes', 'sqlite_bytes',
                   'python_peak_rss_bytes', 'native_peak_rss_bytes']
        summary = {method: {key: statistics_of([item[key] for item in items])
                            for key in metrics} for method, items in samples.items()}
        report = dict(format_version=1, kind='same_fixture_decoder_or_local_AB', layer=args.layer,
                      events=args.events, rows_per_event=args.rows_per_event,
                      events_per_transaction=args.events_per_transaction, width=args.width,
                      seed=args.seed, repeats=args.repeats, samples=samples, summary=summary,
                      native_cpu_scope='includes_child_startup_shutdown',
                      correctness='same_fixture_counts_checksum_and_prior_full_differential_gate',
                      environment=dict(python=platform.python_version(), arrow=j4.pa.__version__,
                                       duckdb=j4.duckdb.__version__, architecture=platform.machine(), cpu_count=os.cpu_count()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key not in ('samples', 'environment')}, sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
