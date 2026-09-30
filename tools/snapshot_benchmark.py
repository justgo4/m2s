#!/usr/bin/env python3
"""Actual production snapshot reader A/B on existing isolated synthetic.events.

Run binlog_parity.py --live first. This tool never creates or drops a table.
The fixture must not be mutated concurrently. No source data/config artifacts.
"""
import argparse
import json
import os
from pathlib import Path
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
from binlog_contract import mapping
from binlog_parity import config
from e2e_contract import source_options


def process_io():
    try:
        return {key: int(value) for key, value in
                (line.split(':', 1) for line in Path('/proc/self/io').read_text().splitlines())
                if key in ('read_bytes', 'write_bytes', 'rchar', 'wchar')}
    except (OSError, ValueError):
        return None


def io_delta(before, after):
    return {key: after[key] - before[key] for key in before} if before and after else None


def sample(args):
    prepared = mapping()
    cfg = config(args.binary, [prepared])
    cfg.update(batch_bytes=16*1024**2, snapshot_chunk_bytes=64*1024**2)
    options = dict(source_options(), database='synthetic')
    cfg['mysql'] = options
    source = j4.pymysql.connect(**options)
    decoder = None
    child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        with source.cursor() as cur:
            cur.execute('SELECT VERSION()')
            version = cur.fetchone()[0]
            cur.execute('SELECT * FROM synthetic.events ORDER BY id,part')
            rows = cur.fetchall()
        if not rows or len(rows) > 100000:
            raise ValueError('existing synthetic fixture must contain 1..100000 rows')
        expected = j4.snapshot_arrow(prepared, rows)
        upper = j4.snapshot_upper(source, prepared)
        if args.worker == 'native':
            decoder = j4.native_start(cfg, [prepared])
        tables, cursor, read_rows, chunks = [], None, 0, 0
        io_before, cpu_before, wall_before = process_io(), time.process_time(), time.perf_counter()
        while True:
            batch, detail = j4.fetch_snapshot(source, prepared, cursor, upper,
                                              args.batch_rows, cfg, decoder)
            if decoder is None:
                batch = j4.snapshot_arrow(prepared, batch)
            read_rows += detail['source_rows']
            chunks += 1
            if not batch.num_rows:
                break
            tables.append(batch)
            cursor = tuple(batch[name][-1].as_py() for name in prepared['primary_key'])
            if cursor == upper:
                break
        wall, cpu = time.perf_counter() - wall_before, time.process_time() - cpu_before
        io = io_delta(io_before, process_io())
        j4.native_stop(decoder)
        decoder = None
        child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        child_cpu = ((child_after.ru_utime + child_after.ru_stime)
                     - (child_before.ru_utime + child_before.ru_stime))
        actual = j4.pa.concat_tables(tables)
        # Each production snapshot chunk has its own order range. Compare every
        # business value/schema, and independently validate chunk-local order/op.
        names = [name for name, _ in prepared['_schema']]
        if not actual.select(names).equals(expected.select(names)):
            raise AssertionError('production reader Arrow schema/value differs from MySQL typed oracle')
        for table in tables:
            if table['_sync_op'].to_pylist() != [0]*table.num_rows or table['_sync_order'].to_pylist() != list(range(table.num_rows)):
                raise AssertionError('snapshot operation/order differs')
        if read_rows != len(rows):
            raise AssertionError('reader reread/skipped source rows despite ample chunk budget')
        report = dict(method=args.worker, wall_seconds=wall, python_cpu_seconds=cpu,
                      c_cpu_seconds=child_cpu, total_cpu_seconds=cpu+child_cpu,
                      rows=actual.num_rows, rows_per_second=actual.num_rows/wall,
                      logical_arrow_bytes=actual.nbytes, chunks=chunks,
                      peak_python_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                      child_peak_rss_bytes=child_after.ru_maxrss*1024 if args.worker == 'native' else 0,
                      process_io_delta=io, source_rows_read=read_rows,
                      source_version=version, exact_typed_result=True)
        args.output.parent.mkdir(exist_ok=True, parents=True)
        args.output.write_text(json.dumps(report, indent=2)+'\n')
        return report
    finally:
        j4.native_stop(decoder)
        source.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--isolated', action='store_true', required=True)
    parser.add_argument('--binary', type=Path, default=ROOT/'build/native/mysql_arrow_reader')
    parser.add_argument('--batch-rows', type=int, default=512)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--worker', choices=['python', 'native'])
    parser.add_argument('--output', type=Path, default=Path('benchmark-results/snapshot.json'))
    args = parser.parse_args()
    if not 1 <= args.repeats <= 10 or not 1 <= args.batch_rows <= 10000:
        parser.error('repeats=1..10 and batch-rows=1..10000')
    if args.worker:
        return sample(args)
    samples = dict(python=[], native=[])
    rng = random.Random(819)
    with tempfile.TemporaryDirectory(prefix='m2s-snapshot-ab-') as directory:
        for repeat in range(args.repeats):
            order = list(samples)
            rng.shuffle(order)
            for method in order:
                output = Path(directory)/f'{repeat}-{method}.json'
                command = [sys.executable, __file__, '--isolated', '--worker', method,
                           '--binary', str(args.binary.resolve()), '--batch-rows', str(args.batch_rows),
                           '--output', str(output)]
                run = subprocess.run(command, capture_output=True, timeout=180)
                if run.returncode:
                    # Keep credential-bearing connection exceptions out of public logs.
                    raise RuntimeError(f'snapshot worker {method} failed rc={run.returncode}; check isolated fixture/binary')
                samples[method].append(json.loads(output.read_text()))
    counts = {value['rows'] for group in samples.values() for value in group}
    if len(counts) != 1:
        raise AssertionError('fixture changed during source A/B')
    report = dict(format_version=1, kind='actual_production_snapshot_reader_AB',
                  samples=samples, repeats=args.repeats, batch_rows=args.batch_rows,
                  schema='21_column_synthetic_MYSQL_types_ROW_FULL_contract',
                  scope='actual_MySQL_read_to_Arrow_no_routing_journal_sink; warm_server_cache',
                  cpu_scope='Python_scan_CPU_plus_full_native_child_lifecycle_CPU',
                  rss_scope='whole_fresh_worker_including_typed_oracle; child_max_RSS_not_concurrent_sum',
                  io_scope='/proc/self/io_scan_interval_not_socket_network_or_MySQL_server_IO',
                  unmeasured=['MySQL_server_CPU', 'MySQL_server_IO', 'IPC_bytes', 'network_bytes'],
                  wall_scope='production_fetch_and_Python_Arrow_conversion; connection_oracle_child_start_excluded',
                  versions=dict(python=sys.version.split()[0],arrow=j4.pa.__version__))
    args.output.parent.mkdir(exist_ok=True, parents=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(dict(kind=report['kind'], rows=next(iter(counts)), exact_typed_result=True,
                         median_wall_seconds={method:statistics.median(row['wall_seconds'] for row in values)
                                              for method,values in samples.items()})), flush=True)


if __name__ == '__main__':
    main()
