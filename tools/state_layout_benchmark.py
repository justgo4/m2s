#!/usr/bin/env python3
"""Candidate source-state layouts with atomic state/changelog/checkpoint.

Synthetic and bounded; neither prototype is wired into production. Compare
SQLite FULL/WAL Arrow batches+key references with DuckDB typed latest rows+WAL.
Includes layout/codec/index costs, not a pure storage-engine leaderboard.
"""
import argparse
import base64
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import shutil
import sqlite3
import statistics
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4

COLUMNS = ['id', 'part', 'v', 'note', 'amount']
SCHEMA = j4.pa.schema([('id',j4.pa.int64()),('part',j4.pa.uint32()),('v',j4.pa.int64()),
                      ('note',j4.pa.large_string()),('amount',j4.pa.decimal128(19,4)),
                      ('_sync_op',j4.pa.int8()),('_sync_order',j4.pa.int64())])


def fixtures(rows, transactions, changes, width, entropy):
    rng = random.Random(991)
    def text(fill):
        if not width:
            return fill + '🙂'
        if entropy == 'low':
            return fill[0] * width
        return base64.b64encode(rng.randbytes((width*3+3)//4)).decode()[:width]
    base = [dict(id=i,part=i%7,v=i,note=text('initial'),
                 amount=Decimal('123.4567'),_sync_op=0,_sync_order=i) for i in range(rows)]
    batches = [j4.pa.Table.from_pylist(base,schema=SCHEMA)]
    for sequence in range(1,transactions+1):
        delta = []
        for change in range(changes):
            key = (sequence*changes+change)%rows
            op = int((sequence+change)%5==0)
            delta.append(dict(id=key,part=key%7,v=-sequence,
                              note=None if change%3==0 else text('changed'),
                              amount=None if change%7==0 else Decimal('-0.0001'),
                              _sync_op=op,_sync_order=change))
        # Same-key repeated images in one transaction: final image must win.
        if delta:
            copy = dict(delta[0],v=sequence,_sync_op=0,_sync_order=len(delta))
            delta.append(copy)
        if sequence % 2 == 0:
            delta.reverse()  # Physical Arrow row order need not equal source order.
        batches.append(j4.pa.Table.from_pylist(delta,schema=SCHEMA))
    return batches


def payload(batch):
    return bytes(j4.arrow_table_payload(batch))


def decode(data):
    return j4.pa.ipc.open_stream(memoryview(data)[len(j4.ARROW_JOB_MAGIC):]).read_all()


def open_engine(method, directory):
    if method == 'rocks_arrow_index':
        from rocksdict import BlockBasedOptions, Cache, Options, Rdict, WriteOptions
        options = Options(raw_mode=True)
        options.create_if_missing(True)
        options.set_write_buffer_size(8*1024**2)
        options.set_max_write_buffer_number(2)
        options.set_max_background_jobs(2)
        block = BlockBasedOptions()
        block.set_block_cache(Cache(16*1024**2))
        options.set_block_based_table_factory(block)
        con = Rdict(str(directory/'rocks'), options)
        writes = WriteOptions()
        writes.sync = True
        writes.disable_wal = False
        con.set_write_options(writes)
        if con.get(b'C') is None:
            con[b'C'] = b'-1'
        return con
    if method=='sqlite_arrow_index':
        con = sqlite3.connect(str(directory/'state.sqlite3'),isolation_level=None)
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('PRAGMA synchronous=FULL')
        con.execute('PRAGMA foreign_keys=ON')
        con.execute('PRAGMA wal_autocheckpoint=0')
        con.executescript('''
            CREATE TABLE IF NOT EXISTS commits(seq INTEGER PRIMARY KEY,payload BLOB NOT NULL,checksum TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS checkpoint(id INTEGER PRIMARY KEY CHECK(id=1),seq INTEGER NOT NULL);
            INSERT OR IGNORE INTO checkpoint VALUES(1,-1);
            CREATE TABLE IF NOT EXISTS source_index(
                id INTEGER NOT NULL,part INTEGER NOT NULL,batch_seq INTEGER NOT NULL REFERENCES commits(seq),
                row_index INTEGER NOT NULL,deleted INTEGER NOT NULL,PRIMARY KEY(id,part)) WITHOUT ROWID;
        ''')
    else:
        con = j4.duckdb.connect(str(directory/'state.duckdb'),config={'threads':1,'memory_limit':'256MB'})
        con.execute('''CREATE TABLE IF NOT EXISTS commits(seq BIGINT PRIMARY KEY,payload BLOB NOT NULL,checksum VARCHAR NOT NULL)''')
        con.execute('CREATE TABLE IF NOT EXISTS checkpoint(id INTEGER PRIMARY KEY,seq BIGINT NOT NULL)')
        con.execute('INSERT OR IGNORE INTO checkpoint VALUES(1,-1)')
        con.execute('''CREATE TABLE IF NOT EXISTS source_state(
            id BIGINT,part UINTEGER,v BIGINT,note VARCHAR,amount DECIMAL(19,4),PRIMARY KEY(id,part))''')
    return con


def checkpoint_seq(con, method):
    if method == 'rocks_arrow_index':
        return int(con[b'C'])
    return con.execute('SELECT seq FROM checkpoint WHERE id=1').fetchone()[0]


def commit_count(con, method):
    if method == 'rocks_arrow_index':
        count = 0
        for key in con.keys(from_key=b'I'):
            if not key.startswith(b'I'):
                break
            count += 1
        return count
    return con.execute('SELECT COUNT(*) FROM commits').fetchone()[0]


def apply_rocks(con, batch, sequence, data, digest, crash):
    from rocksdict import WriteBatch
    identity = b'I'+struct.pack('>Q', sequence)
    existing = con.get(identity)
    if existing is not None:
        if existing != digest.encode():
            raise ValueError('conflicting replay identity')
        return False
    if sequence != int(con[b'C'])+1:
        raise ValueError('source transaction gap')
    write = WriteBatch(raw_mode=True)
    write.put(b'L'+struct.pack('>Q', sequence), data)
    write.put(identity, digest.encode())
    ids, parts, ops, orders = [batch[name].to_pylist() for name in ('id','part','_sync_op','_sync_order')]
    for index in sorted(range(batch.num_rows), key=orders.__getitem__):
        # Sign-flipped big-endian IDs preserve signed numeric key ordering.
        key = b'K'+struct.pack('>QI', ids[index]+2**63, parts[index])
        write.put(key, struct.pack('>QIB', sequence, index, ops[index]))
    write.put(b'C', str(sequence).encode())
    if crash == 'before_commit':
        os._exit(86)
    con.write(write)  # sync WAL WriteBatch: latest references/log/watermark together.
    if crash == 'after_commit':
        os._exit(87)
    return True


def apply(con, method, batch, sequence, crash=None):
    data = payload(batch)
    digest = hashlib.sha256(data).hexdigest()
    if (batch.schema!=SCHEMA or batch['id'].null_count or batch['part'].null_count
            or batch['_sync_op'].null_count or batch['_sync_order'].null_count):
        raise ValueError('source schema or key contract mismatch')
    if j4.pc.any(j4.pc.invert(j4.pc.is_in(batch['_sync_op'],value_set=j4.pa.array([0,1],type=j4.pa.int8())))).as_py():
        raise ValueError('unsupported row operation')
    if j4.pc.count_distinct(batch['_sync_order']).as_py() != batch.num_rows:
        raise ValueError('ambiguous source order')
    if method == 'rocks_arrow_index':
        return apply_rocks(con, batch, sequence, data, digest, crash)
    con.execute('BEGIN TRANSACTION')
    registered = False
    try:
        existing = con.execute('SELECT checksum FROM commits WHERE seq=?',[sequence]).fetchone()
        if existing:
            if existing[0]!=digest:
                raise ValueError('conflicting replay identity')
            con.execute('ROLLBACK')
            return False
        last = con.execute('SELECT seq FROM checkpoint WHERE id=1').fetchone()[0]
        if sequence!=last+1:
            raise ValueError('source transaction gap')
        con.execute('INSERT INTO commits VALUES(?,?,?)',[sequence,data,digest])
        if method=='sqlite_arrow_index':
            ids,parts,ops,orders = [batch[name].to_pylist() for name in ('id','part','_sync_op','_sync_order')]
            # Only key/op metadata becomes Python scalars. Business columns stay
            # in immutable Arrow; references keep exact typed values.
            con.executemany('''INSERT INTO source_index VALUES(?,?,?,?,?)
                ON CONFLICT(id,part) DO UPDATE SET batch_seq=excluded.batch_seq,
                    row_index=excluded.row_index,deleted=excluded.deleted''',
                [(ids[index],parts[index],sequence,index,ops[index])
                 for index in sorted(range(batch.num_rows),key=orders.__getitem__)])
        else:
            con.register('_incoming',batch)
            registered = True
            con.execute('''CREATE OR REPLACE TEMP TABLE _last AS
                SELECT * FROM _incoming QUALIFY row_number() OVER(PARTITION BY id,part ORDER BY _sync_order DESC)=1''')
            con.execute('DELETE FROM source_state USING _last WHERE source_state.id=_last.id AND source_state.part=_last.part')
            con.execute('INSERT INTO source_state SELECT id,part,v,note,amount FROM _last WHERE _sync_op=0')
        con.execute('UPDATE checkpoint SET seq=? WHERE id=1',[sequence])
        if crash=='before_commit':
            os._exit(86)
        con.execute('COMMIT')
        if crash=='after_commit':
            os._exit(87)
        return True
    except BaseException:
        con.execute('ROLLBACK')
        raise
    finally:
        if registered:
            con.unregister('_incoming')


def scan(con, method):
    schema = j4.pa.schema([SCHEMA.field(name) for name in COLUMNS])
    if method=='duckdb_typed':
        return con.execute('SELECT id,part,v,note,amount FROM source_state ORDER BY id,part').to_arrow_table().cast(schema)
    references = {}
    if method == 'rocks_arrow_index':
        for key, value in con.items(from_key=b'K'):
            if not key.startswith(b'K'):
                break
            sequence, index, deleted = struct.unpack('>QIB', value)
            if not deleted:
                references.setdefault(sequence, []).append(index)
    else:
        for sequence,index in con.execute('SELECT batch_seq,row_index FROM source_index WHERE deleted=0 ORDER BY batch_seq,row_index'):
            references.setdefault(sequence,[]).append(index)
    tables = []
    for sequence,indexes in references.items():
        data = (con[b'L'+struct.pack('>Q', sequence)] if method == 'rocks_arrow_index' else
                con.execute('SELECT payload FROM commits WHERE seq=?',[sequence]).fetchone()[0])
        tables.append(decode(data).select(COLUMNS).take(j4.pa.array(indexes,type=j4.pa.int64())))
    return (j4.pa.concat_tables(tables) if tables else j4.pa.Table.from_batches([],schema)).sort_by([('id','ascending'),('part','ascending')])


def oracle(batches):
    latest = {}
    for batch in batches:
        for row in sorted(batch.to_pylist(),key=lambda value:value['_sync_order']):
            key = row['id'],row['part']
            if row['_sync_op']:
                latest.pop(key,None)
            else:
                latest[key] = {name:row[name] for name in COLUMNS}
    schema = j4.pa.schema([SCHEMA.field(name) for name in COLUMNS])
    return j4.pa.Table.from_pylist([latest[key] for key in sorted(latest)],schema=schema)


def fixed_checkpoint(con, method, directory):
    """Paused-writer prototype: DB carries W, not a second unbound manifest.

    Production online snapshot/retention is not implemented by this helper.
    The final directory is published after close+fsync; orphan builds are ignored.
    """
    target = directory / 'fixed-w'
    if target.exists():
        return target
    staging = directory / 'fixed-w-building'
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir()
    if method == 'rocks_arrow_index':
        from rocksdict import Checkpoint
        Checkpoint(con).create_checkpoint(str(staging/'rocks'))
    elif method == 'sqlite_arrow_index':
        copied = sqlite3.connect(str(staging / 'state.sqlite3'))
        try:
            con.backup(copied)
        finally:
            copied.close()
    else:
        con.execute('CHECKPOINT')
        shutil.copyfile(directory / 'state.duckdb', staging / 'state.duckdb')
    for path in staging.rglob('*'):
        if not path.is_file():
            continue
        with path.open('rb') as handle:
            os.fsync(handle.fileno())
    for path in sorted([staging]+[item for item in staging.rglob('*') if item.is_dir()],
                       key=lambda value:len(value.parts), reverse=True):
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    os.rename(staging, target)
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return target


def verify_fixed_checkpoint(args, batches):
    # Copy the immutable base into a separate task build. Never advance W itself.
    base = args.directory / 'fixed-w'
    build = args.directory / 'task-build'
    if build.exists():
        shutil.rmtree(build)
    shutil.copytree(base, build)
    con = open_engine(args.worker, build)
    try:
        watermark = checkpoint_seq(con, args.worker)
        if watermark != 3 or not scan(con, args.worker).equals(oracle(batches[:4])):
            raise AssertionError('fixed W changed or was not bound to copied source state')
        for sequence, batch in enumerate(batches[4:], 4):
            apply(con, args.worker, batch, sequence)
        if not scan(con, args.worker).equals(oracle(batches)):
            raise AssertionError('fixed W plus suffix replay differs from source final state')
    finally:
        con.close()
    # Reopen the untouched W after reconstruction and source advancement.
    con = open_engine(args.worker, base)
    try:
        if checkpoint_seq(con, args.worker) != 3:
            raise AssertionError('task replay mutated the shared fixed W checkpoint')
    finally:
        con.close()
    return sum(path.stat().st_size for path in base.rglob('*') if path.is_file())


def worker(args):
    batches = fixtures(args.rows,args.transactions,args.changes,args.width,args.entropy)
    directory = args.directory
    directory.mkdir(exist_ok=True,parents=True)
    con = open_engine(args.worker,directory)
    timing = []
    initial_wall = time.perf_counter()
    initial_cpu = time.process_time()
    if not args.recover:
        apply(con,args.worker,batches[0],0)
    initial = dict(wall_seconds=time.perf_counter()-initial_wall,cpu_seconds=time.process_time()-initial_cpu)
    wall,cpu = time.perf_counter(),time.process_time()
    applied = 0
    for sequence,batch in enumerate(batches[1:],1):
        started = time.perf_counter()
        crash = args.crash if sequence==min(5,args.transactions) and not args.recover else None
        applied += apply(con,args.worker,batch,sequence,crash)
        timing.append(time.perf_counter()-started)
        if sequence == 3:
            checkpoint_started = time.perf_counter()
            fixed_checkpoint(con, args.worker, directory)
            checkpoint_seconds = time.perf_counter() - checkpoint_started
    changes = dict(wall_seconds=time.perf_counter()-wall,cpu_seconds=time.process_time()-cpu,
                   changes_per_second=args.transactions*args.changes/(time.perf_counter()-wall))
    scan_started,scan_cpu = time.perf_counter(),time.process_time()
    actual = scan(con,args.worker)
    scan_time = dict(wall_seconds=time.perf_counter()-scan_started,cpu_seconds=time.process_time()-scan_cpu)
    expected = oracle(batches)
    if not actual.equals(expected):
        raise AssertionError('typed latest state differs from independent replay oracle')
    if commit_count(con, args.worker)!=args.transactions+1:
        raise AssertionError('duplicate or missing durable changelog entries')
    if checkpoint_seq(con, args.worker)!=args.transactions:
        raise AssertionError('checkpoint did not advance with exact state')
    # Exact replay succeeds; a different payload under the same identity fails
    # and must leave all state/checkpoint/changelog unchanged.
    if apply(con,args.worker,batches[-1],args.transactions):
        raise AssertionError('exact durable replay was duplicated')
    conflict = batches[-1].set_column(2,'v',j4.pa.array([999]*batches[-1].num_rows,type=j4.pa.int64()))
    try:
        apply(con,args.worker,conflict,args.transactions)
    except ValueError:
        pass
    else:
        raise AssertionError('conflicting replay identity was accepted')
    if not scan(con,args.worker).equals(expected):
        raise AssertionError('replay rejection changed committed state')
    for name in ('id', 'part', '_sync_op', '_sync_order'):
        position = batches[-1].schema.get_field_index(name)
        malformed = batches[-1].set_column(position, name, j4.pa.nulls(
            batches[-1].num_rows, type=SCHEMA.field(name).type))
        try:
            apply(con, args.worker, malformed, args.transactions+1)
        except ValueError:
            pass
        else:
            raise AssertionError('NULL source identity/order/operation accepted')
    try:
        apply(con, args.worker, batches[-1], args.transactions+2)
    except ValueError:
        pass
    else:
        raise AssertionError('source transaction gap accepted')
    if checkpoint_seq(con, args.worker) != args.transactions:
        raise AssertionError('invalid source input advanced durable cursor')
    checkpoint_bytes = verify_fixed_checkpoint(args, batches)
    close_started = time.perf_counter()
    con.close()
    report = dict(method=args.worker,initial=initial,changes=changes,scan=scan_time,
                  close_checkpoint_seconds=time.perf_counter()-close_started,
                  process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                  file_bytes=sum(p.stat().st_size for p in directory.rglob('*') if p.is_file()
                                 and p.relative_to(directory).parts[0] not in ('fixed-w','task-build')),
                  retained_workspace_file_bytes=sum(p.stat().st_size for p in directory.rglob('*') if p.is_file()),
                  fixed_w_shared_sst_bytes=sum(p.stat().st_size for p in (directory/'fixed-w').rglob('*.sst')
                                              if p.stat().st_nlink>1),
                  exact_result=True,rows=actual.num_rows,transactions_applied_this_process=int(applied),
                  state_changelog_checkpoint_atomic=True,conflicting_replay_rejected=True,
                  fixed_w_checkpoint_replay_exact=True, fixed_w=3,
                  shuffled_arrow_row_order_exact=True, invalid_input_cursor_unchanged=True,
                  fixed_w_checkpoint_bytes=checkpoint_bytes,
                  fixed_w_checkpoint_seconds=checkpoint_seconds,
                  checkpoint_scope=('engine_checkpoint_with_paused_application_writer' if args.worker == 'rocks_arrow_index'
                                    else 'paused_writer_full_copy_prototype_not_online_MVCC'),
                  changes_wall_includes_checkpoint=True,
                  latency_seconds={name:percentile(timing,p) for name,p in [('p50',.5),('p95',.95),('p99',.99)]})
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


def percentile(values,p):
    ordered = sorted(values)
    return ordered[min(len(ordered)-1,int((len(ordered)-1)*p))]


def command(args, method, directory, output, crash=None, recover=False):
    result = [sys.executable,__file__,'--worker',method,'--directory',str(directory),'--output',str(output),
              '--rows',str(args.rows),'--transactions',str(args.transactions),'--changes',str(args.changes),'--width',str(args.width),'--entropy',args.entropy]
    if crash:
        result += ['--crash',crash]
    if recover:
        result += ['--recover']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,default=10000)
    parser.add_argument('--transactions',type=int,default=100)
    parser.add_argument('--changes',type=int,default=50)
    parser.add_argument('--width',type=int,default=0)
    parser.add_argument('--entropy',choices=['low','high'],default='low')
    parser.add_argument('--repeats',type=int,default=5)
    parser.add_argument('--faults',action='store_true')
    parser.add_argument('--rocks',action='store_true',help='add optional rocksdict==0.3.29 candidate')
    parser.add_argument('--worker',choices=['sqlite_arrow_index','duckdb_typed','rocks_arrow_index'])
    parser.add_argument('--directory',type=Path)
    parser.add_argument('--recover',action='store_true')
    parser.add_argument('--crash',choices=['before_commit','after_commit'])
    parser.add_argument('--output',type=Path,default=Path('benchmark-results/state-layout.json'))
    args = parser.parse_args()
    if not 100 <= args.rows <= 100000 or not 5 <= args.transactions <= 1000 or not 1 <= args.changes <= 1000 or not 0 <= args.width <= 4096 or not 1 <= args.repeats <= 10:
        parser.error('bounded synthetic workload required')
    if (args.rows+args.transactions*(args.changes+1))*(args.width+256)>256*1024**2:
        parser.error('fixture estimate must be <=256 MiB')
    if args.worker:
        if not args.directory:
            parser.error('--worker requires --directory')
        return worker(args)
    samples = {method:[] for method in ('sqlite_arrow_index','duckdb_typed')}
    if args.rocks:
        samples['rocks_arrow_index'] = []
    faults = []
    rng = random.Random(42)
    with tempfile.TemporaryDirectory(prefix='m2s-state-layout-') as temp:
        root = Path(temp)
        for repeat in range(args.repeats):
            order = list(samples)
            rng.shuffle(order)
            for method in order:
                output = root/f'{method}-{repeat}.json'
                run = subprocess.run(command(args,method,root/f'{method}-{repeat}',output),capture_output=True,timeout=300)
                if run.returncode:
                    raise RuntimeError('layout worker failed: '+run.stderr.decode(errors='replace')[-2500:])
                samples[method].append(json.loads(output.read_text()))
        if args.faults:
            for method in samples:
                for fault,code in [('before_commit',86),('after_commit',87)]:
                    directory = root/f'{method}-{fault}'
                    output = root/f'{method}-{fault}.json'
                    crashed = subprocess.run(command(args,method,directory,output,fault),capture_output=True,timeout=300)
                    if crashed.returncode!=code:
                        raise AssertionError('crash injection failed: '+crashed.stderr.decode(errors='replace')[-1000:])
                    recovered = subprocess.run(command(args,method,directory,output,recover=True),capture_output=True,timeout=300)
                    if recovered.returncode:
                        raise AssertionError('durable restart failed: '+recovered.stderr.decode(errors='replace')[-1000:])
                    result = json.loads(output.read_text())
                    expected = args.transactions-(4 if fault=='before_commit' else 5)
                    if result['transactions_applied_this_process']!=expected:
                        raise AssertionError('state/changelog/checkpoint commit boundary torn')
                    faults.append(dict(method=method,boundary=fault,exact_result=True,duplicate_commits=0))
    report = dict(format_version=1,kind='candidate_state_layout_AB_not_production',rows=args.rows,
                  transactions=args.transactions,changes_per_transaction=args.changes,width=args.width,entropy=args.entropy,
                  repeats=args.repeats,samples=samples,faults=faults,
                  durability={'sqlite':'FULL_WAL','duckdb':'default_transactional_WAL',
                              'rocks':'WriteBatch_sync_WAL' if args.rocks else 'not_run'},
                  scope='includes_codec_index_state_changelog_commit_scan; no_socket_no_sink_no_GC_no_50M',
                  file_bytes_scope='space_occupancy_after_close_not_IO_write_amplification',
                  peak_rss_scope='whole_process_including_fixtures_and_oracle',
                  versions=dict(python=sys.version.split()[0],duckdb=j4.duckdb.__version__,sqlite=sqlite3.sqlite_version,arrow=j4.pa.__version__))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    # Distinct phase names avoid overwriting wall_seconds in a flattened report.
    summary = {method:{phase:statistics.median(row[phase]['wall_seconds'] for row in values)
                       for phase in ('initial','changes','scan')} for method,values in samples.items()}
    print(json.dumps(dict(summary_wall_seconds=summary,faults=faults)),flush=True)


if __name__=='__main__':
    main()
