#!/usr/bin/env python3
"""Same-byte Python/native differential tests, optionally against live MySQL."""
import argparse
import json
import os
from pathlib import Path
import random
import struct
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import j4
from binlog_contract import SPECS, decode_rows, mapping, parse_map, random_row, row_event, table_map


def config(binary, prepared):
    return dict(native_binlog_path=str(binary.resolve()), query_timeout=10,
                mysql=dict(database="synthetic"))


def equal(expected, actual, label):
    if not actual.schema.equals(expected.schema) or not actual.equals(expected):
        # Do not include field values: a user can also run this locally on a test service.
        differing = [name for name in expected.column_names
                     if name not in actual.column_names or not expected[name].equals(actual[name])]
        raise AssertionError(f"{label}: schema/value/null/order mismatch columns={differing}")


def synthetic(binary, cases, seed):
    rng = random.Random(seed)
    prepared = mapping()
    cfg = config(binary, [prepared])
    decoder = j4.native_start(cfg, [prepared])
    total_rows, snapshots = 0, 0
    try:
        tm = table_map()
        info = parse_map(tm)
        if j4.native_decode_event(decoder, tm, 10) is not None:
            raise AssertionError("TABLE_MAP must return ACK")
        for case in range(cases):
            kind = ("insert", "update", "delete")[case % 3]
            rows = [random_row(rng, case * 8 + i) for i in range(1 + case % 4)]
            if kind == "update":
                before_after = []
                for row in rows:
                    after = random_row(rng, row["id"] + 100000)
                    before_after.extend([row, after])
                rows = before_after
            raw = row_event(kind, rows, v2=case % 2 == 0)
            expected = decode_rows(raw, info, prepared)
            db, table, actual = j4.native_decode_event(decoder, raw, 10)
            if (db, table) != ("synthetic", "events"):
                raise AssertionError("decoder table identity mismatch")
            equal(expected, actual, f"seed={seed} event={case}")
            total_rows += actual.num_rows
            if case % 10 == 0:
                packets = []
                for row in rows:
                    payload = bytearray()
                    for name, kind_id, _, _, _ in SPECS:
                        value = row[name]
                        if value is None:
                            payload.extend(b"\xfb")
                            continue
                        value = value.isoformat(" ") if kind_id == 18 else value.isoformat() if kind_id == 10 else value
                        data = value if isinstance(value, bytes) else str(value).encode()
                        from binlog_contract import lenenc
                        payload.extend(lenenc(len(data)))
                        payload.extend(data)
                    packets.append(bytes(payload))
                actual = j4.native_snapshot_decode(decoder, cfg, prepared, packets, 100)
                expected = j4.snapshot_arrow(prepared, [tuple(row[name] for name, *_ in SPECS) for row in rows])
                expected = expected.set_column(expected.num_columns - 1, "_sync_order", j4.pa.array(range(100, 100 + len(rows)), type=j4.pa.int64()))
                equal(expected, actual, f"snapshot seed={seed} case={case}")
                snapshots += len(rows)
        ignored_tm = table_map(table="ignored")
        j4.native_decode_event(decoder, ignored_tm, 10)
        if j4.native_decode_event(decoder, row_event("insert", [random_row(rng, 1)]), 10) is not None:
            raise AssertionError("explicitly ignored table must return ACK")
        j4.native_reset(decoder, cfg, [prepared])
        try:
            j4.native_decode_event(decoder, row_event("insert", [random_row(rng, 1)]), 10)
        except j4.NativeDecoderError as exc:
            if "unknown TABLE_MAP" not in str(exc):
                raise
        else:
            raise AssertionError("unknown TABLE_MAP was silently accepted")
    finally:
        j4.native_stop(decoder)
    return dict(kind="same_raw_event_differential", seed=seed, events=cases,
                row_images=total_rows, snapshot_rows=snapshots, correctness="equal",
                columns=len(SPECS), source="deterministic_mysql_wire_fixture")


def frame(kind, payload):
    return struct.pack("<cI", kind, len(payload)) + payload


def frames(data):
    output = []
    while data:
        if len(data) < 5:
            raise AssertionError("truncated decoder output")
        kind, size = struct.unpack_from("<cI", data)
        if len(data) < size + 5:
            raise AssertionError("truncated decoder payload")
        output.append((kind, data[5:5 + size]))
        data = data[5 + size:]
    return output


def faults(binary, cases, seed):
    rng = random.Random(seed)
    cfg = config(binary, [])
    payload = j4.native_config_payload(cfg, [mapping()])
    tm = table_map()
    row = row_event("insert", [random_row(rng, 1)])
    categories = {}
    for case in range(cases):
        category = ("config_truncated", "input_truncated", "unknown_map", "oversize",
                    "invalid_config_kind", "invalid_decimal", "unknown_frame", "missing_nullable")[case % 8]
        prefix = frame(b"C", payload)
        if category == "config_truncated":
            # This used to dereference a NULL column allocation during decoder_reset.
            bad = payload[:rng.randrange(len(payload))]
            request = prefix + frame(b"C", bad)
        elif category == "input_truncated":
            cut = rng.randrange(len(row))
            request = prefix + frame(b"E", tm) + struct.pack("<cI", b"E", len(row)) + row[:cut]
        elif category == "unknown_map":
            request = prefix + frame(b"E", row)
        elif category == "oversize":
            request = prefix + struct.pack("<cI", b"E", j4.NATIVE_MAX_FRAME_BYTES + 1 + case)
        elif category == "invalid_config_kind":
            # Last field is BIT, whose config record ends with kind/unsigned/precision/scale.
            bad = bytearray(payload)
            bad[-4] = 255
            request = prefix + frame(b"C", bad)
        elif category == "invalid_decimal":
            bad = bytearray(tm)
            at = bad.index(b"\x13\x04\x09\x09", 19)
            bad[at] = 0
            request = prefix + frame(b"E", bad)
        elif category == "missing_nullable":
            request = prefix + frame(b"E", tm[:-3])
        else:
            request = prefix + frame(b"?", rng.randbytes(case % 32))
        proc = subprocess.run([str(binary), "--stdio"], input=request,
                              capture_output=True, timeout=10)
        if proc.returncode not in (2, 3):
            raise AssertionError(f"fault seed={seed} case={case} category={category} rc={proc.returncode}")
        result = frames(proc.stdout)
        if category != "input_truncated" and not any(kind == b"X" for kind, _ in result):
            raise AssertionError(f"missing diagnostic category={category}")
        categories[category] = categories.get(category, 0) + 1
    return dict(kind="native_protocol_faults", seed=seed, cases=cases,
                categories=categories, crashes=0, unsafe_ack=0)


def live(binary, cases, seed):
    """Use only an isolated disposable MySQL instance: destroys synthetic DB."""
    import pymysql
    options = dict(host=os.environ.get("M2S_TEST_MYSQL_HOST", "127.0.0.1"),
                   port=int(os.environ.get("M2S_TEST_MYSQL_PORT", "3306")),
                   user=os.environ.get("M2S_TEST_MYSQL_USER", "root"),
                   password=os.environ.get("M2S_TEST_MYSQL_PASSWORD", ""),
                   charset="utf8mb4", autocommit=True, read_timeout=15)
    con = pymysql.connect(**options)
    rng = random.Random(seed)
    cfg = config(binary, [])
    columns = """id BIGINT, part INT UNSIGNED, i8 TINYINT, u8 TINYINT UNSIGNED,
        i16 SMALLINT, u16 SMALLINT UNSIGNED, i24 MEDIUMINT, u24 MEDIUMINT UNSIGNED,
        i32 INT, u64 BIGINT UNSIGNED, f32 FLOAT, f64 DOUBLE, text VARCHAR(512),
        blob BLOB, `binary` BINARY(8), day DATE, stamp DATETIME(6),
        amount DECIMAL(19,4), fraction DECIMAL(9,9), year YEAR, bits BIT(9),
        PRIMARY KEY(id,part)"""
    import re
    columns = re.sub(r"(?m)(^|,)\s*(`?[a-z][a-z0-9_]*`?)\s+(?=[A-Z])",
                     lambda m: m[1] + " `" + m[2].strip("`") + "` ", columns)
    with con.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS synthetic")
        cur.execute("CREATE DATABASE synthetic CHARACTER SET utf8mb4")
        cur.execute("CREATE TABLE synthetic.events(" + columns + ")")
        cur.execute("SHOW BINARY LOG STATUS")
        start = cur.fetchone()[:2]
        for case in range(cases):
            rows = [random_row(rng, case * 10 + i) for i in range(1 + case % 4)]
            sql = "INSERT INTO synthetic.events VALUES(" + ",".join(["%s"] * len(SPECS)) + ")"
            con.begin()
            cur.executemany(sql, [tuple(row[name] for name, *_ in SPECS) for row in rows])
            if case % 2:
                cur.execute("UPDATE synthetic.events SET id=id+100000000,text=%s WHERE id=%s AND part=%s",
                            ("changed🙂", rows[0]["id"], rows[0]["part"]))
            if case % 3 == 0:
                cur.execute("DELETE FROM synthetic.events WHERE id=%s AND part=%s", (rows[-1]["id"], rows[-1]["part"]))
            con.commit()
        cur.execute("SHOW BINARY LOG STATUS")
        end = cur.fetchone()[:2]
        cur.execute("SELECT * FROM synthetic.events ORDER BY id,part")
        source_final = j4.snapshot_arrow(mapping(), cur.fetchall())
    con.close()
    cfg.update(mysql=dict(options, database="synthetic"), query_timeout=15, server_id=198611)
    prepared = mapping()
    decoder = j4.native_start(cfg, [prepared])
    stream = j4.replication_open_stream(cfg, start, None)
    maps, events, rows, transactions, sink = {}, 0, 0, 0, {}
    try:
        while True:
            packet = j4.replication_read_packet(stream)
            raw = j4.native_raw_event(packet, stream["use_checksum"])
            _, kind, _, _, pos, _ = j4.native_event_header(raw)
            position = j4.native_advance_position(stream, raw, kind, pos)
            if kind == 19:
                info = parse_map(raw)
                maps[info[0]] = info
                j4.native_decode_event(decoder, raw, 15)
            elif kind in (23, 24, 25, 30, 31, 32):
                info = maps[int.from_bytes(raw[19:25], "little")]
                expected = decode_rows(raw, info, prepared)
                db, name, actual = j4.native_decode_event(decoder, raw, 15)
                if (db, name) != ("synthetic", "events"):
                    raise AssertionError("unexpected table")
                equal(expected, actual, f"live event={events}")
                for mutation in actual.to_pylist():
                    key = (mutation["id"], mutation["part"])
                    op = mutation.pop("_sync_op")
                    mutation.pop("_sync_order")
                    if op:
                        sink.pop(key, None)
                    else:
                        sink[key] = mutation
                events += 1
                rows += actual.num_rows
            elif kind == 16:
                transactions += 1
            if j4.position_ge(position, end):
                break
    finally:
        j4.replication_close_stream(stream)
        j4.native_stop(decoder)
    if transactions != cases or not events:
        raise AssertionError(f"live coverage incomplete transactions={transactions} expected={cases}")
    expected_rows = source_final.drop(["_sync_op", "_sync_order"])
    final = j4.pa.Table.from_pylist([sink[key] for key in sorted(sink)], schema=expected_rows.schema)
    equal(expected_rows, final, "replayed native output vs actual MySQL final SELECT")
    return dict(kind="live_mysql_differential", transactions=transactions, events=events,
                row_images=rows, correctness="equal", seed=seed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "build/native/mysql_arrow_reader")
    parser.add_argument("--cases", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--faults", action="store_true")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.cases < 1 or args.cases > 100000:
        parser.error("cases must be 1..100000")
    started = time.perf_counter()
    results = [live(args.binary, args.cases, args.seed) if args.live
               else synthetic(args.binary, args.cases, args.seed)]
    if args.faults:
        results.append(faults(args.binary, args.cases, args.seed))
    report = dict(format_version=1, tests=results, seconds=time.perf_counter() - started)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
