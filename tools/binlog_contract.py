#!/usr/bin/env python3
"""Independent Python ROW/FULL oracle and deterministic MySQL wire fixtures.

This is a test oracle, never a production reader. No production packets or rows
are saved. Both decoders consume the same bytes, including TABLE_MAP metadata.
"""
import datetime as dt
from decimal import Decimal
import struct

import pyarrow as pa


DIGITS = (0, 1, 1, 2, 2, 3, 3, 4, 4, 4)
SPECS = [
    ("id", 8, b"", pa.int64(), False),
    ("part", 3, b"", pa.uint32(), True),
    ("i8", 1, b"", pa.int8(), False),
    ("u8", 1, b"", pa.uint8(), True),
    ("i16", 2, b"", pa.int16(), False),
    ("u16", 2, b"", pa.uint16(), True),
    ("i24", 9, b"", pa.int32(), False),
    ("u24", 9, b"", pa.uint32(), True),
    ("i32", 3, b"", pa.int32(), False),
    ("u64", 8, b"", pa.uint64(), True),
    ("f32", 4, b"\x04", pa.float32(), False),
    ("f64", 5, b"\x08", pa.float64(), False),
    ("text", 15, struct.pack("<H", 2048), pa.large_string(), False),
    ("blob", 252, b"\x02", pa.large_binary(), False),
    ("binary", 254, b"\xfe\x08", pa.large_binary(), False),
    ("day", 10, b"", pa.date32(), False),
    ("stamp", 18, b"\x06", pa.timestamp("us"), False),
    ("amount", 246, b"\x13\x04", pa.decimal128(19, 4), False),
    ("fraction", 246, b"\x09\x09", pa.decimal128(9, 9), False),
    ("year", 13, b"", pa.int16(), False),
    ("bits", 16, b"\x01\x01", pa.large_binary(), False),
]


def cursor(data):
    return dict(data=memoryview(data), pos=0)


def take(c, size):
    end = c["pos"] + size
    if size < 0 or end > len(c["data"]):
        raise ValueError("truncated MySQL row event")
    value = c["data"][c["pos"]:end].tobytes()
    c["pos"] = end
    return value


def number(c, size, endian="little", signed=False):
    return int.from_bytes(take(c, size), endian, signed=signed)


def length(c):
    tag = number(c, 1)
    if tag < 251:
        return tag
    if tag not in (252, 253, 254):
        raise ValueError("invalid length-encoded integer")
    return number(c, {252: 2, 253: 3, 254: 8}[tag])


def lenenc(value):
    if value < 251:
        return bytes([value])
    size = 2 if value < 65536 else 3 if value < 16777216 else 8
    return bytes([{2: 252, 3: 253, 8: 254}[size]]) + value.to_bytes(size, "little")


def header(kind, body, position=100):
    return struct.pack("<IBIIIH", 1700000000, kind, 1, 19 + len(body), position, 0) + body


def mapping(specs=SPECS, table="events"):
    names = {1: "tinyint", 2: "smallint", 3: "int", 8: "bigint", 9: "mediumint",
             4: "float", 5: "double", 15: "varchar", 252: "blob", 254: "binary",
             10: "date", 18: "datetime", 246: "decimal", 13: "year", 16: "bit"}
    signature = []
    for name, kind, meta, dtype, unsigned in specs:
        column_type = names[kind] + (" unsigned" if unsigned else "")
        if kind == 246:
            column_type = f"decimal({dtype.precision},{dtype.scale})"
        signature.append((name, names[kind], column_type, "YES", None, None))
    return dict(src_table=table, sr_table=table, primary_key=["id", "part"],
                _schema=[(name, dtype) for name, _, _, dtype, _ in specs],
                _schema_signature=signature)


def table_map(specs=SPECS, table_id=7, table="events"):
    db, name = b"synthetic", table.encode()
    metadata = b"".join(item[2] for item in specs)
    body = (table_id.to_bytes(6, "little") + b"\0\0" + bytes([len(db)]) + db + b"\0"
            + bytes([len(name)]) + name + b"\0" + lenenc(len(specs))
            + bytes(item[1] for item in specs) + lenenc(len(metadata)) + metadata
            + b"\xff" * ((len(specs) + 7) // 8))
    return header(19, body)


def decimal_encode(value, precision, scale):
    integral = precision - scale
    scaled = int(abs(value).scaleb(scale))
    digits = str(scaled).zfill(precision)
    sizes = ([integral % 9] if integral % 9 else []) + [9] * (integral // 9)
    sizes += [9] * (scale // 9) + ([scale % 9] if scale % 9 else [])
    out, pos = bytearray(), 0
    for width in sizes:
        out.extend(int(digits[pos:pos + width]).to_bytes(DIGITS[width], "big"))
        pos += width
    if value < 0:
        out = bytearray(byte ^ 255 for byte in out)
    out[0] ^= 128
    return bytes(out)


def encode_value(spec, value):
    _, kind, meta, dtype, unsigned = spec
    if kind in (1, 2, 3, 8, 9):
        return value.to_bytes({1: 1, 2: 2, 3: 4, 8: 8, 9: 3}[kind], "little", signed=not unsigned)
    if kind in (4, 5):
        return struct.pack("<f" if kind == 4 else "<d", value)
    if kind in (15, 254, 252):
        raw = value.encode() if isinstance(value, str) else value
        n = 2 if kind == 15 else 1 if kind == 254 else meta[0]
        return len(raw).to_bytes(n, "little") + raw
    if kind == 246:
        return decimal_encode(value, dtype.precision, dtype.scale)
    if kind == 10:
        return ((value.year << 9) | (value.month << 5) | value.day).to_bytes(3, "little")
    if kind == 18:
        packed = ((value.year * 13 + value.month) << 22) | (value.day << 17)
        packed |= (value.hour << 12) | (value.minute << 6) | value.second
        return (packed + (1 << 39)).to_bytes(5, "big") + value.microsecond.to_bytes(3, "big")
    if kind == 13:
        return bytes([value - 1900 if value else 0])
    if kind == 16:
        return value
    raise ValueError(kind)


def image(row, specs):
    nulls = sum(1 << i for i, item in enumerate(specs) if row[item[0]] is None)
    return nulls.to_bytes((len(specs) + 7) // 8, "little") + b"".join(
        encode_value(item, row[item[0]]) for item in specs if row[item[0]] is not None)


def row_event(kind, rows, specs=SPECS, table_id=7, v2=True):
    event_type = {"insert": 30, "update": 31, "delete": 32}[kind]
    if not v2:
        event_type -= 7
    bitmap = ((1 << len(specs)) - 1).to_bytes((len(specs) + 7) // 8, "little")
    body = table_id.to_bytes(6, "little") + b"\0\0" + (b"\x02\0" if v2 else b"")
    body += lenenc(len(specs)) + bitmap + (bitmap if kind == "update" else b"")
    body += b"".join(image(row, specs) for row in rows)
    return header(event_type, body)


def parse_map(event):
    c = cursor(event[19:])
    table_id = number(c, 6)
    take(c, 2)
    db = take(c, number(c, 1)).decode()
    take(c, 1)
    table = take(c, number(c, 1)).decode()
    take(c, 1)
    count = length(c)
    types = take(c, count)
    m = cursor(take(c, length(c)))
    metas = []
    for kind in types:
        size = 2 if kind in (15, 254, 253, 246, 16) else 1 if kind in (4, 5, 17, 18, 19, 252, 245, 255) else 0
        metas.append(take(m, size))
    if m["pos"] != len(m["data"]):
        raise ValueError("extra TABLE_MAP metadata")
    take(c, (count + 7) // 8)
    return table_id, db, table, list(zip(types, metas))


def decode_decimal(c, precision, scale):
    integral = precision - scale
    groups = ([integral % 9] if integral % 9 else []) + [9] * (integral // 9)
    groups += [9] * (scale // 9) + ([scale % 9] if scale % 9 else [])
    raw = bytearray(take(c, sum(DIGITS[width] for width in groups)))
    positive = bool(raw[0] & 128)
    raw[0] ^= 128
    if not positive:
        raw = bytearray(byte ^ 255 for byte in raw)
    packed, offset = 0, 0
    for width in groups:
        size = DIGITS[width]
        group = int.from_bytes(raw[offset:offset + size], "big")
        if group >= 10 ** width:
            raise ValueError("invalid decimal group")
        packed = packed * 10 ** width + group
        offset += size
    # Construct a Decimal tuple: Python's default decimal context has only 28 digits.
    return Decimal((int(not positive), tuple(map(int, str(packed))), -scale))


def decode_value(c, kind, meta, dtype, unsigned):
    if kind in (1, 2, 3, 8, 9):
        return number(c, {1: 1, 2: 2, 3: 4, 8: 8, 9: 3}[kind], signed=not unsigned)
    if kind in (4, 5):
        return struct.unpack("<f" if kind == 4 else "<d", take(c, 4 if kind == 4 else 8))[0]
    if kind in (15, 253, 254, 252, 249, 250, 251, 255):
        if kind == 15:
            size = 2 if int.from_bytes(meta, "little") > 255 else 1
        elif kind in (253, 254):
            maximum = (((int.from_bytes(meta, "big") >> 4) & 768) ^ 768) + meta[1]
            size = 2 if maximum > 255 else 1
        else:
            size = meta[0]
        raw = take(c, number(c, size))
        if kind == 254 and (pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype)):
            raw = raw.ljust(maximum, b"\0")
        return raw.decode("utf-8") if pa.types.is_string(dtype) or pa.types.is_large_string(dtype) else raw
    if kind == 246:
        return decode_decimal(c, meta[0], meta[1])
    if kind in (10, 14):
        value = number(c, 3)
        return dt.date(value >> 9, (value >> 5) & 15, value & 31) if value else None
    if kind == 18:
        value = number(c, 5, "big") - (1 << 39)
        ym, day = value >> 22, (value >> 17) & 31
        year, month = divmod(ym, 13)
        fsp = meta[0]
        micros = number(c, (fsp + 1) // 2, "big") * (10000 if fsp <= 2 else 100 if fsp <= 4 else 1) if fsp else 0
        return dt.datetime(year, month, day, (value >> 12) & 31, (value >> 6) & 63, value & 63, micros) if year and month and day else None
    if kind == 13:
        year = number(c, 1)
        return year + 1900 if year else 0
    if kind == 16:
        return take(c, (meta[1] * 8 + meta[0] + 7) // 8)
    raise ValueError(f"unsupported MySQL type {kind}")


def decode_rows(event, table_info, mapping_info):
    """Decode raw bytes independently of the C decoder and fixture encoder."""
    kind = event[4]
    c = cursor(event[19:])
    table_id = number(c, 6)
    take(c, 2)
    if table_id != table_info[0]:
        raise ValueError("missing TABLE_MAP")
    if kind in (30, 31, 32):
        take(c, number(c, 2) - 2)
    count = length(c)
    schema = mapping_info["_schema"]
    if count != len(schema):
        raise ValueError("column count mismatch")
    present = take(c, (count + 7) // 8)
    update = kind in (24, 31)
    if update and take(c, (count + 7) // 8) != present:
        raise ValueError("FULL image required")
    if any(not (present[i // 8] & (1 << (i % 8))) for i in range(count)):
        raise ValueError("FULL image required")
    rows = []
    while c["pos"] < len(c["data"]):
        nulls = take(c, (count + 7) // 8)
        row = {}
        for i, ((name, dtype), (mysql_type, meta)) in enumerate(zip(schema, table_info[3])):
            unsigned = "unsigned" in mapping_info["_schema_signature"][i][2].lower()
            row[name] = None if nulls[i // 8] & (1 << (i % 8)) else decode_value(c, mysql_type, meta, dtype, unsigned)
        row["_sync_op"] = int(kind in (25, 32) or (update and len(rows) % 2 == 0))
        row["_sync_order"] = len(rows)
        rows.append(row)
    if update and len(rows) % 2:
        raise ValueError("missing update after-image")
    return pa.Table.from_pylist(rows, schema=pa.schema(
        [pa.field(name, dtype) for name, dtype in schema]
        + [pa.field("_sync_op", pa.int8()), pa.field("_sync_order", pa.int64())]))


def random_row(rng, index):
    row = {}
    for name, kind, meta, dtype, unsigned in SPECS:
        if kind in (1, 2, 3, 8, 9):
            bits = {1: 8, 2: 16, 3: 32, 8: 64, 9: 24}[kind]
            lo, hi = (0, (1 << bits) - 1) if unsigned else (-(1 << (bits - 1)), (1 << (bits - 1)) - 1)
            row[name] = rng.choice([lo, hi, 0, rng.randint(lo, hi)])
        elif kind in (4, 5):
            row[name] = rng.choice([0.0, -0.0, 1.25, -12345.5])
        elif kind == 15:
            row[name] = rng.choice(["", "中文🙂", "\x00\n\"\\", "x" * 300])
        elif kind in (252, 254):
            row[name] = rng.choice([b"", b"\x00\xff\n", rng.randbytes(8)])
            if kind == 254:
                row[name] = row[name].ljust(8, b"\0")
        elif kind == 10:
            row[name] = rng.choice([dt.date(1000, 1, 1), dt.date(9999, 12, 31), dt.date(2024, 2, 29)])
        elif kind == 18:
            row[name] = rng.choice([dt.datetime(1000, 1, 1), dt.datetime(9999, 12, 31, 23, 59, 59, 999999), dt.datetime(2024, 2, 29, 12, 34, 56, 123456)])
        elif kind == 246:
            row[name] = Decimal(rng.randint(-(10 ** dtype.precision - 1), 10 ** dtype.precision - 1)).scaleb(-dtype.scale)
        elif kind == 13:
            row[name] = rng.choice([0, 1901, 2155])
        elif kind == 16:
            row[name] = rng.randint(0, 511).to_bytes(2, "big")
        if name not in ("id", "part") and rng.randrange(5) == 0:
            row[name] = None
    row["id"], row["part"] = index, index % 7
    return row
