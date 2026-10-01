#!/usr/bin/env python3
"""Durable retractable GROUP BY state for P8B COUNT/SUM/AVG.

Consumes an already-filtered keyed change stream where delete=1 carries the
full before image and upsert=0 carries the full after image. SQL compilation is
kept separate until this state/recovery contract is proven.
"""
import base64
import contextlib
import datetime
import decimal
import hashlib
import json
import math
import pickle
import time


FORMAT_VERSION = 1
FUNCTIONS = {"count", "sum", "avg"}


@contextlib.contextmanager
def transaction(con):
    if con.in_transaction:
        yield
        return
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def install(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS aggregate_states(
            state_id TEXT PRIMARY KEY,
            spec_hash TEXT NOT NULL,
            spec_json TEXT NOT NULL,
            watermark INTEGER NOT NULL,
            last_digest TEXT,
            created REAL NOT NULL,
            updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS aggregate_groups(
            state_id TEXT NOT NULL
                REFERENCES aggregate_states(state_id) ON DELETE CASCADE,
            key_blob BLOB NOT NULL,
            key_payload BLOB NOT NULL,
            row_count INTEGER NOT NULL CHECK(row_count>0),
            accum_payload BLOB NOT NULL,
            PRIMARY KEY(state_id,key_blob));
    """)


def _text(value, name):
    value = str(value or "").strip()
    if not value:
        raise ValueError(name + " must be non-empty")
    return value


def aggregate_spec(group_keys, aggregates):
    group_keys = [str(value).strip() for value in group_keys or ()]
    if not group_keys or any(not value for value in group_keys):
        raise ValueError("aggregate group_keys must be non-empty")
    if len(group_keys) != len(set(group_keys)):
        raise ValueError("aggregate group_keys must be unique")
    items = []
    outputs = set(group_keys)
    for item in aggregates or ():
        if not isinstance(item, dict):
            raise ValueError("aggregate item must be a dict")
        function = str(item.get("function") or "").strip().lower()
        output = str(item.get("output") or "").strip()
        input_name = str(item.get("input") or "").strip()
        if function not in FUNCTIONS:
            raise ValueError("unsupported aggregate function: " + function)
        if not output or output in outputs:
            raise ValueError("aggregate output must be unique")
        if function == "count":
            input_name = input_name or "*"
        elif not input_name or input_name == "*":
            raise ValueError(function + " requires one input column")
        outputs.add(output)
        items.append(dict(output=output,function=function,input=input_name))
    if not items:
        raise ValueError("at least one aggregate is required")
    spec = dict(
        format_version=FORMAT_VERSION,
        group_keys=group_keys,
        aggregates=items,
    )
    validate_spec(spec)
    return spec


def validate_spec(spec):
    if not isinstance(spec, dict):
        raise ValueError("aggregate spec must be a dict")
    if set(spec) != {"format_version", "group_keys", "aggregates"}:
        raise ValueError("aggregate spec fields differ from format v1")
    if int(spec["format_version"]) != FORMAT_VERSION:
        raise ValueError("unsupported aggregate spec format")
    group_keys = spec["group_keys"]
    if (
        not isinstance(group_keys, list)
        or not group_keys
        or not all(isinstance(value, str) and value for value in group_keys)
        or len(group_keys) != len(set(group_keys))
    ):
        raise ValueError("invalid aggregate group_keys")
    outputs = set(group_keys)
    if not isinstance(spec["aggregates"], list) or not spec["aggregates"]:
        raise ValueError("invalid aggregate list")
    for item in spec["aggregates"]:
        if (
            not isinstance(item, dict)
            or set(item) != {"output", "function", "input"}
        ):
            raise ValueError("invalid aggregate item")
        output = str(item["output"])
        function = str(item["function"])
        input_name = str(item["input"])
        if not output or output in outputs:
            raise ValueError("duplicate aggregate output")
        if function not in FUNCTIONS:
            raise ValueError("unsupported aggregate function")
        if function == "count":
            if not input_name:
                raise ValueError("count input cannot be empty")
        elif not input_name or input_name == "*":
            raise ValueError(function + " requires one input column")
        outputs.add(output)
    return spec


def canonical_bytes(value):
    return json.dumps(
        value,ensure_ascii=False,sort_keys=True,
        separators=(",",":")).encode("utf-8")


def semantic_id(spec):
    validate_spec(spec)
    return hashlib.sha256(canonical_bytes(spec)).hexdigest()


def create_state(con, state_id, spec, watermark=0):
    state_id = _text(state_id, "state_id")
    spec = validate_spec(spec)
    watermark = int(watermark)
    if watermark < 0:
        raise ValueError("aggregate watermark cannot be negative")
    now = time.time()
    with transaction(con):
        con.execute("""
            INSERT INTO aggregate_states(
                state_id,spec_hash,spec_json,watermark,last_digest,
                created,updated)
            VALUES(?,?,?,?,NULL,?,?)
        """, (
            state_id,semantic_id(spec),
            canonical_bytes(spec).decode("utf-8"),
            watermark,now,now,
        ))
    return state_info(con,state_id)


def state_info(con, state_id):
    row = con.execute("""
        SELECT spec_hash,spec_json,watermark,last_digest,created,updated
        FROM aggregate_states WHERE state_id=?
    """,(str(state_id),)).fetchone()
    if not row:
        raise KeyError("aggregate state does not exist")
    spec = json.loads(row[1])
    actual = semantic_id(spec)
    if actual != str(row[0]):
        raise RuntimeError("aggregate state spec hash does not match persisted spec")
    return dict(
        state_id=str(state_id),spec_hash=actual,spec=spec,
        watermark=int(row[2]),
        last_digest=None if row[3] is None else str(row[3]),
        created=float(row[4]),updated=float(row[5]),
    )


def ensure_state(con, state_id, spec, watermark=0):
    try:
        current = state_info(con,state_id)
    except KeyError:
        return create_state(con,state_id,spec,watermark)
    if current["spec_hash"] != semantic_id(spec):
        raise RuntimeError("aggregate state id was reused for new semantics")
    if current["watermark"] < int(watermark):
        raise RuntimeError(
            "existing aggregate state is behind requested initial watermark")
    return current


def _tag(value):
    if value is None:
        return ["null"]
    if isinstance(value,bool):
        return ["bool",1 if value else 0]
    if isinstance(value,int):
        return ["int",str(value)]
    if isinstance(value,decimal.Decimal):
        return ["decimal",format(value,"f")]
    if isinstance(value,float):
        if not math.isfinite(value):
            raise ValueError("non-finite aggregate values are unsupported")
        return ["float",value.hex()]
    if isinstance(value,str):
        return ["str",value]
    if isinstance(value,(bytes,bytearray,memoryview)):
        return ["bytes",base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value,datetime.datetime):
        return ["datetime",value.isoformat()]
    if isinstance(value,datetime.date):
        return ["date",value.isoformat()]
    if isinstance(value,datetime.time):
        return ["time",value.isoformat()]
    raise TypeError("unsupported aggregate value type: "+type(value).__name__)


def _change_digest(source_seq, changes):
    encoded = []
    for row in changes:
        if not isinstance(row,dict):
            raise ValueError("aggregate change must be a dict")
        encoded.append([
            [str(name),_tag(row[name])]
            for name in sorted(row)
        ])
    return hashlib.sha256(canonical_bytes(dict(
        source_seq=int(source_seq),changes=encoded))).hexdigest()


def _group_key(spec, row):
    try:
        values = tuple(row[name] for name in spec["group_keys"])
    except KeyError as exc:
        raise ValueError(
            "aggregate change lacks group key "+str(exc.args[0])) from None
    return (
        canonical_bytes([_tag(value) for value in values]),
        pickle.dumps(values,protocol=5),
    )


def _initial_accum(spec):
    result = {}
    for item in spec["aggregates"]:
        if item["function"] == "count":
            result[item["output"]] = dict(count=0)
        else:
            result[item["output"]] = dict(nonnull=0,sum=None)
    return result


def _numeric(value, output):
    if isinstance(value,bool) or not isinstance(
        value,(int,float,decimal.Decimal)
    ):
        raise TypeError(
            "aggregate "+output+" requires int/float/Decimal input")
    if isinstance(value,float) and not math.isfinite(value):
        raise ValueError("non-finite aggregate values are unsupported")
    return value


def _apply_change_locked(con, state_id, spec, row):
    if "_sync_op" not in row:
        raise ValueError("aggregate change lacks _sync_op")
    op = int(row["_sync_op"])
    if op not in (0,1):
        raise ValueError("aggregate _sync_op must be 0(upsert) or 1(delete)")
    sign = 1 if op == 0 else -1
    key_blob,key_payload = _group_key(spec,row)
    existing = con.execute("""
        SELECT row_count,accum_payload
        FROM aggregate_groups
        WHERE state_id=? AND key_blob=?
    """,(str(state_id),key_blob)).fetchone()
    if existing is None:
        row_count = 0
        accum = _initial_accum(spec)
    else:
        row_count = int(existing[0])
        accum = pickle.loads(existing[1])

    if sign < 0 and row_count == 0:
        raise RuntimeError("aggregate retract references a missing group")
    next_rows = row_count+sign
    if next_rows < 0:
        raise RuntimeError("aggregate group row count became negative")

    for item in spec["aggregates"]:
        output = item["output"]
        function = item["function"]
        input_name = item["input"]
        state = accum[output]
        if input_name == "*":
            value = None
        else:
            if input_name not in row:
                raise ValueError(
                    "aggregate change lacks input column " + input_name)
            value = row[input_name]

        if function == "count":
            contributes = input_name == "*" or value is not None
            if contributes:
                state["count"] = int(state["count"])+sign
                if state["count"] < 0:
                    raise RuntimeError(
                        "aggregate count became negative for "+output)
            if state["count"] > next_rows:
                raise RuntimeError(
                    "aggregate count exceeds group cardinality for "+output)
            if input_name == "*" and state["count"] != next_rows:
                raise RuntimeError("count(*) diverged from group cardinality")
            continue

        if value is None:
            continue
        value = _numeric(value,output)
        previous_nonnull = int(state["nonnull"])
        if sign < 0 and previous_nonnull == 0:
            raise RuntimeError(
                "aggregate retract references missing non-NULL input")
        next_nonnull = previous_nonnull+sign
        if next_nonnull < 0 or next_nonnull > next_rows:
            raise RuntimeError(
                "aggregate non-NULL count violates group cardinality")
        total = state["sum"]
        if sign > 0:
            total = value if previous_nonnull == 0 else total+value
        else:
            total = total-value
        state["nonnull"] = next_nonnull
        state["sum"] = None if next_nonnull == 0 else total

    if next_rows == 0:
        for item in spec["aggregates"]:
            state = accum[item["output"]]
            if item["function"] == "count":
                if int(state["count"]) != 0:
                    raise RuntimeError("empty aggregate group retains COUNT state")
            elif int(state["nonnull"]) != 0 or state["sum"] is not None:
                raise RuntimeError("empty aggregate group retains SUM/AVG state")
        con.execute("""
            DELETE FROM aggregate_groups
            WHERE state_id=? AND key_blob=?
        """,(str(state_id),key_blob))
        return

    con.execute("""
        INSERT INTO aggregate_groups(
            state_id,key_blob,key_payload,row_count,accum_payload)
        VALUES(?,?,?,?,?)
        ON CONFLICT(state_id,key_blob) DO UPDATE SET
            key_payload=excluded.key_payload,
            row_count=excluded.row_count,
            accum_payload=excluded.accum_payload
    """,(
        str(state_id),key_blob,key_payload,next_rows,
        pickle.dumps(accum,protocol=5),
    ))


def apply_transaction(con, state_id, source_seq, changes):
    source_seq = int(source_seq)
    changes = list(changes or ())
    digest = _change_digest(source_seq,changes)
    with transaction(con):
        row = con.execute("""
            SELECT spec_json,watermark,last_digest
            FROM aggregate_states WHERE state_id=?
        """,(str(state_id),)).fetchone()
        if not row:
            raise KeyError("aggregate state does not exist")
        spec = validate_spec(json.loads(row[0]))
        watermark = int(row[1])
        last_digest = None if row[2] is None else str(row[2])
        if source_seq == watermark:
            if digest == last_digest:
                return False
            raise RuntimeError(
                "aggregate source_seq retry has a different payload")
        if source_seq != watermark+1:
            raise RuntimeError(
                "aggregate source_seq gap/regression expected=%d got=%d"
                % (watermark+1,source_seq))
        for change in changes:
            _apply_change_locked(con,state_id,spec,change)
        con.execute("""
            UPDATE aggregate_states
            SET watermark=?,last_digest=?,updated=?
            WHERE state_id=?
        """,(source_seq,digest,time.time(),str(state_id)))
    return True


def read_rows(con, state_id):
    info = state_info(con,state_id)
    spec = info["spec"]
    result = []
    rows = con.execute("""
        SELECT key_payload,row_count,accum_payload
        FROM aggregate_groups
        WHERE state_id=?
        ORDER BY key_blob
    """,(str(state_id),)).fetchall()
    for key_payload,row_count,accum_payload in rows:
        keys = pickle.loads(key_payload)
        accum = pickle.loads(accum_payload)
        item = dict(zip(spec["group_keys"],keys))
        for aggregate in spec["aggregates"]:
            output = aggregate["output"]
            function = aggregate["function"]
            state = accum[output]
            if function == "count":
                item[output] = int(state["count"])
            elif function == "sum":
                item[output] = state["sum"]
            else:
                item[output] = (
                    None if int(state["nonnull"]) == 0
                    else float(state["sum"]/int(state["nonnull"])))
        item["_row_count"] = int(row_count)
        result.append(item)
    return result
