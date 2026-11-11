#!/usr/bin/env python3
"""Event time, windows, top-n, interval joins and recursive CTEs.

Tumbling, hopping and cumulative windows update only the windows a row
touches. Session windows, OVER and top-n recompute the affected key or
partition. Interval joins recompute the affected equi-join key. A recursive
CTE reruns a semi-naive fixed point when its base inputs change.
"""
import pyarrow as pa

import stream_zset


def _positive_ts(value, label):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(label + " must be an int")
    if value < 0:
        raise ValueError(label + " cannot be negative")
    return int(value)


def window_bounds(kind, ts, size, slide, step):
    ts = _positive_ts(ts, "event time")
    size = _positive_ts(size, "window size")
    if size <= 0:
        raise ValueError("window size must be positive")
    if kind == "tumble":
        start = (ts // size) * size
        return [(start, start + size)]
    if kind == "hop":
        slide = _positive_ts(slide, "window slide")
        if slide <= 0:
            raise ValueError("window slide must be positive")
        last = (ts // slide) * slide
        span = ((size - 1) // slide) * slide
        first = last - span
        bounds = []
        start = first
        while start <= ts:
            if start <= ts < start + size:
                bounds.append((start, start + size))
            start += slide
        return bounds
    if kind == "cumulate":
        step = _positive_ts(step, "cumulative step")
        if step <= 0 or size % step != 0:
            raise ValueError("cumulative step must divide the window size")
        base = (ts // size) * size
        bounds = []
        end = base + step
        while end <= base + size:
            if ts < end:
                bounds.append((base, end))
            end += step
        return bounds
    raise ValueError("unsupported window kind: " + str(kind))


def _group_identity(row, keys):
    try:
        values = tuple(row[name] for name in keys)
    except KeyError as exc:
        raise ValueError(
            "window input lacks column " + str(exc.args[0])) from None
    return tuple(stream_zset.tag(item) for item in values), values


def _initial_accum(aggregates):
    state = {}
    for item in aggregates:
        function = item["function"]
        if function == "count":
            state[item["output"]] = dict(count=0)
        elif function == "sum":
            state[item["output"]] = dict(total=None, nonnull=0)
        elif function in {"min", "max"}:
            state[item["output"]] = dict(values={})
        else:
            raise ValueError("unsupported window aggregate: " + function)
    return state


def _apply_accum(state, row, weight, aggregates):
    for item in aggregates:
        function = item["function"]
        output = item["output"]
        slot = state[output]
        if function == "count":
            if item["input"] == "*" or row.get(item["input"]) is not None:
                slot["count"] = int(slot["count"]) + weight
                if slot["count"] < 0:
                    raise RuntimeError("window count became negative")
            continue
        value = row.get(item["input"])
        if value is None:
            continue
        if function == "sum":
            slot["nonnull"] = int(slot["nonnull"]) + weight
            if slot["nonnull"] < 0:
                raise RuntimeError("window sum count became negative")
            if slot["nonnull"] == 0:
                slot["total"] = None
            elif slot["total"] is None:
                slot["total"] = value * weight
            else:
                slot["total"] = slot["total"] + value * weight
            continue
        identity = stream_zset.tag(value)
        count = int(slot["values"].get(identity, [value, 0])[1]) + weight
        if count < 0:
            raise RuntimeError("window extreme count became negative")
        if count == 0:
            slot["values"].pop(identity, None)
        else:
            slot["values"][identity] = [value, count]


def _render_accum(keys, key_values, window, aggregates, state):
    row = {}
    for name, value in zip(keys, key_values):
        row[name] = value
    row["w_start"] = int(window[0])
    row["w_end"] = int(window[1])
    for item in aggregates:
        function = item["function"]
        slot = state[item["output"]]
        if function == "count":
            row[item["output"]] = int(slot["count"])
        elif function == "sum":
            row[item["output"]] = slot["total"]
        else:
            chosen = None
            chosen_key = None
            for value, _count in slot["values"].values():
                identity = stream_zset.tag(value)
                if (
                    chosen is None
                    or (function == "max" and identity > chosen_key)
                    or (function == "min" and identity < chosen_key)
                ):
                    chosen = value
                    chosen_key = identity
            row[item["output"]] = chosen
    return row


def _session_bounds(rows, gap, time_name):
    ordered = sorted(
        (row[time_name], row, weight)
        for row, weight in rows
        if weight > 0)
    if not ordered:
        return []
    sessions = []
    start = ordered[0][0]
    previous = ordered[0][0]
    members = [(ordered[0][1], ordered[0][2])]
    for ts, row, weight in ordered[1:]:
        if ts - previous > gap:
            sessions.append((start, previous, members))
            start = ts
            members = []
        members.append((row, weight))
        previous = ts
    sessions.append((start, previous, members))
    return sessions


def eval_window(state, delta, node):
    kind = node["kind"]
    time_name = node["time"]
    keys = list(node["keys"])
    aggregates = node["aggregates"]
    lateness = int(node["lateness"])
    policy = node["late_policy"]
    ttl = node["ttl"]
    emitted = []
    incoming = stream_zset.consolidate(delta)
    observed = state.get("max_ts")
    for row, weight in incoming:
        if weight > 0:
            ts = _positive_ts(row[time_name], "event time")
            if observed is None or ts > observed:
                observed = ts
    state["max_ts"] = observed
    watermark = None if observed is None else observed - lateness

    def late(row):
        if watermark is None or policy == "recompute":
            return False
        return _positive_ts(row[time_name], "event time") < watermark

    if kind == "session":
        return _eval_sessions(
            state, incoming, node, late, watermark, ttl, emitted)
    groups = state.setdefault("groups", {})
    for row, weight in incoming:
        if policy == "drop" and late(row):
            continue
        ts = _positive_ts(row[time_name], "event time")
        identity, key_values = _group_identity(row, keys)
        for bound in window_bounds(
            kind, ts, int(node["size"]),
            node.get("slide"), node.get("step"),
        ):
            group_key = identity + (
                stream_zset.tag(bound[0]), stream_zset.tag(bound[1]))
            slot = groups.get(group_key)
            old = None
            if slot is not None and slot["rows"] != 0:
                old = _render_accum(
                    keys, slot["key_values"], slot["window"],
                    aggregates, slot["accum"])
            if slot is None:
                if weight < 0:
                    raise RuntimeError(
                        "window retract references a missing window")
                slot = dict(
                    rows=0, key_values=key_values, window=bound,
                    accum=_initial_accum(aggregates))
                groups[group_key] = slot
            _apply_accum(slot["accum"], row, weight, aggregates)
            slot["rows"] = int(slot["rows"]) + weight
            if slot["rows"] < 0:
                raise RuntimeError("window row count became negative")
            if slot["rows"] == 0:
                groups.pop(group_key, None)
                new = None
            else:
                new = _render_accum(
                    keys, slot["key_values"], slot["window"],
                    aggregates, slot["accum"])
            if old is not None and old != new:
                emitted.append((old, -1))
            elif old is not None and new is None:
                emitted.append((old, -1))
            if new is not None and old != new:
                emitted.append((new, 1))
            if old is not None and new is None:
                pass
    if ttl is not None and watermark is not None:
        horizon = watermark - int(ttl)
        for group_key in list(groups):
            slot = groups[group_key]
            if int(slot["window"][1]) <= horizon and slot["rows"] > 0:
                emitted.append((
                    _render_accum(
                        keys, slot["key_values"], slot["window"],
                        aggregates, slot["accum"]),
                    -1))
                groups.pop(group_key, None)
    return stream_zset.consolidate(emitted)


def _eval_sessions(state, incoming, node, late, watermark, ttl, emitted):
    keys = list(node["keys"])
    gap = int(node["gap"])
    stored = state.setdefault("rows", {})
    previous = state.setdefault("outputs", {})
    touched = set()
    for row, weight in incoming:
        if node["late_policy"] == "drop" and late(row):
            continue
        identity, key_values = _group_identity(row, keys)
        bucket = stored.setdefault(identity, dict(values=key_values, rows={}))
        row_key = stream_zset.row_key(row)
        current = bucket["rows"].get(row_key, (row, 0))[1] + weight
        if current < 0:
            raise RuntimeError("session weight became negative")
        if current == 0:
            bucket["rows"].pop(row_key, None)
        else:
            bucket["rows"][row_key] = (dict(row), current)
        touched.add(identity)
    for identity in touched:
        bucket = stored.get(identity)
        old = previous.get(identity, stream_zset.ZSet())
        new = stream_zset.ZSet()
        if bucket is not None:
            members = [
                (dict(payload[0]), payload[1])
                for payload in bucket["rows"].values()
            ]
            for start, end, group in _session_bounds(
                members, gap, node["time"]):
                count = 0
                for _row, item_weight in group:
                    count += item_weight
                rendered = {}
                for name, value in zip(keys, bucket["values"]):
                    rendered[name] = value
                rendered["w_start"] = int(start)
                rendered["w_end"] = int(end)
                rendered["n"] = int(count)
                new.add(rendered, 1)
            if not bucket["rows"]:
                stored.pop(identity, None)
        if ttl is not None and watermark is not None:
            horizon = watermark - int(ttl)
            kept = stream_zset.ZSet()
            for row, weight in new.items():
                if int(row["w_end"]) > horizon:
                    kept.add(row, weight)
            new = kept
        previous[identity] = new
        for row, weight in _diff(old, new):
            emitted.append((row, weight))
    return stream_zset.consolidate(emitted)


def _diff(previous, current):
    pairs = []
    for key in set(previous.weights) | set(current.weights):
        delta = current.weights.get(key, 0) - previous.weights.get(key, 0)
        if not delta:
            continue
        row = current.rows.get(key)
        if row is None:
            row = previous.rows[key]
        pairs.append((dict(row), delta))
    return pairs


def eval_watermark(state, delta, node):
    lateness = int(node["lateness"])
    policy = node["policy"]
    time_name = node["time"]
    incoming = stream_zset.consolidate(delta)
    observed = state.get("max_ts")
    for row, weight in incoming:
        if weight > 0:
            ts = _positive_ts(row[time_name], "event time")
            if observed is None or ts > observed:
                observed = ts
    state["max_ts"] = observed
    watermark = None if observed is None else observed - lateness
    state["watermark"] = watermark
    emitted = []
    for row, weight in incoming:
        ts = _positive_ts(row[time_name], "event time")
        is_late = watermark is not None and ts < watermark
        if policy == "drop" and is_late:
            continue
        out = dict(row)
        if policy == "side":
            out["_late"] = 1 if is_late else 0
        emitted.append((out, weight))
    return stream_zset.consolidate(emitted)


def _register_rows(con, name, rows, columns):
    payload = []
    for row in rows:
        payload.append({column: row.get(column) for column in columns})
    if payload:
        table = pa.Table.from_pylist(payload)
    else:
        fields = [pa.field(column, pa.null()) for column in columns]
        table = pa.schema(fields).empty_table()
    con.register(name, table)


def _expand(collection, columns):
    rows = []
    for row, weight in collection.items():
        if weight < 0:
            raise RuntimeError("partition weight became negative")
        if weight > 100000:
            raise RuntimeError("partition weight is too large")
        for _count in range(weight):
            rows.append({column: row.get(column) for column in columns})
    return rows


def _query_zset(con, sql):
    cursor = con.execute(sql)
    names = [item[0] for item in cursor.description]
    result = stream_zset.ZSet()
    for record in cursor.fetchall():
        result.add(dict(zip(names, record)), 1)
    return result


def eval_over(state, delta, node, con):
    columns = list(node["columns"])
    partition = list(node["partition"])
    parts = state.setdefault("parts", {})
    outputs = state.setdefault("outputs", {})
    touched = set()
    for row, weight in stream_zset.consolidate(delta):
        identity, _values = _group_identity(row, partition)
        bucket = parts.setdefault(identity, stream_zset.ZSet())
        bucket.add(row, weight)
        if bucket.weights and min(bucket.weights.values()) < 0:
            raise RuntimeError("window partition weight became negative")
        touched.add(identity)
    emitted = []
    for identity in touched:
        bucket = parts.get(identity, stream_zset.ZSet())
        rows = _expand(bucket, columns)
        old = outputs.get(identity, stream_zset.ZSet())
        if rows:
            _register_rows(con, "_input", rows, columns)
            try:
                new = _query_zset(con, node["sql"])
            finally:
                con.unregister("_input")
        else:
            new = stream_zset.ZSet()
            parts.pop(identity, None)
        outputs[identity] = new
        emitted.extend(_diff(old, new))
    return stream_zset.consolidate(emitted)


def _order_key(row, specs):
    keys = []
    for spec in specs:
        value = row[spec["column"]]
        if value is None:
            keys.append((0 if spec["nulls_first"] else 1, 0))
        else:
            identity = stream_zset.tag(value)
            keys.append((1 if spec["nulls_first"] else 0, identity))
    return tuple(keys)


def eval_topn(state, delta, node):
    partition = list(node["partition"])
    parts = state.setdefault("parts", {})
    outputs = state.setdefault("outputs", {})
    touched = set()
    for row, weight in stream_zset.consolidate(delta):
        identity, _values = _group_identity(row, partition)
        bucket = parts.setdefault(identity, stream_zset.ZSet())
        bucket.add(row, weight)
        if bucket.weights and min(bucket.weights.values()) < 0:
            raise RuntimeError("top-n weight became negative")
        touched.add(identity)
    emitted = []
    limit = int(node["limit"])
    order = list(node["order"])
    for identity in touched:
        bucket = parts.get(identity, stream_zset.ZSet())
        rows = _expand(bucket, node["columns"])
        rows.sort(key=lambda item: _order_key(item, order))
        # desc columns are reversed by sorting ascending on a flipped tag
        rows.sort(key=lambda item: _topn_key(item, order))
        new = stream_zset.ZSet()
        for item in rows[:limit]:
            new.add(item, 1)
        old = outputs.get(identity, stream_zset.ZSet())
        outputs[identity] = new
        if not bucket.weights:
            parts.pop(identity, None)
        emitted.extend(_diff(old, new))
    return stream_zset.consolidate(emitted)


def _topn_key(row, specs):
    keys = []
    for spec in specs:
        value = row.get(spec["column"])
        identity = stream_zset.tag(value)
        if spec["desc"]:
            keys.append((0, _invert_tag(identity)))
        else:
            keys.append((0, identity))
    keys.append((1, stream_zset.row_key(row)))
    return tuple(keys)


def _invert_tag(identity):
    kind = identity[0]
    if kind == "null":
        return identity
    if kind in {"int", "decimal"}:
        return (kind, -identity[1])
    if kind == "float":
        return (kind, tuple(-item for item in identity[1:]))
    if kind == "str":
        return (kind, tuple(-ord(char) for char in identity[1]))
    return ("inv", identity)


def _interval_hit(left, right, node):
    if node["mode"] == "contains_point":
        point = left[node["point"]]
        return (
            point is not None
            and right[node["right_start"]] is not None
            and right[node["right_end"]] is not None
            and right[node["right_start"]] <= point < right[node["right_end"]]
        )
    return (
        left[node["left_start"]] < right[node["right_end"]]
        and right[node["right_start"]] < left[node["left_end"]]
    )


def _project_interval(node, left, right):
    projected = {}
    for item in node["projections"]:
        source = left if item["source"] == "left" else right
        projected[item["output"]] = source[item["column"]]
    return projected


def eval_interval(state, left_delta, right_delta, node):
    left_rows = state.setdefault("left", {})
    right_rows = state.setdefault("right", {})
    outputs = state.setdefault("outputs", {})
    touched = set()

    def apply_side(store, delta, columns):
        for row, weight in stream_zset.consolidate(delta):
            identity, _values = _group_identity(row, columns)
            bucket = store.setdefault(identity, stream_zset.ZSet())
            bucket.add(row, weight)
            if bucket.weights and min(bucket.weights.values()) < 0:
                raise RuntimeError("interval join weight became negative")
            touched.add(identity)

    apply_side(left_rows, left_delta, [item["left"] for item in node["keys"]])
    apply_side(right_rows, right_delta, [item["right"] for item in node["keys"]])
    emitted = []
    for identity in touched:
        left_bucket = left_rows.get(identity, stream_zset.ZSet())
        right_bucket = right_rows.get(identity, stream_zset.ZSet())
        new = stream_zset.ZSet()
        for left, left_weight in left_bucket.items():
            for right, right_weight in right_bucket.items():
                if _interval_hit(left, right, node):
                    new.add(
                        _project_interval(node, left, right),
                        left_weight * right_weight)
        old = outputs.get(identity, stream_zset.ZSet())
        outputs[identity] = new
        emitted.extend(_diff(old, new))
    return stream_zset.consolidate(emitted)


def eval_recursive(state, current, node, con):
    for source_id, table_name in zip(node["sources"], node["tables"]):
        collection = state.setdefault("inputs", {}).setdefault(
            source_id, stream_zset.ZSet())
        collection.add_all(current[source_id])
        if collection.weights and min(collection.weights.values()) < 0:
            raise RuntimeError("recursive input weight became negative")
        rows = _expand(collection, node["input_columns"][source_id])
        _register_rows(
            con, table_name, rows, node["input_columns"][source_id])
    try:
        seed = _query_zset(con, node["seed_sql"])
        accum = seed.copy()
        delta = seed.copy()
        for _step in range(int(node["max_iterations"])):
            if not delta.weights:
                break
            _register_rows(con, "_delta", _expand(delta, node["columns"]), node["columns"])
            try:
                step = _query_zset(con, node["step_sql"])
            finally:
                con.unregister("_delta")
            fresh = stream_zset.ZSet()
            if node["set_mode"] == "union":
                for row, weight in step.items():
                    if weight > 0 and stream_zset.row_key(row) not in accum.weights:
                        fresh.add(row, 1)
            else:
                fresh = step
            if not fresh.weights:
                delta = fresh
                break
            accum.add_all(fresh.items())
            delta = fresh
        else:
            if delta.weights:
                raise RuntimeError(
                    "recursive CTE exceeded max_iterations")
        produced = accum
    finally:
        for table_name in node["tables"]:
            con.unregister(table_name)
    previous = state.setdefault("output", stream_zset.ZSet())
    emitted = _diff(previous, produced)
    state["output"] = produced
    return stream_zset.consolidate(emitted)
