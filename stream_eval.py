#!/usr/bin/env python3
"""Incremental evaluator for a stream DAG.

Filter and project are linear: each output weight is computed from the input
delta only. COUNT/SUM/AVG keep one accumulator per group and emit +1/-1 when
the rendered group appears, changes, or disappears. Equi-join is bilinear:
an input weight multiplies the matching weights on the other side, and a NULL
join key matches nothing. One push applies every left delta before every
right delta, so a new left row meets a new right row in the same batch.

A failed push leaves the graph as it was before that batch.
"""
import contextlib
import copy
import datetime
import decimal
import functools
import math

import duckdb
import pyarrow as pa

import stream_ir
import stream_time
import stream_zset


def _register(con, pairs):
    payload = []
    for row, value in pairs:
        item = dict(row)
        if "_m2s_z_weight" in item:
            raise ValueError("row uses reserved weight column")
        item["_m2s_z_weight"] = stream_zset.weight(value)
        payload.append(item)
    table = pa.Table.from_pylist(payload)
    with contextlib.suppress(Exception):
        con.unregister("_m2s_z")
    con.register("_m2s_z", table)


def _rows_from_cursor(cursor):
    names = [item[0] for item in cursor.description]
    if "_m2s_z_weight" not in names:
        raise RuntimeError("stream expression dropped the weight column")
    weight_at = names.index("_m2s_z_weight")
    body = [name for name in names if name != "_m2s_z_weight"]
    result = []
    for record in cursor.fetchall():
        value = record[weight_at]
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("expression weight must be an int")
        if value == 0:
            continue
        row = {}
        for index, name in enumerate(names):
            if index == weight_at:
                continue
            row[name] = record[index]
        if not body:
            raise RuntimeError("stream expression produced no columns")
        result.append((row, int(value)))
    return stream_zset.consolidate(result)


def _eval_filter(con, delta, predicate):
    rows = stream_zset.consolidate(delta)
    if not rows:
        return []
    _register(con, rows)
    try:
        cursor = con.execute(
            "SELECT * EXCLUDE (_m2s_z_weight), _m2s_z_weight "
            "FROM _m2s_z WHERE (" + predicate + ")")
        return _rows_from_cursor(cursor)
    finally:
        with contextlib.suppress(Exception):
            con.unregister("_m2s_z")


def _eval_project(con, delta, expressions):
    rows = stream_zset.consolidate(delta)
    if not rows:
        return []
    _register(con, rows)
    try:
        select_list = ", ".join(
            item["expression"] for item in expressions)
        cursor = con.execute(
            "SELECT " + select_list
            + ", SUM(_m2s_z_weight) AS _m2s_z_weight FROM _m2s_z "
            "GROUP BY ALL HAVING SUM(_m2s_z_weight) <> 0")
        return _rows_from_cursor(cursor)
    finally:
        with contextlib.suppress(Exception):
            con.unregister("_m2s_z")


def _initial(aggregates):
    result = {}
    for item in aggregates:
        function = item["function"]
        if function == "count":
            result[item["output"]] = dict(count=0)
        elif function in {"sum", "avg"}:
            result[item["output"]] = dict(nonnull=0, sum=None)
        else:
            result[item["output"]] = dict(values={})
    return result


def _numeric(value, output):
    if isinstance(value, bool) or not isinstance(
        value, (int, float, decimal.Decimal)
    ):
        raise TypeError(
            "aggregate " + output + " requires int/float/Decimal input")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite aggregate values are unsupported")
    return value


def _accumulate(slot, row, value, aggregates):
    value = stream_zset.weight(value)
    if value == 0:
        return
    next_rows = slot["rows"] + value
    if next_rows < 0:
        raise RuntimeError("aggregate group row count became negative")
    for item in aggregates:
        output = item["output"]
        function = item["function"]
        input_name = item["input"]
        state = slot["accum"][output]
        if input_name == "*":
            raw = None
        else:
            if input_name not in row:
                raise ValueError(
                    "aggregate input lacks column " + input_name)
            raw = row[input_name]
        if function == "count":
            contributes = input_name == "*" or raw is not None
            if contributes:
                state["count"] = int(state["count"]) + value
                if state["count"] < 0:
                    raise RuntimeError(
                        "aggregate count became negative for " + output)
            if state["count"] > next_rows:
                raise RuntimeError(
                    "aggregate count exceeds group cardinality for "
                    + output)
            if input_name == "*" and state["count"] != next_rows:
                raise RuntimeError(
                    "count(*) diverged from group cardinality")
            continue
        if function in {"min", "max", "count_distinct"}:
            if raw is not None:
                _bump_value(state["values"], stream_zset.tag(raw), raw, value)
            continue
        if function == "string_agg":
            if raw is not None:
                try:
                    order_values = tuple(
                        row[spec["column"]] for spec in item["order_by"])
                except KeyError as exc:
                    raise ValueError(
                        "string_agg input lacks order column "
                        + str(exc.args[0])) from None
                identity = tuple(
                    stream_zset.tag(part)
                    for part in order_values + (raw,))
                _bump_value(
                    state["values"], identity, (order_values, raw), value)
            continue
        if raw is None:
            continue
        number = _numeric(raw, output)
        previous = int(state["nonnull"])
        next_nonnull = previous + value
        if next_nonnull < 0 or next_nonnull > next_rows:
            raise RuntimeError(
                "aggregate non-NULL count violates group cardinality")
        total = state["sum"]
        if previous == 0:
            total = number * value
        else:
            total = total + (number * value)
        state["nonnull"] = next_nonnull
        state["sum"] = None if next_nonnull == 0 else total
    if next_rows == 0:
        for item in aggregates:
            state = slot["accum"][item["output"]]
            function = item["function"]
            if function == "count":
                if int(state["count"]) != 0:
                    raise RuntimeError(
                        "empty aggregate group retains COUNT state")
            elif function in {"sum", "avg"}:
                if int(state["nonnull"]) != 0 or state["sum"] is not None:
                    raise RuntimeError(
                        "empty aggregate group retains SUM/AVG state")
            elif state["values"]:
                raise RuntimeError(
                    "empty aggregate group retains " + function + " state")
    slot["rows"] = next_rows


def _render(group_keys, key, aggregates, slot):
    row = {}
    for name, value in zip(group_keys, key):
        row[name] = value
    for item in aggregates:
        output = item["output"]
        function = item["function"]
        state = slot["accum"][output]
        if function == "count":
            row[output] = int(state["count"])
        elif function == "sum":
            row[output] = state["sum"]
        elif function == "avg":
            row[output] = (
                None if int(state["nonnull"]) == 0
                else float(state["sum"] / int(state["nonnull"])))
        elif function == "count_distinct":
            row[output] = len(state["values"])
        elif function == "min":
            row[output] = _pick_extreme(state["values"], False)
        elif function == "max":
            row[output] = _pick_extreme(state["values"], True)
        elif function == "string_agg":
            row[output] = _render_string_agg(state["values"], item)
        else:
            raise ValueError("unsupported aggregate function: " + function)
    return row


def _eval_aggregate(groups, delta, node):
    emitted = []
    keys = node["group_keys"]
    aggregates = node["aggregates"]
    for row, value in stream_zset.consolidate(delta):
        value = stream_zset.weight(value)
        if value == 0:
            continue
        try:
            values = tuple(row[name] for name in keys)
        except KeyError as exc:
            raise ValueError(
                "aggregate input lacks group key " + str(exc.args[0])
            ) from None
        # Tags keep bool distinct from int. The rendered row keeps the
        # first value stored for this identity.
        key = tuple(stream_zset.tag(item) for item in values)
        slot = groups.get(key)
        old = None
        if slot is not None and slot["rows"] != 0:
            old = _render(keys, slot["values"], aggregates, slot)
        if slot is None:
            if value < 0:
                raise RuntimeError(
                    "aggregate retract references a missing group")
            slot = dict(
                rows=0, accum=_initial(aggregates), values=values)
            groups[key] = slot
        _accumulate(slot, row, value, aggregates)
        if slot["rows"] == 0:
            groups.pop(key, None)
            new = None
        else:
            new = _render(keys, slot["values"], aggregates, slot)
        if old == new:
            continue
        if old is not None:
            emitted.append((old, -1))
        if new is not None:
            emitted.append((new, 1))
    return stream_zset.consolidate(emitted)


def _new_index():
    return dict(weights={}, rows={}, by_join={})


def _join_values(row, columns):
    try:
        values = tuple(row[name] for name in columns)
    except KeyError as exc:
        raise ValueError(
            "equi-join input lacks column " + str(exc.args[0])) from None
    for item in values:
        stream_zset.tag(item)
    return values


def _index_add(index, row, value, columns):
    value = stream_zset.weight(value)
    if value == 0:
        return
    key = stream_zset.row_key(row)
    join_values = _join_values(row, columns)
    indexable = all(item is not None for item in join_values)
    join_key = tuple(stream_zset.tag(item) for item in join_values)
    previous = index["weights"].get(key, 0)
    nxt = previous + value
    if nxt < 0:
        raise RuntimeError("z-set weight became negative")
    if nxt == 0:
        index["weights"].pop(key, None)
        index["rows"].pop(key, None)
        if indexable:
            bucket = index["by_join"].get(join_key)
            if bucket is not None:
                bucket.pop(key, None)
                if not bucket:
                    index["by_join"].pop(join_key, None)
        return
    index["weights"][key] = nxt
    index["rows"][key] = dict(row)
    if indexable:
        index["by_join"].setdefault(join_key, {})[key] = nxt


def _project_join(node, left_row, right_row):
    projected = {}
    for item in node["projections"]:
        source = left_row if item["source"] == "left" else right_row
        try:
            projected[item["output"]] = source[item["column"]]
        except KeyError as exc:
            raise ValueError(
                "equi-join projection lacks column " + str(exc.args[0])
            ) from None
    return projected


def _emit_matches(row, value, other, columns, node, out, side):
    value = stream_zset.weight(value)
    if value == 0:
        return
    join_values = _join_values(row, columns)
    if any(item is None for item in join_values):
        return
    join_key = tuple(stream_zset.tag(item) for item in join_values)
    bucket = other["by_join"].get(join_key)
    if not bucket:
        return
    for other_key, other_weight in bucket.items():
        other_row = other["rows"][other_key]
        if side == "left":
            projected = _project_join(node, row, other_row)
        else:
            projected = _project_join(node, other_row, row)
        produced = value * other_weight
        if produced != 0:
            out.append((projected, produced))


def _eval_join(state, left_delta, right_delta, node):
    left_columns = [item["left"] for item in node["join_pairs"]]
    right_columns = [item["right"] for item in node["join_pairs"]]
    out = []
    for row, value in stream_zset.consolidate(left_delta):
        _emit_matches(
            row, value, state["right"], left_columns, node, out, "left")
        _index_add(state["left"], row, value, left_columns)
    for row, value in stream_zset.consolidate(right_delta):
        _emit_matches(
            row, value, state["left"], right_columns, node, out, "right")
        _index_add(state["right"], row, value, right_columns)
    return stream_zset.consolidate(out)


def _bump_value(values, identity, raw, weight):
    current = values.get(identity)
    count = 0 if current is None else int(current[1])
    count += weight
    if count < 0:
        raise RuntimeError("aggregate value count became negative")
    if count == 0:
        values.pop(identity, None)
    else:
        values[identity] = [raw, count]


def _cmp_key(value):
    if isinstance(value, bool):
        raise TypeError("boolean aggregate order is unsupported")
    if isinstance(value, (int, decimal.Decimal)):
        return (0, decimal.Decimal(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite aggregate values are unsupported")
        return (1, value)
    if isinstance(value, str):
        return (2, value)
    if isinstance(value, datetime.datetime):
        return (3, value.isoformat())
    if isinstance(value, datetime.date):
        return (4, value.isoformat())
    if isinstance(value, datetime.time):
        return (5, value.isoformat())
    if isinstance(value, (bytes, bytearray, memoryview)):
        return (6, bytes(value))
    raise TypeError(
        "unsupported aggregate order type: " + type(value).__name__)


def _pick_extreme(values, want_max):
    chosen = None
    chosen_key = None
    family = None
    for raw, _count in values.values():
        key = _cmp_key(raw)
        if family is None:
            family = key[0]
        elif key[0] != family:
            raise RuntimeError("MIN/MAX cannot order mixed value types")
        if (
            chosen is None
            or (want_max and key > chosen_key)
            or (not want_max and key < chosen_key)
        ):
            chosen = raw
            chosen_key = key
    return chosen


def _render_string_agg(values, item):
    rows = []
    for raw, count in values.values():
        order_values, text = raw
        if text is None:
            continue
        rows.append((order_values, str(text), int(count)))
    specs = item["order_by"]

    def compare(left, right):
        for index, spec in enumerate(specs):
            outcome = _cmp_ordered(
                left[0][index], right[0][index], spec)
            if outcome:
                return outcome
        return 0

    rows.sort(key=functools.cmp_to_key(compare))
    parts = []
    for _order, text, count in rows:
        parts.extend([text] * count)
    if not parts:
        return None
    return item["separator"].join(parts)


def _cmp_ordered(left, right, spec):
    if left is None and right is None:
        return 0
    if left is None:
        return -1 if spec["nulls_first"] else 1
    if right is None:
        return 1 if spec["nulls_first"] else -1
    left_key = _cmp_key(left)
    right_key = _cmp_key(right)
    if left_key[0] != right_key[0]:
        raise RuntimeError("STRING_AGG cannot order mixed value types")
    if left_key < right_key:
        outcome = -1
    elif left_key > right_key:
        outcome = 1
    else:
        outcome = 0
    if spec["desc"]:
        outcome = -outcome
    return outcome


def _reject_negative(collection):
    for weight in collection.weights.values():
        if weight < 0:
            raise RuntimeError("z-set weight became negative")


def _zset_diff(previous, current):
    pairs = []
    for key in set(previous.weights) | set(current.weights):
        delta = current.weights.get(key, 0) - previous.weights.get(key, 0)
        if delta:
            row = current.rows.get(key)
            if row is None:
                row = previous.rows[key]
            pairs.append((dict(row), delta))
    return stream_zset.consolidate(pairs)


def _register_weighted(con, name, collection, columns):
    payload = []
    for row, weight in collection.items():
        item = {}
        for column in columns:
            item[column] = row.get(column)
        item["_m2s_z_weight"] = int(weight)
        payload.append(item)
    if payload:
        table = pa.Table.from_pylist(payload)
    else:
        fields = [pa.field(column, pa.null()) for column in columns]
        fields.append(pa.field("_m2s_z_weight", pa.int64()))
        table = pa.schema(fields).empty_table()
    with contextlib.suppress(Exception):
        con.unregister(name)
    con.register(name, table)


def _eval_distinct(state, delta):
    emitted = []
    for row, weight in stream_zset.consolidate(delta):
        key = stream_zset.row_key(row)
        previous = state["weights"].get(key, 0)
        nxt = previous + weight
        if nxt < 0:
            raise RuntimeError("distinct weight became negative")
        if previous <= 0 < nxt:
            emitted.append((dict(row), 1))
        elif previous > 0 >= nxt:
            emitted.append((dict(row), -1))
        if nxt == 0:
            state["weights"].pop(key, None)
            state["rows"].pop(key, None)
        else:
            state["weights"][key] = nxt
            state["rows"][key] = dict(row)
    return stream_zset.consolidate(emitted)


def _set_weight(mode, left, right):
    if mode == "union_all":
        return left + right
    if mode == "union":
        return 1 if left + right > 0 else 0
    if mode == "intersect_all":
        if left > 0 and right > 0:
            return min(left, right)
        return 0
    if mode == "intersect":
        return 1 if left > 0 and right > 0 else 0
    if mode == "except_all":
        return max(0, left - right)
    if mode == "except":
        return 1 if left > 0 and right <= 0 else 0
    raise ValueError("unsupported set operator mode: " + mode)


def _apply_side(collection, delta):
    touched = {}
    for row, weight in stream_zset.consolidate(delta):
        key = stream_zset.row_key(row)
        previous = collection.weights.get(key, 0)
        stored = collection.rows.get(key, row)
        if key not in touched:
            touched[key] = (previous, dict(stored))
        collection.add(row, weight)
        if collection.weights.get(key, 0) < 0:
            raise RuntimeError("set operator weight became negative")
    return touched


def _eval_set(state, left_delta, right_delta, mode):
    before = _apply_side(state["left"], left_delta)
    right_before = _apply_side(state["right"], right_delta)
    emitted = []
    for key in set(before) | set(right_before):
        if key in before:
            old_left, row = before[key]
        else:
            old_left = state["left"].weights.get(key, 0)
            row = None
        if key in right_before:
            old_right, right_row = right_before[key]
            if row is None:
                row = right_row
        else:
            old_right = state["right"].weights.get(key, 0)
        new_left = state["left"].weights.get(key, 0)
        new_right = state["right"].weights.get(key, 0)
        old_weight = _set_weight(mode, old_left, old_right)
        new_weight = _set_weight(mode, new_left, new_right)
        if old_weight != new_weight:
            if row is None:
                row = (
                    state["left"].rows.get(key)
                    or state["right"].rows.get(key))
            emitted.append((dict(row), new_weight - old_weight))
    return stream_zset.consolidate(emitted)


def _project_outer(node, left_row, right_row):
    projected = {}
    for item in node["projections"]:
        source = left_row if item["source"] == "left" else right_row
        if source is None:
            projected[item["output"]] = None
        else:
            try:
                projected[item["output"]] = source[item["column"]]
            except KeyError as exc:
                raise ValueError(
                    "keyed join projection lacks column "
                    + str(exc.args[0])) from None
    return projected


def _bucket_weight(index, join_key):
    if join_key is None:
        return 0
    bucket = index["by_join"].get(join_key)
    if not bucket:
        return 0
    return sum(bucket.values())


def _join_identity(row, columns):
    values = _join_values(row, columns)
    if any(item is None for item in values):
        return None
    return tuple(stream_zset.tag(item) for item in values)


def _apply_keyed_left(state, row, weight, node, out):
    kind = node["kind"]
    columns = [item["left"] for item in node["join_pairs"]]
    join_key = _join_identity(row, columns)
    right_match = _bucket_weight(state["right"], join_key)
    left_before = _bucket_weight(state["left"], join_key)
    if right_match and kind in {"left", "right", "full"}:
        bucket = state["right"]["by_join"][join_key]
        for other_key, other_weight in bucket.items():
            out.append((
                _project_outer(
                    node, row, state["right"]["rows"][other_key]),
                weight * other_weight))
    elif right_match and kind == "semi":
        out.append((_project_outer(node, row, None), weight))
    elif not right_match and kind in {"left", "full", "anti"}:
        out.append((_project_outer(node, row, None), weight))
    if (
        join_key is not None and right_match
        and kind in {"right", "full"}
    ):
        after = left_before + weight
        if (left_before == 0 and after > 0) or (left_before > 0 and after == 0):
            sign = -1 if after > 0 else 1
            bucket = state["right"]["by_join"][join_key]
            for other_key, other_weight in bucket.items():
                out.append((
                    _project_outer(
                        node, None, state["right"]["rows"][other_key]),
                    sign * other_weight))
    _index_add(state["left"], row, weight, columns)


def _apply_keyed_right(state, row, weight, node, out):
    kind = node["kind"]
    columns = [item["right"] for item in node["join_pairs"]]
    join_key = _join_identity(row, columns)
    left_match = _bucket_weight(state["left"], join_key)
    right_before = _bucket_weight(state["right"], join_key)
    if left_match and kind in {"left", "right", "full"}:
        bucket = state["left"]["by_join"][join_key]
        for other_key, other_weight in bucket.items():
            out.append((
                _project_outer(
                    node, state["left"]["rows"][other_key], row),
                other_weight * weight))
    if join_key is not None and left_match and kind in {
        "left", "full", "semi", "anti",
    }:
        after = right_before + weight
        if (right_before == 0 and after > 0) or (
            right_before > 0 and after == 0
        ):
            bucket = state["left"]["by_join"][join_key]
            for other_key, other_weight in bucket.items():
                left_row = state["left"]["rows"][other_key]
                if kind in {"left", "full"}:
                    sign = -1 if after > 0 else 1
                    out.append((
                        _project_outer(node, left_row, None),
                        sign * other_weight))
                if kind == "semi":
                    sign = 1 if after > 0 else -1
                    out.append((
                        _project_outer(node, left_row, None),
                        sign * other_weight))
                if kind == "anti":
                    sign = -1 if after > 0 else 1
                    out.append((
                        _project_outer(node, left_row, None),
                        sign * other_weight))
    if not left_match and kind in {"right", "full"}:
        out.append((_project_outer(node, None, row), weight))
    _index_add(state["right"], row, weight, columns)


def _eval_keyed_join(state, left_delta, right_delta, node):
    out = []
    for row, weight in stream_zset.consolidate(left_delta):
        _apply_keyed_left(state, row, weight, node, out)
    for row, weight in stream_zset.consolidate(right_delta):
        _apply_keyed_right(state, row, weight, node, out)
    return stream_zset.consolidate(out)


def _eval_theta(state, left_delta, right_delta, node, con):
    state["left"].add_all(left_delta)
    state["right"].add_all(right_delta)
    _reject_negative(state["left"])
    _reject_negative(state["right"])
    _register_weighted(
        con, "_left", state["left"], node["left_columns"])
    _register_weighted(
        con, "_right", state["right"], node["right_columns"])
    try:
        cursor = con.execute(node["sql"])
        produced = stream_zset.ZSet()
        produced.add_all(_rows_from_cursor(cursor))
    finally:
        with contextlib.suppress(Exception):
            con.unregister("_left")
            con.unregister("_right")
    delta = _zset_diff(state["output"], produced)
    state["output"] = produced
    return delta


def _bag_rows(collection):
    rows = []
    for row, weight in collection.items():
        if weight < 0:
            raise RuntimeError("recompute input weight became negative")
        if weight > 100000:
            raise RuntimeError("recompute weight is too large")
        for _count in range(weight):
            rows.append(dict(row))
    return rows


def _eval_recompute(state, current, node, con):
    for source_id, table_name in zip(node["sources"], node["tables"]):
        collection = state["inputs"].setdefault(source_id, stream_zset.ZSet())
        collection.add_all(current[source_id])
        _reject_negative(collection)
        rows = _bag_rows(collection)
        columns = node["input_columns"][source_id]
        payload = []
        for row in rows:
            payload.append({column: row.get(column) for column in columns})
        if payload:
            table = pa.Table.from_pylist(payload)
        else:
            fields = [pa.field(column, pa.null()) for column in columns]
            table = pa.schema(fields).empty_table()
        with contextlib.suppress(Exception):
            con.unregister(table_name)
        con.register(table_name, table)
    try:
        cursor = con.execute(node["sql"])
        names = [item[0] for item in cursor.description]
        produced = stream_zset.ZSet()
        for record in cursor.fetchall():
            produced.add(dict(zip(names, record)), 1)
    finally:
        for table_name in node["tables"]:
            with contextlib.suppress(Exception):
                con.unregister(table_name)
    delta = _zset_diff(state["output"], produced)
    state["output"] = produced
    return delta


class StreamGraph:
    def __init__(self, plan):
        self.plan = stream_ir.validate_plan(plan)
        self.nodes = {
            node["id"]: node for node in self.plan["nodes"]
        }
        self.order = [node["id"] for node in self.plan["nodes"]]
        self.con = duckdb.connect(":memory:")
        self.agg_state = {}
        self.join_state = {}
        self.extra = {}
        self.output = stream_zset.ZSet()

    def close(self):
        self.con.close()

    def push(self, deltas):
        if not isinstance(deltas, dict):
            raise TypeError("stream deltas must be a dict of input names")
        unknown = sorted(set(deltas) - set(self.plan["inputs"]))
        if unknown:
            raise ValueError(
                "unknown stream input: " + ",".join(unknown))
        agg = copy.deepcopy(self.agg_state)
        joins = copy.deepcopy(self.join_state)
        extra = copy.deepcopy(self.extra)
        output = self.output.copy()
        current = {}
        for node_id in self.order:
            node = self.nodes[node_id]
            current[node_id] = self._eval(
                node, current, deltas, agg, joins, extra)
        produced = current[self.plan["output"]]
        output.add_all(produced)
        self.agg_state = agg
        self.join_state = joins
        self.extra = extra
        self.output = output
        return produced

    def _eval(self, node, current, deltas, agg, joins, extra):
        op = node["op"]
        if op == "input":
            return stream_zset.consolidate(deltas.get(node["input"], ()))
        if op == "filter":
            return _eval_filter(
                self.con, current[node["input"]], node["predicate"])
        if op == "project":
            return _eval_project(
                self.con, current[node["input"]], node["expressions"])
        if op == "aggregate":
            return _eval_aggregate(
                agg.setdefault(node["id"], {}),
                current[node["input"]], node)
        if op == "distinct":
            state = extra.setdefault(
                node["id"], dict(weights={}, rows={}))
            return _eval_distinct(state, current[node["input"]])
        if op == "equi_join":
            state = joins.setdefault(
                node["id"],
                dict(left=_new_index(), right=_new_index()))
            return _eval_join(
                state, current[node["left"]], current[node["right"]],
                node)
        if op == "keyed_join":
            state = joins.setdefault(
                node["id"],
                dict(left=_new_index(), right=_new_index()))
            return _eval_keyed_join(
                state, current[node["left"]], current[node["right"]],
                node)
        if op == "set_op":
            state = extra.setdefault(
                node["id"],
                dict(left=stream_zset.ZSet(), right=stream_zset.ZSet()))
            return _eval_set(
                state, current[node["left"]], current[node["right"]],
                node["mode"])
        if op == "theta_join":
            state = extra.setdefault(
                node["id"],
                dict(
                    left=stream_zset.ZSet(),
                    right=stream_zset.ZSet(),
                    output=stream_zset.ZSet()))
            return _eval_theta(
                state, current[node["left"]], current[node["right"]],
                node, self.con)
        if op == "recompute":
            state = extra.setdefault(
                node["id"],
                dict(inputs={}, output=stream_zset.ZSet()))
            return _eval_recompute(state, current, node, self.con)
        if op == "watermark":
            return stream_time.eval_watermark(
                extra.setdefault(node["id"], {}),
                current[node["input"]], node)
        if op == "window_agg":
            return stream_time.eval_window(
                extra.setdefault(node["id"], {}),
                current[node["input"]], node)
        if op == "over":
            return stream_time.eval_over(
                extra.setdefault(node["id"], {}),
                current[node["input"]], node, self.con)
        if op == "topn":
            return stream_time.eval_topn(
                extra.setdefault(node["id"], {}),
                current[node["input"]], node)
        if op == "interval_join":
            return stream_time.eval_interval(
                extra.setdefault(node["id"], {}),
                current[node["left"]], current[node["right"]], node)
        if op == "recursive_cte":
            return stream_time.eval_recursive(
                extra.setdefault(node["id"], {}),
                current, node, self.con)
        raise ValueError("unsupported stream operator: " + str(op))
