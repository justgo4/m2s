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
import decimal
import math

import duckdb
import pyarrow as pa

import stream_ir
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
        if item["function"] == "count":
            result[item["output"]] = dict(count=0)
        else:
            result[item["output"]] = dict(nonnull=0, sum=None)
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
            if item["function"] == "count":
                if int(state["count"]) != 0:
                    raise RuntimeError(
                        "empty aggregate group retains COUNT state")
            elif int(state["nonnull"]) != 0 or state["sum"] is not None:
                raise RuntimeError(
                    "empty aggregate group retains SUM/AVG state")
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
        else:
            row[output] = (
                None if int(state["nonnull"]) == 0
                else float(state["sum"] / int(state["nonnull"])))
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
        output = self.output.copy()
        current = {}
        for node_id in self.order:
            node = self.nodes[node_id]
            current[node_id] = self._eval(
                node, current, deltas, agg, joins)
        produced = current[self.plan["output"]]
        output.add_all(produced)
        self.agg_state = agg
        self.join_state = joins
        self.output = output
        return produced

    def _eval(self, node, current, deltas, agg, joins):
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
        if op == "equi_join":
            state = joins.setdefault(
                node["id"],
                dict(left=_new_index(), right=_new_index()))
            return _eval_join(
                state, current[node["left"]], current[node["right"]],
                node)
        raise ValueError("unsupported stream operator: " + str(op))
