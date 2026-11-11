#!/usr/bin/env python3
"""Unified stream plan: a DAG of Z-set operators.

Existing projection/filter, COUNT/SUM/AVG and restricted INNER JOIN plans
lower into this DAG. Their own canonical bytes stay unchanged so durable
task identities do not move. The stream plan is a second, derived identity.
"""
import hashlib
import json

import aggregate_ir
import incremental_ir
import join_ir
import relational_ir


FORMAT_VERSION = 1
SEMANTICS = dict(
    model="zset",
    weight="integer",
    positive=1,
    negative=-1,
    zero_eliminated=True,
    nulls="sql",
    bag=True,
)
FILTER_SCOPES = {"source", "query", "predicate"}
AGGREGATE_FUNCTIONS = {
    "count", "sum", "avg", "min", "max", "count_distinct", "string_agg",
}
SET_MODES = {
    "union_all", "union", "intersect_all", "intersect",
    "except_all", "except",
}
JOIN_KINDS = {"left", "right", "full", "semi", "anti"}
THETA_KINDS = {"inner", "left", "right", "full", "semi", "anti"}


def canonical_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")


def semantic_id(plan):
    plan = validate_plan(plan)
    return hashlib.sha256(canonical_bytes(plan)).hexdigest()


def _text(value, name):
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(name + " must be a non-empty string")
    return value


def _names(values, name):
    result = []
    for item in values or ():
        text = _text(item, name)
        if text in result:
            raise ValueError(name + " repeats " + text)
        result.append(text)
    return result


def _node_inputs(node):
    op = node.get("op")
    if op == "input":
        return []
    if op in {"filter", "project", "aggregate", "distinct"}:
        return [_text(node.get("input"), op + " input")]
    if op in {"equi_join", "keyed_join", "theta_join", "set_op"}:
        left = _text(node.get("left"), op + " left")
        right = _text(node.get("right"), op + " right")
        if left == right:
            raise ValueError(op + " inputs must be distinct nodes")
        return [left, right]
    if op == "recompute":
        sources = node.get("sources")
        if not isinstance(sources, list) or not sources:
            raise ValueError("recompute sources cannot be empty")
        return [_text(item, "recompute source") for item in sources]
    raise ValueError("unsupported stream operator: " + str(op))


def _validate_filter(node):
    if set(node) != {"id", "op", "input", "predicate", "scope"}:
        raise ValueError("filter node fields differ from stream plan v1")
    predicate = node["predicate"]
    if not isinstance(predicate, str) or not predicate.strip():
        raise ValueError("filter predicate must be a non-empty string")
    if predicate != predicate.strip():
        raise ValueError("filter predicate is not canonical")
    if node["scope"] not in FILTER_SCOPES:
        raise ValueError("unsupported filter scope")


def _validate_project(node):
    if set(node) != {"id", "op", "input", "expressions"}:
        raise ValueError("project node fields differ from stream plan v1")
    expressions = node["expressions"]
    if not isinstance(expressions, list) or not expressions:
        raise ValueError("project expressions cannot be empty")
    seen = set()
    for item in expressions:
        if not isinstance(item, dict) or set(item) != {"output", "expression"}:
            raise ValueError("invalid project expression")
        output = _text(item["output"], "project output")
        expression = item["expression"]
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError("project expression must be a non-empty string")
        if expression != expression.strip():
            raise ValueError("project expression is not canonical")
        if output in seen or output == "_m2s_z_weight":
            raise ValueError("project output is reserved or duplicate")
        seen.add(output)


def _validate_aggregate(node):
    if set(node) != {"id", "op", "input", "group_keys", "aggregates"}:
        raise ValueError("aggregate node fields differ from stream plan v1")
    group_keys = _names(node["group_keys"], "group key")
    if list(node["group_keys"]) != group_keys:
        raise ValueError("aggregate group keys are not canonical")
    aggregates = node["aggregates"]
    if not isinstance(aggregates, list) or not aggregates:
        raise ValueError("aggregate outputs cannot be empty")
    seen = set(group_keys)
    for item in aggregates:
        _validate_aggregate_item(item, seen)
        seen.add(item["output"])


def _validate_aggregate_item(item, seen):
    if not isinstance(item, dict):
        raise ValueError("invalid aggregate output")
    function = _text(item.get("function"), "aggregate function")
    if function not in AGGREGATE_FUNCTIONS:
        raise ValueError("unsupported aggregate function: " + function)
    output = _text(item.get("output"), "aggregate output")
    source = _text(item.get("input"), "aggregate input")
    if output in seen or output == "_m2s_z_weight":
        raise ValueError("aggregate output is reserved or duplicate")
    if function == "string_agg":
        if set(item) != {
            "output", "function", "input", "separator", "order_by",
        }:
            raise ValueError("string_agg fields differ from stream plan v1")
        if not isinstance(item["separator"], str):
            raise ValueError("string_agg separator must be a string")
        order_by = item["order_by"]
        if not isinstance(order_by, list) or not order_by:
            raise ValueError(
                "string_agg requires ORDER BY; unordered aggregation is rejected")
        for spec in order_by:
            if (
                not isinstance(spec, dict)
                or set(spec) != {"column", "desc", "nulls_first"}
            ):
                raise ValueError("invalid string_agg order key")
            _text(spec["column"], "string_agg order column")
            if not isinstance(spec["desc"], bool) or not isinstance(
                spec["nulls_first"], bool
            ):
                raise ValueError("string_agg order flags must be booleans")
    else:
        if set(item) != {"output", "function", "input"}:
            raise ValueError("invalid aggregate output")
        if source == "*" and function != "count":
            raise ValueError(function + "(*) is unsupported")
    return output


def _validate_projection_list(projections, label):
    if not isinstance(projections, list) or not projections:
        raise ValueError(label + " projections cannot be empty")
    seen = set()
    for item in projections:
        if (
            not isinstance(item, dict)
            or set(item) != {"output", "source", "column"}
        ):
            raise ValueError("invalid " + label + " projection")
        output = _text(item["output"], label + " output")
        source = str(item["source"])
        _text(item["column"], label + " projection column")
        if source not in {"left", "right"}:
            raise ValueError(label + " projection source is invalid")
        if output in seen or output == "_m2s_z_weight":
            raise ValueError(label + " output is reserved or duplicate")
        seen.add(output)
    return projections


def _validate_join_pairs(pairs, label):
    if not isinstance(pairs, list) or not pairs:
        raise ValueError(label + " requires join pairs")
    seen_left = set()
    seen_right = set()
    normalized = []
    for item in pairs:
        if not isinstance(item, dict) or set(item) != {"left", "right"}:
            raise ValueError("invalid " + label + " pair")
        left = _text(item["left"], label + " left key")
        right = _text(item["right"], label + " right key")
        if left in seen_left or right in seen_right:
            raise ValueError(label + " key repeats a source column")
        seen_left.add(left)
        seen_right.add(right)
        normalized.append(dict(left=left, right=right))
    if pairs != normalized:
        raise ValueError(label + " pairs are not canonical")
    return pairs


def _validate_keyed_join(node):
    if set(node) != {
        "id", "op", "left", "right", "kind", "join_pairs", "projections",
    }:
        raise ValueError("keyed join fields differ from stream plan v1")
    if node["kind"] not in JOIN_KINDS:
        raise ValueError("unsupported keyed join kind")
    _validate_join_pairs(node["join_pairs"], "keyed join")
    projections = _validate_projection_list(
        node["projections"], "keyed join")
    if node["kind"] in {"semi", "anti"} and any(
        item["source"] != "left" for item in projections
    ):
        raise ValueError("semi/anti join projections must come from the left")


def _validate_theta_join(node):
    if set(node) != {
        "id", "op", "left", "right", "kind", "sql", "strategy",
        "left_columns", "right_columns",
    }:
        raise ValueError("theta join fields differ from stream plan v1")
    if node["kind"] not in THETA_KINDS:
        raise ValueError("unsupported theta join kind")
    if node["strategy"] != "full_input_recompute":
        raise ValueError("theta join must be marked full_input_recompute")
    sql = node["sql"]
    if not isinstance(sql, str) or not sql.strip() or sql != sql.strip():
        raise ValueError("theta join sql is not canonical")
    for name in ("left_columns", "right_columns"):
        columns = node[name]
        if not isinstance(columns, list) or not columns:
            raise ValueError("theta join columns cannot be empty")
        _names(columns, name)


def _validate_set_op(node):
    if set(node) != {"id", "op", "left", "right", "mode"}:
        raise ValueError("set operator fields differ from stream plan v1")
    if node["mode"] not in SET_MODES:
        raise ValueError("unsupported set operator mode")


def _validate_distinct(node):
    if set(node) != {"id", "op", "input"}:
        raise ValueError("distinct node fields differ from stream plan v1")


def _validate_recompute(node):
    if set(node) != {
        "id", "op", "sources", "tables", "input_columns", "sql", "strategy",
    }:
        raise ValueError("recompute fields differ from stream plan v1")
    if node["strategy"] != "full_input_recompute":
        raise ValueError("recompute must be marked full_input_recompute")
    sql = node["sql"]
    if not isinstance(sql, str) or not sql.strip() or sql != sql.strip():
        raise ValueError("recompute sql is not canonical")
    sources = node["sources"]
    tables = node["tables"]
    if (
        not isinstance(sources, list) or not sources
        or not isinstance(tables, list)
        or len(tables) != len(sources)
    ):
        raise ValueError("recompute sources and tables must align")
    for name in tables:
        _text(name, "recompute table")
    columns = node["input_columns"]
    if not isinstance(columns, dict) or set(columns) != set(sources):
        raise ValueError("recompute input columns must cover every source")
    for name, fields in columns.items():
        if not isinstance(fields, list) or not fields:
            raise ValueError("recompute input columns cannot be empty")
        _names(fields, "recompute column")


def _validate_join(node):
    if set(node) != {
        "id", "op", "left", "right", "join_pairs", "projections",
    }:
        raise ValueError("equi-join node fields differ from stream plan v1")
    pairs = node["join_pairs"]
    if not isinstance(pairs, list) or not pairs:
        raise ValueError("equi-join requires join pairs")
    seen_left = set()
    seen_right = set()
    normalized = []
    for item in pairs:
        if not isinstance(item, dict) or set(item) != {"left", "right"}:
            raise ValueError("invalid equi-join pair")
        left = _text(item["left"], "equi-join left key")
        right = _text(item["right"], "equi-join right key")
        if left in seen_left or right in seen_right:
            raise ValueError("equi-join key repeats a source column")
        seen_left.add(left)
        seen_right.add(right)
        normalized.append(dict(left=left, right=right))
    if pairs != normalized:
        raise ValueError("equi-join pairs are not canonical")
    projections = node["projections"]
    if not isinstance(projections, list) or not projections:
        raise ValueError("equi-join projections cannot be empty")
    seen = set()
    for item in projections:
        if (
            not isinstance(item, dict)
            or set(item) != {"output", "source", "column"}
        ):
            raise ValueError("invalid equi-join projection")
        output = _text(item["output"], "equi-join output")
        source = str(item["source"])
        column = _text(item["column"], "equi-join projection column")
        if source not in {"left", "right"}:
            raise ValueError("equi-join projection source is invalid")
        if output in seen or output == "_m2s_z_weight":
            raise ValueError("equi-join output is reserved or duplicate")
        seen.add(output)


def validate_plan(plan):
    if not isinstance(plan, dict):
        raise ValueError("stream plan must be a dict")
    if set(plan) != {
        "format_version", "kind", "inputs", "nodes", "output", "semantics",
    }:
        raise ValueError("stream plan fields differ from format v1")
    if int(plan["format_version"]) != FORMAT_VERSION:
        raise ValueError("unsupported stream plan format")
    if plan["kind"] != "stream_dag":
        raise ValueError("unsupported stream plan kind")
    if plan["semantics"] != SEMANTICS:
        raise ValueError("unsupported stream plan semantics")
    inputs = plan["inputs"]
    if not isinstance(inputs, list) or not inputs:
        raise ValueError("stream plan inputs cannot be empty")
    nodes = plan["nodes"]
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("stream plan nodes cannot be empty")
    seen = set()
    declared = []
    for node in nodes:
        if not isinstance(node, dict) or "op" not in node or "id" not in node:
            raise ValueError("stream node must have an id and operator")
        node_id = _text(node["id"], "stream node id")
        if node_id in seen:
            raise ValueError("duplicate stream node " + node_id)
        for ref in _node_inputs(node):
            if ref not in seen:
                raise ValueError(
                    "stream node input is not an earlier node: " + ref)
        op = node["op"]
        if op == "input":
            if set(node) != {"id", "op", "input"}:
                raise ValueError("input node fields differ from stream plan v1")
            name = _text(node["input"], "stream input")
            declared.append(name)
        elif op == "filter":
            _validate_filter(node)
        elif op == "project":
            _validate_project(node)
        elif op == "aggregate":
            _validate_aggregate(node)
        elif op == "distinct":
            _validate_distinct(node)
        elif op == "equi_join":
            _validate_join(node)
        elif op == "keyed_join":
            _validate_keyed_join(node)
        elif op == "theta_join":
            _validate_theta_join(node)
        elif op == "set_op":
            _validate_set_op(node)
        elif op == "recompute":
            _validate_recompute(node)
        else:
            raise ValueError("unsupported stream operator: " + str(op))
        seen.add(node_id)
    unique = []
    for name in declared:
        if name not in unique:
            unique.append(name)
    if unique != list(inputs):
        raise ValueError("stream inputs do not match input nodes")
    output = _text(plan["output"], "stream output")
    if output not in seen:
        raise ValueError("stream output is not a node")
    return plan


def explain(plan):
    plan = validate_plan(plan)
    lines = ["stream_dag output=" + plan["output"]]
    for node in plan["nodes"]:
        op = node["op"]
        if op == "input":
            lines.append("input " + node["id"] + " " + node["input"])
        elif op == "filter":
            lines.append(
                "filter " + node["id"] + " <- " + node["input"]
                + " scope=" + node["scope"])
        elif op == "project":
            outputs = ",".join(
                item["output"] for item in node["expressions"])
            lines.append(
                "project " + node["id"] + " <- " + node["input"]
                + " " + outputs)
        elif op == "aggregate":
            funcs = ",".join(
                item["function"] + ":" + item["output"]
                for item in node["aggregates"])
            lines.append(
                "aggregate " + node["id"] + " <- " + node["input"]
                + " group=" + ",".join(node["group_keys"])
                + " " + funcs)
        elif op == "distinct":
            lines.append("distinct " + node["id"] + " <- " + node["input"])
        elif op == "set_op":
            lines.append(
                "set_op " + node["mode"] + " " + node["id"]
                + " " + node["left"] + " " + node["right"])
        elif op == "keyed_join":
            lines.append(
                "keyed_join " + node["kind"] + " " + node["id"]
                + " " + node["left"] + " " + node["right"])
        elif op == "theta_join":
            lines.append(
                "theta_join " + node["kind"] + " " + node["id"]
                + " strategy=" + node["strategy"])
        elif op == "recompute":
            lines.append(
                "recompute " + node["id"] + " strategy=" + node["strategy"])
        else:
            pairs = ",".join(
                item["left"] + "=" + item["right"]
                for item in node["join_pairs"])
            lines.append(
                "equi_join " + node["id"] + " "
                + node["left"] + " " + node["right"]
                + " on " + pairs)
    return "\n".join(lines)


def _plan(inputs, nodes, output):
    return validate_plan(dict(
        format_version=FORMAT_VERSION,
        kind="stream_dag",
        inputs=list(inputs),
        nodes=nodes,
        output=output,
        semantics=dict(SEMANTICS),
    ))


def _filters(previous, source_filter, query_filter):
    nodes = []
    if source_filter:
        nodes.append(dict(
            id="filter_source", op="filter", input=previous,
            predicate=source_filter, scope="source"))
        previous = "filter_source"
    if query_filter:
        nodes.append(dict(
            id="filter_query", op="filter", input=previous,
            predicate=query_filter, scope="query"))
        previous = "filter_query"
    return nodes, previous


def lower_stateless(rel):
    relational_ir.validate_ir(rel)
    incremental_ir.compile_ir(rel)
    nodes = [dict(id="in_source", op="input", input="source")]
    extra, previous = _filters(
        "in_source", rel["source_filter"], rel["query_filter"])
    nodes.extend(extra)
    nodes.append(dict(
        id="project", op="project", input=previous,
        expressions=[
            dict(output=item["output"], expression=item["expression"])
            for item in rel["projection"]
        ],
    ))
    return _plan(["source"], nodes, "project")


def lower_aggregate(ir):
    aggregate_ir.validate_ir(ir)
    nodes = [dict(id="in_source", op="input", input="source")]
    extra, previous = _filters(
        "in_source", ir["source_filter"], ir["query_filter"])
    nodes.extend(extra)
    nodes.append(dict(
        id="aggregate", op="aggregate", input=previous,
        group_keys=list(ir["group_keys"]),
        aggregates=[dict(item) for item in ir["aggregates"]],
    ))
    return _plan(["source"], nodes, "aggregate")


def lower_join(ir):
    join_ir.validate_ir(ir)
    nodes = [
        dict(id="in_left", op="input", input="left"),
        dict(id="in_right", op="input", input="right"),
        dict(
            id="join", op="equi_join",
            left="in_left", right="in_right",
            join_pairs=[dict(item) for item in ir["join_pairs"]],
            projections=[dict(item) for item in ir["projections"]],
        ),
    ]
    return _plan(["left", "right"], nodes, "join")
