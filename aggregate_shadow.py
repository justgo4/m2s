#!/usr/bin/env python3
"""COUNT/SUM shadow for one aggregate consumer transaction.

The publisher remains aggregate_state and the outbox. This module replays
the durable source log through the in-memory stream aggregate and compares
group rows before the outbox write. It does not store its own frontier.
If the source log no longer contains commits 1..seq, comparison refuses
the publish instead of guessing.
"""
import os

import aggregate_state
import source_state
import stream_eval
import stream_ir


class ShadowMismatch(RuntimeError):
    pass


def enabled(flag):
    if flag is False:
        return False
    if flag is True or callable(flag):
        return True
    raw = str(os.environ.get("M2S_AGGREGATE_SHADOW", "")).strip().lower()
    return raw in {"1", "true", "on"}


def _plan(ir):
    aggregates = []
    for item in ir["aggregates"]:
        function = item["function"]
        if function not in {"count", "sum"}:
            raise ShadowMismatch(
                "aggregate shadow v1 only covers COUNT and SUM")
        aggregates.append(dict(
            output=item["output"],
            function=function,
            input=item["input"],
        ))
    nodes = [
        dict(id="in_source", op="input", input="source"),
        dict(
            id="aggregate", op="aggregate", input="in_source",
            group_keys=list(ir["group_keys"]),
            aggregates=aggregates,
        ),
    ]
    return stream_ir.validate_plan(dict(
        format_version=stream_ir.FORMAT_VERSION,
        kind="stream_dag",
        inputs=["source"],
        nodes=nodes,
        output="aggregate",
        semantics=dict(stream_ir.SEMANTICS),
    ))


def _norm(value):
    if value is None:
        return None
    if hasattr(value, "as_tuple"):
        return format(value, "f")
    return value


def _identity(ir, row):
    keys = tuple(_norm(row[name]) for name in ir["group_keys"])
    values = tuple(_norm(row[item["output"]]) for item in ir["aggregates"])
    return keys + values


def _delta(ir, changes):
    rows = []
    for change in changes:
        op = int(change["_sync_op"])
        if op not in (0, 1):
            raise ShadowMismatch("aggregate shadow saw _sync_op %s" % op)
        row = {}
        for name in ir["group_keys"]:
            row[name] = change[name]
        for item in ir["aggregates"]:
            if item["input"] != "*":
                row[item["input"]] = change[item["input"]]
        rows.append((row, 1 if op == 0 else -1))
    return rows


def verify(con, ir, state_id, seq, source_relation, transform):
    """Replay durable commits 1..seq and compare to aggregate_state.

    transform is aggregate_log_consumer.transform_commit. It is passed in
    so this module does not import the consumer while the consumer imports
    this module.
    """
    seq = int(seq)
    plan = _plan(ir)
    graph = stream_eval.StreamGraph(plan)
    try:
        commits = source_state.read_commits(
            con, 0, through_seq=seq, limit=max(1, seq))
        seen = [int(commit["seq"]) for commit in commits]
        if seen != list(range(1, seq + 1)):
            raise ShadowMismatch(
                "source log is not a complete prefix through %d" % seq)
        for commit in commits:
            changes = transform(commit, source_relation, ir)
            graph.push({"source": _delta(ir, changes)})
        expected = []
        for row, weight in graph.output.items():
            if int(weight) != 1:
                raise ShadowMismatch(
                    "shadow group weight is %s" % weight)
            expected.append(_identity(ir, row))
    finally:
        graph.close()
    actual = [
        _identity(ir, row)
        for row in aggregate_state.read_rows(con, state_id)
    ]
    if sorted(expected) != sorted(actual):
        raise ShadowMismatch(
            "aggregate shadow mismatch groups=%d durable=%d seq=%d"
            % (len(expected), len(actual), seq))
    return seq
