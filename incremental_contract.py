#!/usr/bin/env python3
"""Backend-neutral contracts for shared incremental state and plan selection.

This module is intentionally independent from the production daemon.  P6/P8/P9
can evolve storage engines and incremental runtimes without changing the
semantic identity of reusable physical state or silently changing retention and
candidate-selection rules.
"""
import hashlib
import json
import math


STATE_KINDS = {
    "base",
    "arrangement",
    "materialized_subview",
    "task_state",
}
PLAN_STRATEGIES = {
    "reuse_state",
    "incremental_build",
    "partial_recompute",
    "full_recompute",
}
COST_FIELDS = (
    "time_to_ready_ms",
    "source_read_bytes",
    "state_bytes",
    "steady_cpu_ms_per_s",
    "write_bytes",
    "catchup_lag_ms",
)


def _nonempty_text(value, name):
    value = str(value or "").strip()
    if not value:
        raise ValueError(name + " must be non-empty")
    return value


def _text_list(values, name, allow_empty=False):
    result = [str(value).strip() for value in values or ()]
    if not allow_empty and not result:
        raise ValueError(name + " must be non-empty")
    if any(not value for value in result):
        raise ValueError(name + " contains an empty value")
    return result


def state_spec(kind, relations, schema_epochs, key_exprs=(), value_exprs=(),
               predicate="TRUE", collation="binary", semantics_version=1):
    """Return the canonical semantic identity input for reusable physical state.

    Expressions and relation names must already be normalized by the SQL/IR
    layer.  Watermarks, storage paths and refcounts are deliberately excluded:
    they describe one instance of state, not whether two states are semantically
    interchangeable.
    """
    kind = _nonempty_text(kind, "kind")
    if kind not in STATE_KINDS:
        raise ValueError("unsupported state kind: " + kind)
    relations = _text_list(relations, "relations")
    epochs = [int(value) for value in schema_epochs or ()]
    if len(epochs) != len(relations):
        raise ValueError("schema_epochs must match relations")
    if any(value < 0 for value in epochs):
        raise ValueError("schema epoch cannot be negative")
    version = int(semantics_version)
    if version < 1:
        raise ValueError("semantics_version must be >= 1")
    return {
        "format_version": 1,
        "kind": kind,
        "relations": relations,
        "schema_epochs": epochs,
        "key_exprs": _text_list(key_exprs, "key_exprs", allow_empty=True),
        "value_exprs": _text_list(value_exprs, "value_exprs", allow_empty=True),
        "predicate": _nonempty_text(predicate, "predicate"),
        "collation": _nonempty_text(collation, "collation"),
        "semantics_version": version,
    }


def canonical_bytes(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def state_identity(spec):
    if not isinstance(spec, dict):
        raise ValueError("state spec must be a dict")
    return hashlib.sha256(canonical_bytes(spec)).hexdigest()


def state_handle(spec, watermark, backend=None, metadata=None):
    watermark = int(watermark)
    if watermark < 0:
        raise ValueError("watermark cannot be negative")
    result = {
        "identity": state_identity(spec),
        "spec": spec,
        "watermark": watermark,
    }
    if backend is not None:
        result["backend"] = _nonempty_text(backend, "backend")
    if metadata is not None:
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a dict")
        result["metadata"] = dict(metadata)
    return result


def state_compatible(existing, requested_spec, minimum_watermark=None):
    if not isinstance(existing, dict):
        return False
    if existing.get("identity") != state_identity(requested_spec):
        return False
    if minimum_watermark is not None:
        try:
            if int(existing.get("watermark", -1)) < int(minimum_watermark):
                return False
        except (TypeError, ValueError):
            return False
    return True


def changelog_retention_floor(source_watermark, consumer_watermarks=(), fixed_w_pins=()):
    """Return the oldest commit sequence that must remain replayable.

    Commits strictly older than the returned floor may be considered for GC.
    A task build pin therefore prevents the changelog from being collected past
    its fixed W even if every live consumer has already advanced further.
    """
    source_watermark = int(source_watermark)
    if source_watermark < 0:
        raise ValueError("source_watermark cannot be negative")
    values = []
    for group_name, group in (
        ("consumer watermark", consumer_watermarks),
        ("fixed-W pin", fixed_w_pins),
    ):
        for value in group or ():
            value = int(value)
            if value < 0 or value > source_watermark:
                raise ValueError(
                    group_name + " must be between 0 and source_watermark"
                )
            values.append(value)
    return min(values) if values else source_watermark


def plan_candidate(strategy, required_state_ids=(), estimates=None, details=None):
    strategy = _nonempty_text(strategy, "strategy")
    if strategy not in PLAN_STRATEGIES:
        raise ValueError("unsupported plan strategy: " + strategy)
    required = sorted(set(_text_list(
        required_state_ids, "required_state_ids", allow_empty=True)))
    estimates = dict(estimates or {})
    normalized = {}
    for name in COST_FIELDS:
        if name not in estimates:
            raise ValueError("missing cost estimate: " + name)
        value = float(estimates[name])
        if not math.isfinite(value) or value < 0:
            raise ValueError("cost estimate must be finite and non-negative: " + name)
        normalized[name] = value
    payload = {
        "format_version": 1,
        "strategy": strategy,
        "required_state_ids": required,
        "estimates": normalized,
        "details": dict(details or {}),
    }
    payload["candidate_id"] = hashlib.sha256(canonical_bytes(payload)).hexdigest()
    return payload


def candidate_dominates(left, right, metrics=COST_FIELDS):
    """True when left is no worse on every metric and better on at least one."""
    metrics = tuple(metrics)
    if not metrics:
        raise ValueError("at least one metric is required")
    better = False
    for name in metrics:
        if name not in COST_FIELDS:
            raise ValueError("unknown cost metric: " + str(name))
        a = float(left["estimates"][name])
        b = float(right["estimates"][name])
        if a > b:
            return False
        if a < b:
            better = True
    return better


def pareto_frontier(candidates, metrics=COST_FIELDS):
    """Keep non-dominated candidates without inventing hidden policy weights."""
    candidates = list(candidates or ())
    seen = set()
    for candidate in candidates:
        identity = candidate.get("candidate_id")
        if not identity or identity in seen:
            raise ValueError("candidate ids must be present and unique")
        seen.add(identity)
    result = []
    for index, candidate in enumerate(candidates):
        dominated = False
        for other_index, other in enumerate(candidates):
            if index == other_index:
                continue
            if candidate_dominates(other, candidate, metrics):
                dominated = True
                break
        if not dominated:
            result.append(candidate)
    return result
