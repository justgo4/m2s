#!/usr/bin/env python3
"""UDF plan identity, sandbox, explain, metrics, and tenant audit."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_ops
import stream_sql


def test_identity_and_sandbox():
    plus = stream_ops.register_udf(
        "plus_one", "python", "scalar", True, "def udf(n):\n    return n + 1\n")
    randomish = stream_ops.register_udf(
        "coin", "python", "scalar", False, "def udf(n):\n    return n\n")
    try:
        stream_ops.udf_plan_id([randomish])
        raise AssertionError("non-deterministic udf was accepted")
    except ValueError as exc:
        if "timed_recompute" not in str(exc):
            raise
    plain = stream_ops.udf_plan_id([plus])
    marked = stream_ops.udf_plan_id([randomish], recompute_mode="timed_recompute")
    if plain == marked:
        raise AssertionError("determinism did not change the plan id")
    flipped = stream_ops.udf_plan_id([dict(plus, deterministic=False)], recompute_mode="timed_recompute")
    if flipped == stream_ops.udf_plan_id([plus], recompute_mode="timed_recompute"):
        raise AssertionError("deterministic flag was ignored")
    if stream_ops.execute_python_udf(plus, [4]) != 5:
        raise AssertionError("sandbox result")
    try:
        stream_ops.execute_python_udf(
            dict(plus, source="def udf(n):\n    return open('x')\n"), [1])
        raise AssertionError("open() was executed")
    except ValueError:
        pass
    rust = stream_ops.register_udf("r", "rust", "scalar", True, "fn udf() {}")
    try:
        stream_ops.execute_python_udf(rust, [])
        raise AssertionError("rust udf was executed")
    except RuntimeError:
        pass


def test_explain_metrics_quota():
    compiled = stream_sql.compile_sql(
        "SELECT id FROM events", {"events": ["id"]})
    text = stream_ops.explain_analyze(compiled["plan"])
    if "stream_dag" not in text or "input" not in text:
        raise AssertionError(text)
    page = stream_ops.status_page({"m2s_rows": 3})
    if "<pre>" not in page or "m2s_rows 3" not in page:
        raise AssertionError(page)
    quota = stream_ops.TenantQuota()
    if quota.admit("blue", 2, 3) != 2:
        raise AssertionError("admit")
    try:
        quota.admit("blue", 2, 3)
        raise AssertionError("quota was not enforced")
    except stream_ops.QuotaExceeded:
        pass
    if quota.audit[-1]["decision"] != "deny":
        raise AssertionError(quota.audit)
    dockerfile = (ROOT / "deploy" / "Dockerfile").read_text()
    values = (ROOT / "deploy" / "helm" / "m2s" / "values.yaml").read_text()
    env = (ROOT / "deploy" / "synthetic.env").read_text()
    if "password" in dockerfile.lower() or "password" in values.lower() or "password" in env.lower():
        raise AssertionError("packaging contains a credential")
    if "exampleOnly: true" not in values:
        raise AssertionError("helm values are not marked synthetic")


def main():
    test_identity_and_sandbox()
    test_explain_metrics_quota()
    print("stream_ops_test ok", flush=True)


if __name__ == "__main__":
    main()
