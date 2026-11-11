#!/usr/bin/env python3
"""UDF identity, a Python sandbox, explain, metrics, and tenant quota.

Rust and WASM sources can be recorded, but they are not executed: this VM
has no WASM runtime wired in, and a Rust UDF is not compiled on the CDC path.
Non-deterministic functions are rejected unless the plan says timed_recompute.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import subprocess
import sys
import threading

import stream_ir


class QuotaExceeded(RuntimeError):
    pass


def register_udf(name, language, kind, deterministic, source):
    language = str(language).lower()
    kind = str(kind).lower()
    if language not in {"python", "rust", "wasm"}:
        raise ValueError("udf language is not python, rust, or wasm")
    if kind not in {"scalar", "aggregate", "table"}:
        raise ValueError("udf kind is not scalar, aggregate, or table")
    if not isinstance(deterministic, bool):
        raise TypeError("deterministic must be a bool")
    return dict(
        name=str(name),
        language=language,
        kind=kind,
        deterministic=deterministic,
        source=str(source),
    )


def udf_plan_id(functions, recompute_mode="reject"):
    if recompute_mode not in {"reject", "timed_recompute"}:
        raise ValueError("recompute mode must be reject or timed_recompute")
    body = []
    for item in functions:
        if not item["deterministic"] and recompute_mode != "timed_recompute":
            raise ValueError(
                "non-deterministic udf %s requires timed_recompute" % item["name"])
        body.append(dict(
            name=item["name"],
            language=item["language"],
            kind=item["kind"],
            deterministic=bool(item["deterministic"]),
            source_sha256=hashlib.sha256(item["source"].encode("utf-8")).hexdigest(),
        ))
    payload = json.dumps(
        dict(recompute_mode=recompute_mode, functions=body),
        sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def execute_python_udf(function, args):
    if function["language"] != "python" or function["kind"] != "scalar":
        raise RuntimeError(
            "only a Python scalar UDF is executed; %s/%s is refused"
            % (function["language"], function["kind"]))
    source = function["source"]
    if any(token in source for token in ("import", "open", "exec", "eval", "__")):
        raise ValueError("udf source is outside the sandbox")
    script = (
        "import json,sys\n"
        "allowed={'abs':abs,'min':min,'max':max,'len':len,'int':int,'str':str,'float':float}\n"
        "ns={'__builtins__':allowed}\n"
        "exec(sys.stdin.read(), ns)\n"
        "print(json.dumps(ns['udf'](*json.loads(sys.argv[1]))))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-I", "-c", script, json.dumps(list(args))],
        input=source, text=True, capture_output=True, timeout=2)
    if proc.returncode != 0:
        raise RuntimeError("udf sandbox failed: " + proc.stderr[-400:])
    return json.loads(proc.stdout)


def explain_analyze(plan):
    text = stream_ir.explain(plan)
    strategies = []
    for node in plan["nodes"]:
        strategy = node.get("strategy")
        if strategy:
            strategies.append(node["op"] + " " + node["id"] + " strategy=" + strategy)
    if strategies:
        text = text + "\n" + "\n".join(strategies)
    return text


def prometheus_text(samples):
    lines = []
    for name in sorted(samples):
        value = samples[name]
        lines.append("%s %s" % (name, value))
    return "\n".join(lines) + "\n"


def serve_status(samples, host="127.0.0.1", port=0):
    page = status_page(samples).encode("utf-8")
    metrics = prometheus_text(samples).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/metrics"):
                body, kind = metrics, "text/plain; version=0.0.4"
            else:
                body, kind = page, "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            return

    server = ThreadingHTTPServer((host, int(port)), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def status_page(samples):
    body = prometheus_text(samples).replace("&", "&amp;").replace("<", "&lt;")
    return "<!DOCTYPE html><title>m2s</title><pre>" + body + "</pre>\n"


class TenantQuota:
    def __init__(self):
        self.usage = {}
        self.audit = []

    def admit(self, tenant, rows, limit):
        tenant = str(tenant)
        used = int(self.usage.get(tenant, 0)) + int(rows)
        if used > int(limit):
            self.audit.append(dict(tenant=tenant, decision="deny", rows=used))
            raise QuotaExceeded("tenant %s exceeded %s" % (tenant, limit))
        self.usage[tenant] = used
        self.audit.append(dict(tenant=tenant, decision="allow", rows=used))
        return used
