#!/usr/bin/env python3
"""UDF identity, sandboxes, explain, metrics, and tenant quota.

Python scalar, aggregate, and table functions run in a fresh `python -I`
process with a stripped builtins set. WASM and Rust both run in wasmtime.
Rust is compiled to wasm32-unknown-unknown first. Modules that import
anything are refused, and fuel limits a runaway loop. Non-deterministic
functions are rejected unless the plan says timed_recompute.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
import subprocess
import sys
import tempfile
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


def _reject_unsandboxed(source):
    if any(token in source for token in ("import", "open", "exec", "eval", "__")):
        raise ValueError("udf source is outside the sandbox")


def _python_call(source, kind, payload):
    _reject_unsandboxed(source)
    script = (
        "import json,sys\n"
        "allowed={'abs':abs,'min':min,'max':max,'len':len,'int':int,"
        "'str':str,'float':float,'list':list,'range':range}\n"
        "ns={'__builtins__':allowed}\n"
        "exec(sys.stdin.read(), ns)\n"
        "payload=json.loads(sys.argv[1])\n"
        "kind=sys.argv[2]\n"
        "if kind=='scalar':\n"
        "    out=ns['udf'](*payload)\n"
        "elif kind=='aggregate':\n"
        "    state=0\n"
        "    for value in payload['add']:\n"
        "        state=ns['accumulate'](state, value)\n"
        "    for value in payload['retract']:\n"
        "        state=ns['retract'](state, value)\n"
        "    out=ns['finish'](state)\n"
        "elif kind=='table':\n"
        "    out=ns['udtf'](*payload)\n"
        "    if not isinstance(out, list):\n"
        "        raise SystemExit('udtf must return a list')\n"
        "else:\n"
        "    raise SystemExit('unknown udf kind')\n"
        "print(json.dumps(out))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-I", "-c", script, json.dumps(payload), kind],
        input=source, text=True, capture_output=True, timeout=2)
    if proc.returncode != 0:
        raise RuntimeError("udf sandbox failed: " + proc.stderr[-400:])
    return json.loads(proc.stdout)


def execute_python_udf(function, args):
    if function["language"] != "python" or function["kind"] != "scalar":
        raise RuntimeError(
            "only a Python scalar UDF is executed; %s/%s is refused"
            % (function["language"], function["kind"]))
    return _python_call(function["source"], "scalar", list(args))


def execute_python_udaf(function, additions, retractions=()):
    if function["language"] != "python" or function["kind"] != "aggregate":
        raise RuntimeError("python udaf is required")
    if not function["deterministic"]:
        raise ValueError("non-deterministic udaf is refused")
    return _python_call(function["source"], "aggregate", dict(
        add=list(additions), retract=list(retractions)))


def execute_python_udtf(function, args):
    if function["language"] != "python" or function["kind"] != "table":
        raise RuntimeError("python udtf is required")
    if not function["deterministic"]:
        raise ValueError("non-deterministic udtf is refused")
    return _python_call(function["source"], "table", list(args))


def run_wasm(module_bytes, export, args, fuel=100000):
    import wasmtime
    config = wasmtime.Config()
    config.consume_fuel = True
    engine = wasmtime.Engine(config)
    module = wasmtime.Module(engine, module_bytes)
    if list(module.imports):
        raise ValueError("wasm udf imports are outside the sandbox")
    store = wasmtime.Store(engine)
    store.set_fuel(int(fuel))
    try:
        instance = wasmtime.Instance(store, module, [])
        function = instance.exports(store)[str(export)]
        value = function(store, *list(args))
    except wasmtime.Trap as exc:
        detail = " ".join(str(exc).splitlines())
        cause = exc.__cause__
        if cause is not None:
            detail = detail + " " + " ".join(str(cause).splitlines())
        raise RuntimeError("wasm sandbox trapped: " + detail[:300])
    left = int(store.get_fuel())
    return dict(value=value, fuel_used=int(fuel) - left, fuel_left=left)


def execute_wasm_udf(function, module_bytes, args, fuel=100000):
    if function["language"] not in {"wasm", "rust"} or function["kind"] != "scalar":
        raise RuntimeError("wasm scalar udf is required")
    if not function["deterministic"]:
        raise ValueError("non-deterministic wasm udf is refused")
    return run_wasm(module_bytes, function["name"], args, fuel)


def compile_rust_wasm(source, export):
    if any(token in source for token in ("unsafe", "include!", "std::", "libc")):
        raise ValueError("rust udf source is outside the sandbox")
    if ("fn %s(" % export) not in source and ('fn %s (' % export) not in source:
        raise ValueError("rust udf does not define " + export)
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    cache = os.path.join(tempfile.gettempdir(), "m2s-rust-wasm")
    os.makedirs(cache, exist_ok=True)
    binary = os.path.join(cache, digest + ".wasm")
    if not os.path.exists(binary):
        source_path = os.path.join(cache, digest + ".rs")
        with open(source_path, "w") as handle:
            handle.write(source)
        proc = subprocess.run(
            [
                "rustc", "--target", "wasm32-unknown-unknown",
                "--crate-type", "cdylib", "--edition", "2021",
                "-C", "opt-level=z", "-o", binary, source_path,
            ],
            text=True, capture_output=True, timeout=60)
        if proc.returncode != 0:
            raise RuntimeError("rust udf compile failed: " + proc.stderr[-400:])
    with open(binary, "rb") as handle:
        return handle.read()


def execute_rust_udf(function, args, fuel=100000):
    if function["language"] != "rust" or function["kind"] != "scalar":
        raise RuntimeError("rust scalar udf is required")
    if not function["deterministic"]:
        raise ValueError("non-deterministic rust udf is refused")
    module = compile_rust_wasm(function["source"], function["name"])
    return run_wasm(module, function["name"], args, fuel)


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
