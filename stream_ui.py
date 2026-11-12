#!/usr/bin/env python3
"""Operable status page for one streaming job.

The page draws the plan DAG, per-operator rows and latency, and the
checkpoint epoch. Pause stops batches from landing. Resume lets them
land again. Checkpoint does not advance while the job is paused.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time

import stream_eval
import stream_ir
import stream_sql


def _rows_of(result):
    if isinstance(result, list):
        return sum(int(weight) for _row, weight in result)
    if hasattr(result, "weights"):
        return sum(int(weight) for weight in result.weights.values())
    return 0


def _edges(plan):
    found = []
    for node in plan["nodes"]:
        for ref in stream_ir._node_inputs(node):
            found.append((ref, node["id"]))
    return found


class JobRunner:
    def __init__(self, job_id, sql, sources):
        compiled = stream_sql.compile_sql(sql, sources)
        self.job_id = str(job_id)
        self.plan = compiled["plan"]
        self.graph = stream_eval.StreamGraph(self.plan)
        self.paused = False
        self.checkpoint = dict(epoch=0, status="none")
        self.operators = {}
        for node in self.plan["nodes"]:
            self.operators[node["id"]] = dict(
                id=node["id"], op=node["op"], rows=0,
                latency_seconds=0.0, batches=0)
        self.output_rows = 0
        self.rejected_while_paused = 0

    def close(self):
        self.graph.close()

    def push(self, batch):
        if self.paused:
            self.rejected_while_paused += 1
            return dict(applied=False, output_rows=self.output_rows)
        timings = {}
        original = self.graph._eval

        def wrapped(node, current, deltas, agg, joins, extra):
            started = time.perf_counter()
            result = original(node, current, deltas, agg, joins, extra)
            timings[node["id"]] = (
                _rows_of(result), time.perf_counter() - started)
            return result

        self.graph._eval = wrapped
        try:
            produced = self.graph.push(batch)
        finally:
            self.graph._eval = original
        self.output_rows = sum(self.graph.output.weights.values())
        for node_id, (rows, elapsed) in timings.items():
            slot = self.operators[node_id]
            slot["rows"] += rows
            slot["latency_seconds"] = elapsed
            slot["batches"] += 1
        return dict(
            applied=True,
            output_rows=self.output_rows,
            produced=_rows_of(produced))

    def pause(self):
        self.paused = True
        if self.checkpoint["epoch"]:
            self.checkpoint["status"] = "paused"
        else:
            self.checkpoint["status"] = "paused"
        return dict(paused=True)

    def resume(self):
        self.paused = False
        if self.checkpoint["epoch"]:
            self.checkpoint["status"] = "completed"
        else:
            self.checkpoint["status"] = "none"
        return dict(paused=False)

    def checkpoint_now(self):
        if self.paused:
            self.checkpoint["status"] = "paused"
            return dict(self.checkpoint)
        self.checkpoint = dict(
            epoch=int(self.checkpoint["epoch"]) + 1, status="completed")
        return dict(self.checkpoint)

    def snapshot(self):
        operators = []
        for node in self.plan["nodes"]:
            slot = dict(self.operators[node["id"]])
            elapsed = slot["latency_seconds"]
            slot["rows_per_second"] = (
                slot["rows"] / elapsed if elapsed else float(slot["rows"]))
            operators.append(slot)
        return dict(
            job_id=self.job_id,
            paused=self.paused,
            output_rows=self.output_rows,
            rejected_while_paused=self.rejected_while_paused,
            checkpoint=dict(self.checkpoint),
            edges=_edges(self.plan),
            operators=operators,
        )


def _escape(value):
    return (str(value).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def render_page(snapshot):
    nodes = snapshot["operators"]
    width = 220
    height = 64
    gap = 28
    parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>m2s ", _escape(snapshot["job_id"]), "</title>",
        "<style>body{font-family:sans-serif;margin:24px}",
        "table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:4px 8px}",
        ".paused{color:#a40}.running{color:#063}</style></head><body>",
        "<h1>job ", _escape(snapshot["job_id"]), "</h1>",
        "<p class='", "paused" if snapshot["paused"] else "running", "'>",
        "state=", "paused" if snapshot["paused"] else "running",
        " checkpoint_epoch=", str(snapshot["checkpoint"]["epoch"]),
        " checkpoint_status=", _escape(snapshot["checkpoint"]["status"]),
        " output_rows=", str(snapshot["output_rows"]), "</p>",
        "<svg id='dag' width='640' height='",
        str(max(80, len(nodes) * (height + gap))),
        "' xmlns='http://www.w3.org/2000/svg'>",
    ]
    positions = {}
    order = [item["id"] for item in nodes]
    for index, node in enumerate(nodes):
        x, y = 160, 20 + index * (height + gap)
        positions[node["id"]] = (x + width / 2, y, y + height)
        parts.append(
            "<rect x='%d' y='%d' width='%d' height='%d' fill='#eef' stroke='#225'/>"
            % (x, y, width, height))
        parts.append(
            "<text x='%d' y='%d'>%s %s rows=%s</text>"
            % (x + 8, y + 28, _escape(node["id"]), _escape(node["op"]), node["rows"]))
        parts.append(
            "<text x='%d' y='%d'>latency=%.6fs</text>"
            % (x + 8, y + 48, node["latency_seconds"]))
    for source, target in snapshot["edges"]:
        sx, _top, bottom = positions[source]
        tx, top, _bottom = positions[target]
        lane = order.index(target) - order.index(source)
        bend = 36 if lane != 1 else 0
        parts.append(
            "<path d='M %d %d C %d %d, %d %d, %d %d' fill='none' stroke='#225'/>"
            % (sx, bottom, sx + bend, bottom + 12, tx + bend, top - 12, tx, top))
    parts.append("</svg><table><tr><th>operator</th><th>op</th><th>rows</th>"
                 "<th>latency_seconds</th><th>rows_per_second</th></tr>")
    for node in nodes:
        parts.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%.6f</td><td>%.3f</td></tr>"
            % ( _escape(node["id"]), _escape(node["op"]), node["rows"],
                node["latency_seconds"], node["rows_per_second"]))
    parts.append("</table></body></html>\n")
    return "".join(parts)


def serve_job(job, host="127.0.0.1", port=0):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, kind):
            data = body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path.startswith("/api/job"):
                self._send(200, json.dumps(job.snapshot()), "application/json")
                return
            self._send(200, render_page(job.snapshot()), "text/html; charset=utf-8")

        def do_POST(self):
            if self.path.startswith("/pause"):
                result = job.pause()
            elif self.path.startswith("/resume"):
                result = job.resume()
            elif self.path.startswith("/checkpoint"):
                result = job.checkpoint_now()
            else:
                self._send(404, "{}", "application/json")
                return
            self._send(200, json.dumps(result), "application/json")

        def log_message(self, fmt, *args):
            return

    server = ThreadingHTTPServer((host, int(port)), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
