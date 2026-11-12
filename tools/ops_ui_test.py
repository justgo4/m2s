#!/usr/bin/env python3
"""Drive the job page: DAG, operator latency, pause, resume, checkpoint."""
from pathlib import Path
import json
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_ui


def _request(method, url):
    req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None)
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.read().decode()


def main():
    job = stream_ui.JobRunner(
        "orders", "SELECT id, amount FROM events WHERE amount > 0",
        {"events": ["id", "amount"]})
    server = stream_ui.serve_job(job)
    try:
        base = "http://127.0.0.1:%d" % server.server_address[1]
        batch = {"events": [(dict(id=index, amount=index), 1) for index in range(1, 101)]}
        applied = job.push(batch)
        if not applied["applied"] or applied["output_rows"] != 100:
            raise AssertionError(applied)
        page = _request("GET", base + "/")
        body = json.loads(_request("GET", base + "/api/job"))
        names = [item["id"] for item in body["operators"]]
        if "checkpoint_epoch=0" not in page or "checkpoint_status=none" not in page:
            raise AssertionError("checkpoint text missing")
        for name in names:
            if name not in page:
                raise AssertionError("dag missing " + name)
        if "<svg" not in page or "latency=" not in page:
            raise AssertionError("page has no dag")
        slow = max(item["latency_seconds"] for item in body["operators"])
        _request("POST", base + "/checkpoint")
        _request("POST", base + "/pause")
        held = job.push({"events": [(dict(id=1000, amount=5), 1)]})
        if held["applied"] or held["output_rows"] != 100:
            raise AssertionError(held)
        paused = json.loads(_request("GET", base + "/api/job"))
        if not paused["paused"] or paused["checkpoint"]["status"] != "paused":
            raise AssertionError(paused["checkpoint"])
        if paused["checkpoint"]["epoch"] != 1:
            raise AssertionError(paused["checkpoint"])
        stalled = json.loads(_request("POST", base + "/checkpoint"))
        if stalled["epoch"] != 1 or stalled["status"] != "paused":
            raise AssertionError(stalled)
        _request("POST", base + "/resume")
        job.push({"events": [(dict(id=1000, amount=5), 1)]})
        resumed = json.loads(_request("GET", base + "/api/job"))
        if resumed["paused"] or resumed["output_rows"] != 101:
            raise AssertionError(resumed)
        if "running" not in _request("GET", base + "/"):
            raise AssertionError("resumed page")
        print(
            "ops_ui port=%d operators=%d output_rows=%d paused_rejects=%d "
            "checkpoint_epoch=%d max_operator_latency_seconds=%.6f"
            % (
                server.server_address[1], len(names), resumed["output_rows"],
                resumed["rejected_while_paused"], resumed["checkpoint"]["epoch"],
                slow,
            ),
            flush=True)
    finally:
        server.shutdown()
        job.close()
    print("ops_ui_test ok", flush=True)


if __name__ == "__main__":
    main()
