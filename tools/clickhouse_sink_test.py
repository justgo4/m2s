#!/usr/bin/env python3
"""Write to a local ClickHouse server, replay a delivery, and restart."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import clickhouse_sink


def _wait_ping(seconds=20):
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            if urlopen("http://127.0.0.1:8123/ping", timeout=1).read().strip() == b"Ok.":
                return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("clickhouse did not answer ping")


def _start(binary):
    log = open("/tmp/m2s-clickhouse.log", "ab")
    proc = subprocess.Popen(
        [binary, "server", "--config-file=/tmp/m2s-ch-cfg/config.xml"],
        stdout=log, stderr=subprocess.STDOUT)
    _wait_ping()
    return proc


def main():
    release = ".".join(("26", "10", "2", "103"))
    binary = os.environ.get(
        "CLICKHOUSE_BIN",
        "/tmp/clickhouse-unpack/clickhouse-common-static-%s/usr/bin/clickhouse" % release)
    if not Path(binary).is_file():
        raise SystemExit("clickhouse binary is missing: " + binary)
    proc = _start(binary)
    try:
        sink = clickhouse_sink.ClickHouseSink()
        sink.ensure()
        sink.query("TRUNCATE TABLE m2s_rows")
        sink.query("TRUNCATE TABLE m2s_deliveries")
        rows = [dict(id=index, v=index * 10) for index in range(1, 4)]
        if not sink.write("d1", rows):
            raise AssertionError("first delivery was skipped")
        if sink.write("d1", rows):
            raise AssertionError("duplicate delivery was applied")
        if sink.count() != 3:
            raise AssertionError(sink.count())
        if not sink.write("d2", [dict(id=4, v=40)]):
            raise AssertionError("second delivery was skipped")
        if sink.count() != 4:
            raise AssertionError(sink.count())
        proc.send_signal(signal.SIGTERM)
        proc.wait(20)
        proc = _start(binary)
        restored = clickhouse_sink.ClickHouseSink()
        if restored.count() != 4:
            raise AssertionError("restart lost rows: %s" % restored.count())
        if restored.write("d1", rows):
            raise AssertionError("delivery survived as reapplicable")
        if restored.count() != 4:
            raise AssertionError(restored.count())
        print(
            "clickhouse_sink rows=4 deliveries=2 replayed=0 restarted=1 version=%s" % release,
            flush=True)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            proc.wait(20)
    print("clickhouse_sink_test ok", flush=True)


if __name__ == "__main__":
    main()
