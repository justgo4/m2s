#!/usr/bin/env python3
"""Write to a local Doris FE, replay a label, and restart FE and BE."""
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pymysql

import doris_sink


def _home():
    return Path(os.environ.get(
        "DORIS_HOME", "/tmp/apache-doris-2.1.11-bin-x64"))


def _port_open(port):
    probe = socket.socket()
    probe.settimeout(0.3)
    try:
        probe.connect(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def _alive():
    if not _port_open(9030):
        return False
    try:
        con = pymysql.connect(
            host="127.0.0.1", port=9030, user="root", password="",
            connect_timeout=3, read_timeout=5)
    except pymysql.err.OperationalError:
        return False
    try:
        cur = con.cursor()
        cur.execute("SHOW BACKENDS")
        cols = [item[0] for item in cur.description]
        rows = cur.fetchall()
        index = cols.index("Alive")
        return any(str(row[index]).lower() == "true" for row in rows)
    finally:
        con.close()


def _script(role, name):
    env = dict(os.environ)
    env["JAVA_HOME"] = env.get(
        "JAVA_HOME", "/usr/lib/jvm/java-21-openjdk-amd64")
    command = ["bash", str(_home() / role / "bin" / name)]
    if name.startswith("start_"):
        command.append("--daemon")
    subprocess.check_call(command, cwd=str(_home() / role), env=env)


def _wait_fe():
    deadline = time.time() + 90
    while time.time() < deadline:
        if _port_open(9030):
            try:
                con = pymysql.connect(
                    host="127.0.0.1", port=9030, user="root", password="",
                    connect_timeout=2, read_timeout=5)
                con.close()
                return
            except pymysql.err.OperationalError:
                pass
        time.sleep(0.5)
    raise RuntimeError("doris fe did not answer")


def _wait_alive():
    deadline = time.time() + 90
    while time.time() < deadline:
        if _alive():
            return
        time.sleep(1)
    raise RuntimeError("doris be did not become alive")


def _start():
    if not _port_open(9030):
        _script("fe", "start_fe.sh")
        _wait_fe()
    if not _alive():
        _script("be", "start_be.sh")
        _wait_alive()
        con = pymysql.connect(
            host="127.0.0.1", port=9030, user="root", password="",
            connect_timeout=5)
        try:
            cur = con.cursor()
            try:
                cur.execute("ALTER SYSTEM ADD BACKEND '127.0.0.1:9050'")
            except pymysql.err.OperationalError as exc:
                if "already exists" not in str(exc):
                    raise
        finally:
            con.close()
        _wait_alive()


def _stop():
    if (_home() / "be" / "bin" / "be.pid").exists():
        _script("be", "stop_be.sh")
    if (_home() / "fe" / "bin" / "fe.pid").exists():
        _script("fe", "stop_fe.sh")
    deadline = time.time() + 40
    while time.time() < deadline and _port_open(9030):
        time.sleep(0.5)


def main():
    if not (_home() / "fe" / "bin" / "start_fe.sh").is_file():
        raise SystemExit("doris binary tree is missing: " + str(_home()))
    _start()
    sink = doris_sink.DorisSink()
    version = sink.version()
    sink.ensure()
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
    _stop()
    _start()
    restored = doris_sink.DorisSink()
    if restored.count() != 4:
        raise AssertionError("restart lost rows: %s" % restored.count())
    if restored.write("d1", rows):
        raise AssertionError("delivery survived as reapplicable")
    if restored.count() != 4:
        raise AssertionError(restored.count())
    print(
        "doris_sink rows=4 deliveries=2 replayed=0 restarted=1 version=%s"
        % version,
        flush=True)
    _stop()
    print("doris_sink_test ok", flush=True)


if __name__ == "__main__":
    main()
