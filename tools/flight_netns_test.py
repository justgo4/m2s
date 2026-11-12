#!/usr/bin/env python3
"""Three network namespaces exchange Arrow Flight batches over TCP.

Each worker binds its own veth address. The coordinator on the host
measures the exchange, a killed node's takeover, and a scale from 3 to 4.
This is not a distributed SLO.
"""
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pyarrow.flight as flight

import stream_cluster
import stream_flight


PREFIX = "m2sfn"
BRIDGE = "br-m2sfn"


def _addr(last):
    return ".".join(("10", "78", "0", str(last)))


def _run(command):
    proc = subprocess.run(command, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-400:] or proc.stdout[-400:])
    return proc.stdout


def _host(index):
    return _addr(index + 2)


def _ns(index):
    return "%s%d" % (PREFIX, index)


class Node:
    def __init__(self, index):
        self.index = index
        self.host = _host(index)
        self.ns = _ns(index)
        self.proc = None
        self.client = None
        self.port = None
        self.mirror = {}


def _setup(count):
    _run(["sudo", "-n", "ip", "link", "add", BRIDGE, "type", "bridge"])
    _run(["sudo", "-n", "ip", "addr", "add", _addr(1) + "/24", "dev", BRIDGE])
    _run(["sudo", "-n", "ip", "link", "set", BRIDGE, "up"])
    nodes = []
    for index in range(count):
        nodes.append(_add_namespace(index))
    return nodes


def _add_namespace(index):
    node = Node(index)
    _run(["sudo", "-n", "ip", "netns", "add", node.ns])
    veth = "veth-" + node.ns
    inner = "eth-" + node.ns
    _run(["sudo", "-n", "ip", "link", "add", veth, "type", "veth", "peer", "name", inner])
    _run(["sudo", "-n", "ip", "link", "set", inner, "netns", node.ns])
    _run(["sudo", "-n", "ip", "link", "set", veth, "master", BRIDGE])
    _run(["sudo", "-n", "ip", "link", "set", veth, "up"])
    _run(["sudo", "-n", "ip", "netns", "exec", node.ns, "ip", "link", "set", "lo", "up"])
    _run(["sudo", "-n", "ip", "netns", "exec", node.ns, "ip", "link", "set", inner, "up"])
    _run([
        "sudo", "-n", "ip", "netns", "exec", node.ns,
        "ip", "addr", "add", node.host + "/24", "dev", inner,
    ])
    return node


def _start(node):
    port_path = "/tmp/%s.port" % node.ns
    if os.path.exists(port_path):
        os.remove(port_path)
    code = (
        "import sys\n"
        "sys.path.insert(0, %r)\n"
        "import stream_flight\n"
        "stream_flight.serve_address(%r, %r)\n"
    ) % (str(ROOT), node.host, port_path)
    user = subprocess.check_output(["id", "-un"], text=True).strip()
    node.proc = subprocess.Popen(
        [
            "sudo", "-n", "ip", "netns", "exec", node.ns,
            "runuser", "-u", user, "--",
            sys.executable, "-c", code,
        ],
        cwd=str(ROOT))
    deadline = time.time() + 20
    while time.time() < deadline:
        if node.proc.poll() is not None:
            raise RuntimeError("worker %s exited %s" % (node.ns, node.proc.returncode))
        if os.path.exists(port_path) and os.path.getsize(port_path):
            node.port = int(open(port_path).read().strip())
            break
        time.sleep(0.05)
    if node.port is None:
        raise RuntimeError("worker %s did not publish a port" % node.ns)
    node.client = flight.FlightClient("grpc://%s:%d" % (node.host, node.port))


def _stop_proc(node):
    if node.client is not None:
        node.client.close()
        node.client = None
    if node.proc is not None and node.proc.poll() is None:
        node.proc.kill()
        node.proc.wait(5)
    text = subprocess.run(
        ["sudo", "-n", "ip", "netns", "pids", node.ns],
        text=True, capture_output=True).stdout
    for item in text.split():
        subprocess.run(["sudo", "-n", "kill", "-9", item], check=False)
    node.proc = None


def _cleanup(nodes):
    for node in nodes:
        try:
            _stop_proc(node)
        except Exception:
            pass
    for node in nodes:
        subprocess.run(["sudo", "-n", "ip", "netns", "del", node.ns], check=False)
    subprocess.run(["sudo", "-n", "ip", "link", "del", BRIDGE], check=False)


def _push(node, rows):
    started = time.perf_counter()
    nbytes = stream_flight._push(node.client, rows)
    elapsed = time.perf_counter() - started
    for key, weight in rows:
        node.mirror[str(key)] = node.mirror.get(str(key), 0) + int(weight)
        if node.mirror[str(key)] == 0:
            node.mirror.pop(str(key), None)
    return nbytes, elapsed


def _totals(nodes):
    merged = {}
    for node in nodes:
        for key, weight in stream_flight._pull(node.client).items():
            merged[key] = merged.get(key, 0) + weight
    return {key: weight for key, weight in merged.items() if weight}


def _ingest(nodes, rows):
    buckets = [[] for _node in nodes]
    for key, weight in rows:
        buckets[stream_cluster.shard_of(key, len(nodes))].append((key, int(weight)))
    started = time.perf_counter()
    nbytes = 0
    for node, batch in zip(nodes, buckets):
        if not batch:
            continue
        sent, _elapsed = _push(node, batch)
        nbytes += sent
    return nbytes, time.perf_counter() - started


def main():
    rows = [("k%04d" % index, (index % 5) + 1) for index in range(200)]
    expected = dict(stream_cluster.preaggregate(rows))
    nodes = []
    try:
        nodes = _setup(3)
        for node in nodes:
            _start(node)
        ping = subprocess.run(
            ["ping", "-c", "1", "-W", "1", nodes[0].host],
            text=True, capture_output=True, check=True).stdout
        ping_ms = float(ping.split("time=")[1].split(" ")[0])
        nbytes, exchange_seconds = _ingest(nodes, rows)
        if _totals(nodes) != expected:
            raise AssertionError("three nodes diverged")
        probe_started = time.perf_counter()
        stream_flight._push(nodes[0].client, [("probe", 1)])
        stream_flight._pull(nodes[0].client)
        nodes[0].mirror["probe"] = nodes[0].mirror.get("probe", 0) + 1
        rtt_seconds = time.perf_counter() - probe_started
        stream_flight._push(nodes[0].client, [("probe", -1)])
        nodes[0].mirror.pop("probe", None)

        victim = nodes[1]
        saved = dict(victim.mirror)
        started = time.perf_counter()
        _stop_proc(victim)
        victim.mirror = {}
        _start(victim)
        replay = [(key, weight) for key, weight in saved.items() if weight]
        if replay:
            sent, _elapsed = _push(victim, replay)
            nbytes += sent
        failover_seconds = time.perf_counter() - started
        if _totals(nodes) != expected:
            raise AssertionError("failover changed the totals")

        started = time.perf_counter()
        nodes.append(_add_namespace(3))
        _start(nodes[-1])
        moved = stream_cluster.reshard([node.mirror for node in nodes[:-1]], 4)
        for index, state in enumerate(moved):
            current = nodes[index].mirror
            delta = []
            for key in set(current) | set(state):
                change = int(state.get(key, 0)) - int(current.get(key, 0))
                if change:
                    delta.append((key, change))
            if delta:
                sent, _elapsed = _push(nodes[index], delta)
                nbytes += sent
            nodes[index].mirror = dict(state)
        scale_seconds = time.perf_counter() - started
        if _totals(nodes) != expected:
            raise AssertionError("scale changed the totals")
        print(
            "flight_netns nodes=3 scale_nodes=4 rows=200 sum=%d bytes=%d "
            "ping_ms=%.3f exchange_seconds=%.6f rtt_seconds=%.6f "
            "failover_seconds=%.6f scale_seconds=%.6f"
            % (
                sum(expected.values()), nbytes, ping_ms, exchange_seconds,
                rtt_seconds, failover_seconds, scale_seconds,
            ),
            flush=True)
    finally:
        _cleanup(nodes)
    print("flight_netns_test ok", flush=True)


if __name__ == "__main__":
    main()
