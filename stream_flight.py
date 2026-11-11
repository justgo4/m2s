#!/usr/bin/env python3
"""Workers exchange batches over Arrow Flight.

The coordinator keeps the shard it successfully sent. Scaling moves those
shards to a new worker count. Killing a worker replays its shard to a new
process. The takeover time is the kill plus the Flight reload.
"""
import multiprocessing as mp
import time

import pyarrow as pa
import pyarrow.flight as flight

import stream_cluster


class _Worker(flight.FlightServerBase):
    def __init__(self):
        super().__init__("grpc://127.0.0.1:0")
        self.state = {}
        self.batches = 0
        self.nbytes = 0

    def do_put(self, context, descriptor, reader, writer):
        table = reader.read_all()
        self.batches += 1
        self.nbytes += int(table.nbytes)
        for key, weight in zip(table.column("k").to_pylist(), table.column("w").to_pylist()):
            self.state[str(key)] = self.state.get(str(key), 0) + int(weight)

    def do_get(self, context, ticket):
        keys = list(self.state)
        weights = [self.state[key] for key in keys]
        return flight.RecordBatchStream(pa.table({"k": keys, "w": weights}))


def _serve(ready):
    server = _Worker()
    ready.put(int(server.port))
    server.serve()


def _push(client, rows):
    table = pa.table({
        "k": [str(key) for key, _weight in rows],
        "w": [int(weight) for _key, weight in rows],
    })
    writer, _meta = client.do_put(
        flight.FlightDescriptor.for_command(b"batch"), table.schema)
    writer.write_table(table)
    writer.close()
    return int(table.nbytes)


def _pull(client):
    table = client.do_get(flight.Ticket(b"state")).read_all()
    return {
        str(key): int(weight)
        for key, weight in zip(table.column("k").to_pylist(), table.column("w").to_pylist())
    }


class FlightCluster:
    def __init__(self, workers):
        self.ctx = mp.get_context("spawn")
        self.procs = []
        self.clients = []
        self.mirror = []
        self.nbytes = 0
        self.start(workers)

    def start(self, workers):
        self.stop()
        for _index in range(int(workers)):
            ready = self.ctx.Queue()
            proc = self.ctx.Process(target=_serve, args=(ready,))
            proc.start()
            port = ready.get(timeout=20)
            self.procs.append(proc)
            self.clients.append(flight.FlightClient("grpc://127.0.0.1:%d" % port))
            self.mirror.append({})

    def stop(self):
        for client in self.clients:
            client.close()
        for proc in self.procs:
            if proc.is_alive():
                proc.kill()
            proc.join(5)
        self.procs = []
        self.clients = []
        self.mirror = []

    def ingest(self, rows):
        buckets = [[] for _slot in self.procs]
        for key, weight in rows:
            buckets[stream_cluster.shard_of(key, len(self.procs))].append((key, int(weight)))
        for index, batch in enumerate(buckets):
            if not batch:
                continue
            self.nbytes += _push(self.clients[index], batch)
            for key, weight in batch:
                slot = self.mirror[index]
                slot[str(key)] = slot.get(str(key), 0) + int(weight)

    def totals(self):
        merged = {}
        for client in self.clients:
            for key, weight in _pull(client).items():
                merged[key] = merged.get(key, 0) + weight
        return merged

    def scale(self, workers):
        started = time.perf_counter()
        moved = stream_cluster.reshard(self.mirror, workers)
        self.start(workers)
        for index, state in enumerate(moved):
            rows = [(key, weight) for key, weight in state.items() if weight]
            if rows:
                self.nbytes += _push(self.clients[index], rows)
            self.mirror[index] = dict(state)
        return time.perf_counter() - started

    def failover(self, index):
        started = time.perf_counter()
        state = dict(self.mirror[index])
        self.clients[index].close()
        self.procs[index].kill()
        self.procs[index].join(10)
        ready = self.ctx.Queue()
        proc = self.ctx.Process(target=_serve, args=(ready,))
        proc.start()
        port = ready.get(timeout=20)
        client = flight.FlightClient("grpc://127.0.0.1:%d" % port)
        rows = [(key, weight) for key, weight in state.items() if weight]
        if rows:
            self.nbytes += _push(client, rows)
        self.procs[index] = proc
        self.clients[index] = client
        self.mirror[index] = state
        return time.perf_counter() - started
