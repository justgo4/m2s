#!/usr/bin/env python3
"""Single-machine cluster: hash shards, Arrow Flight, backpressure, failover.

This is a local multi-process layout. It is not a distributed SLO. strict-small
numbers stay single-node measurements.
"""
import hashlib
import multiprocessing as mp
import queue

import pyarrow as pa
import pyarrow.flight as flight


def shard_of(key, workers):
    workers = int(workers)
    if workers < 1:
        raise ValueError("worker count must be positive")
    digest = hashlib.sha256(str(key).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % workers


def preaggregate(rows):
    totals = {}
    for key, weight in rows:
        if isinstance(weight, bool) or not isinstance(weight, int):
            raise TypeError("weight must be an int")
        totals[key] = totals.get(key, 0) + int(weight)
    return [(key, totals[key]) for key in sorted(totals) if totals[key] != 0]


def reshard(states, workers):
    moved = [dict() for _ in range(int(workers))]
    seen = set()
    for state in states:
        for key, value in state.items():
            if key in seen:
                raise RuntimeError("key is owned by two shards")
            seen.add(key)
            moved[shard_of(key, workers)][key] = value
    return moved


def failover(states, dead_index):
    if not 0 <= int(dead_index) < len(states):
        raise ValueError("dead worker is out of range")
    survivors = [dict(state) for index, state in enumerate(states) if index != dead_index]
    if not survivors:
        raise RuntimeError("no survivor for failover")
    for key, value in states[int(dead_index)].items():
        survivors[shard_of(key, len(survivors))][key] = value
    return survivors


def _compute_main(inbox, outbox):
    totals = {}
    while True:
        item = inbox.get()
        if item is None:
            break
        key, weight = item
        totals[key] = totals.get(key, 0) + int(weight)
    outbox.put(totals)


def run_sharded(rows, workers, mailbox_limit):
    """Run one source and N compute processes. A full mailbox counts as backpressure."""
    workers = int(workers)
    mailbox_limit = int(mailbox_limit)
    if workers < 1 or mailbox_limit < 1:
        raise ValueError("workers and mailbox_limit must be positive")
    ctx = mp.get_context("spawn")
    inboxes = [ctx.Queue(mailbox_limit) for _ in range(workers)]
    outbox = ctx.Queue()
    procs = []
    for inbox in inboxes:
        proc = ctx.Process(target=_compute_main, args=(inbox, outbox))
        proc.start()
        procs.append(proc)
    blocked = 0
    for key, weight in rows:
        slot = shard_of(key, workers)
        while True:
            try:
                inboxes[slot].put((key, int(weight)), timeout=0.05)
                break
            except queue.Full:
                blocked += 1
    for inbox in inboxes:
        inbox.put(None)
    merged = {}
    for _worker in range(workers):
        part = outbox.get(timeout=30)
        for key, weight in part.items():
            merged[key] = merged.get(key, 0) + weight
    for proc in procs:
        proc.join(10)
        if proc.exitcode != 0:
            raise RuntimeError("compute process failed")
    for inbox in inboxes:
        inbox.close()
    outbox.close()
    return merged, blocked


class _FlightStore(flight.FlightServerBase):
    def __init__(self, location, limit_bytes):
        super().__init__(location)
        self.limit_bytes = int(limit_bytes)
        self.tables = {}
        self.rejected = 0

    def do_put(self, context, descriptor, reader, writer):
        table = reader.read_all()
        if table.nbytes > self.limit_bytes:
            self.rejected += 1
            raise flight.FlightServerError("backpressure: batch exceeds the exchange limit")
        key = descriptor.command.decode("utf-8")
        self.tables[key] = table

    def do_get(self, context, ticket):
        key = ticket.ticket.decode("utf-8")
        if key not in self.tables:
            raise flight.FlightServerError("unknown ticket")
        return flight.RecordBatchStream(self.tables[key])


def exchange_table(table, limit_bytes):
    """Put a table through a local Arrow Flight server and read it back."""
    server = _FlightStore("grpc://127.0.0.1:0", limit_bytes)
    client = flight.FlightClient("grpc://127.0.0.1:%d" % server.port)
    try:
        writer, _meta = client.do_put(
            flight.FlightDescriptor.for_command(b"batch"),
            table.schema)
        writer.write_table(table)
        writer.close()
        reader = client.do_get(flight.Ticket(b"batch"))
        return reader.read_all(), server.rejected
    finally:
        client.close()
        server.shutdown()
