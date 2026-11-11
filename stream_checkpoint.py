#!/usr/bin/env python3
"""Barrier checkpoints and savepoints for the stream state backend.

A completed checkpoint cuts the source offset, the state bytes, and the
output epoch together. A crash after prepare and before commit restores the
previous completed checkpoint. Replaying an output epoch that was already
committed does not append it again.
"""
import json

import state_backend


def _copy_items(backend):
    return [(bytes(key), bytes(value)) for key, value in backend.items()]


def _load_items(backend, items):
    for key, _value in list(backend.items()):
        backend.delete(key)
    for key, value in items:
        backend.put(key, value)


class CheckpointCoordinator:
    def __init__(self, backend):
        self.backend = backend
        self.completed = {}
        self.pending = None
        self.latest = None
        self.savepoints = {}
        self.outputs = []
        self.output_epoch = 0

    def start_barrier(self, barrier_id, source_offset):
        barrier_id = int(barrier_id)
        source_offset = int(source_offset)
        if barrier_id in self.completed:
            return dict(self.completed[barrier_id], duplicate=True)
        if self.pending is not None and self.pending["barrier_id"] == barrier_id:
            return dict(self.pending, duplicate=True)
        if self.latest is not None and source_offset < self.latest["source_offset"]:
            raise ValueError("checkpoint source offset moved backwards")
        self.pending = dict(
            barrier_id=barrier_id,
            source_offset=source_offset,
            output_epoch=self.output_epoch,
            items=_copy_items(self.backend),
            phase="prepared",
        )
        return dict(self.pending, duplicate=False)

    def commit_barrier(self, barrier_id):
        barrier_id = int(barrier_id)
        if barrier_id in self.completed:
            return dict(self.completed[barrier_id], duplicate=True)
        if self.pending is None or self.pending["barrier_id"] != barrier_id:
            raise ValueError("no prepared barrier " + str(barrier_id))
        done = dict(self.pending, phase="completed")
        self.completed[barrier_id] = done
        self.latest = done
        self.pending = None
        return dict(done, duplicate=False)

    def abort_pending(self):
        """A crash drops the prepared barrier. The last commit stays current."""
        self.pending = None
        return None if self.latest is None else dict(self.latest)

    def restore(self):
        if self.latest is None:
            raise ValueError("no completed checkpoint")
        _load_items(self.backend, self.latest["items"])
        self.output_epoch = int(self.latest["output_epoch"])
        self.pending = None
        return dict(self.latest)

    def emit(self, epoch, row):
        epoch = int(epoch)
        if epoch <= self.output_epoch:
            return False
        if epoch != self.output_epoch + 1:
            raise ValueError("output epoch skipped")
        self.outputs.append((epoch, row))
        self.output_epoch = epoch
        return True

    def savepoint(self, name):
        if self.latest is None:
            raise ValueError("savepoint requires a completed checkpoint")
        self.savepoints[str(name)] = dict(self.latest)
        return str(name)

    def restore_savepoint(self, name):
        key = str(name)
        if key not in self.savepoints:
            raise ValueError("unknown savepoint " + key)
        chosen = dict(self.savepoints[key])
        _load_items(self.backend, chosen["items"])
        self.latest = chosen
        self.output_epoch = int(chosen["output_epoch"])
        self.pending = None
        return chosen

    def export_bytes(self):
        payload = dict(
            latest=self.latest["barrier_id"] if self.latest else None,
            completed={
                str(key): _public(value)
                for key, value in self.completed.items()
            },
            savepoints={
                key: value["barrier_id"]
                for key, value in self.savepoints.items()
            },
            output_epoch=self.output_epoch,
        )
        return json.dumps(payload, sort_keys=True).encode("utf-8")


def _public(checkpoint):
    return dict(
        barrier_id=checkpoint["barrier_id"],
        source_offset=checkpoint["source_offset"],
        output_epoch=checkpoint["output_epoch"],
        items=len(checkpoint["items"]),
        phase=checkpoint["phase"],
    )


def open_checkpoint(kind, path):
    return CheckpointCoordinator(state_backend.open_state_backend(kind, path))
