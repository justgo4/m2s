#!/usr/bin/env python3
"""Write a Paimon table, replay a delivery, and reopen the warehouse."""
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import paimon_sink


def main():
    warehouse = Path("/tmp/m2s-paimon")
    shutil.rmtree(warehouse, ignore_errors=True)
    sink = paimon_sink.PaimonSink()
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
    snapshots = sink.snapshots()
    probe = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0, %r); import paimon_sink; "
         "sink = paimon_sink.PaimonSink(); "
         "print(sink.count()); print(int(sink.write('d1', []))); print(sink.snapshots())"
         % str(ROOT)],
        check=True, capture_output=True, text=True)
    count, applied, restored = [int(line) for line in probe.stdout.splitlines()]
    if count != 4 or applied != 0 or restored != snapshots:
        raise AssertionError((count, applied, restored, snapshots, probe.stdout))
    print(
        "paimon_sink rows=4 deliveries=2 replayed=0 restarted=1 snapshots=%d"
        % snapshots,
        flush=True)
    print("paimon_sink_test ok", flush=True)


if __name__ == "__main__":
    main()
