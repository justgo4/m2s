#!/usr/bin/env python3
"""Kill the process after prepare and prove the pending cut is not published."""
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import stream_durable


def main():
    with tempfile.TemporaryDirectory(prefix="m2s-kill-") as directory:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "stream_durable.py"), "crash", directory],
            capture_output=True, text=True)
        if proc.returncode != -9:
            raise AssertionError(
                "child was not SIGKILL, code=%s stderr=%s" % (proc.returncode, proc.stderr))
        if not (Path(directory) / "prepared").exists():
            raise AssertionError("child died before prepare")
        if not (Path(directory) / "pending.json").exists():
            raise AssertionError("pending cut was not durable")
        restored = stream_durable.DurableCheckpoint(directory)
        try:
            if restored.backend.get(b"a") != b"1":
                raise AssertionError(restored.backend.get(b"a"))
            if restored.output_epoch != 1:
                raise AssertionError(restored.output_epoch)
            if restored.emit(1, {"id": 1}) is not False:
                raise AssertionError("committed epoch was applied twice")
            if restored.emit(2, {"id": 2}) is not True:
                raise AssertionError("next epoch was rejected")
            if restored.emit(2, {"id": 2}) is not False:
                raise AssertionError("replayed epoch was applied twice")
        finally:
            restored.close()
    print("stream_durable_test ok signal=SIGKILL restored_epoch=1", flush=True)


if __name__ == "__main__":
    main()
