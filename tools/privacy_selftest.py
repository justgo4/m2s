#!/usr/bin/env python3
"""Synthetic checks that the public scanner rejects sensitive publication shapes."""
import base64
import gzip
from pathlib import Path
import tempfile

from privacy_check import review


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        candidate = Path("candidate.py")
        secret = "not" + "_a_real_secret"
        (root / candidate).write_text("settings = " + repr(dict(password=secret)) + "\n")
        assert review(root, [candidate]), "hardcoded credential was accepted"
        (root / candidate).write_text("settings = " + repr(dict(password="CHANGE_ME_PASSWORD")) + "\n")
        assert not review(root, [candidate]), "placeholder was rejected"
        address = ".".join(("192", "0", "2", "1"))
        (root / candidate).write_text("host = " + repr(address) + "\n")
        assert review(root, [candidate]), "embedded address was accepted"
        (root / candidate).write_text("host = '127.0.0.1'\n")
        assert not review(root, [candidate]), "loopback was rejected"
        (root / candidate).write_text("value = " + repr("gh" + "p_" + "x" * 40) + "\n")
        assert review(root, [candidate]), "credential token shape was accepted"
        (root / candidate).write_text("value = 'local-only-marker'\n")
        assert review(root, [candidate], ("local-only-marker",)), "private marker was accepted"
        payload = Path("payload.gz.b64")
        (root / payload).write_bytes(base64.b64encode(gzip.compress(b"safe /" + b"world/deployment")))
        assert review(root, [payload]), "deployment path in bundle was accepted"
        (root / "setup.sql").write_text("SELECT 1;\n")
        assert review(root, [Path("setup.sql")]), "local deployment file was accepted"
    print("PRIVACY SELFTEST PASS cases=8", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
