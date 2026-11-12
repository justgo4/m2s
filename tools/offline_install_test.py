#!/usr/bin/env python3
"""Install the offline bundle with the network namespace disconnected.

The bundle is built under /tmp and is not committed. Installation and
`j4.py selftest` run inside `unshare -n`, after a probe that must fail
to reach the network.
"""
from pathlib import Path
import os
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


PROBE = """import urllib.request
try:
    urllib.request.urlopen("https://pypi.org", timeout=5)
except Exception as exc:
    print("network-blocked " + type(exc).__name__, flush=True)
else:
    raise SystemExit("network is reachable")
"""


def _run(command, **kwargs):
    proc = subprocess.run(command, check=False, text=True, **kwargs)
    if proc.returncode != 0:
        detail = (proc.stderr or "")[-1200:] + (proc.stdout or "")[-1200:]
        raise AssertionError(detail or ("exit %s" % proc.returncode))
    return proc


def main():
    if os.geteuid() == 0:
        raise SystemExit("run the test as the unprivileged user")
    parent = Path("/tmp")
    with tempfile.TemporaryDirectory(prefix="m2s-offline-test-", dir=parent) as scratch:
        scratch = Path(scratch)
        tarball = scratch / "m2s-offline.tgz"
        _run([
            sys.executable, str(ROOT / "tools" / "make_offline_bundle.py"),
            str(tarball),
        ])
        extract = scratch / "extract"
        extract.mkdir()
        with tarfile.open(tarball, "r:gz") as tar:
            tar.extractall(extract)
        bundle = extract / "m2s-offline"
        if not (bundle / "install.sh").is_file():
            raise AssertionError("install.sh missing")
        if not (bundle / "wheels").is_dir() or not any((bundle / "wheels").iterdir()):
            raise AssertionError("wheels missing")
        if not (bundle / "native" / "mysql_arrow_reader").is_file():
            raise AssertionError("native reader missing")
        if not (bundle / "templates" / "deploy" / "synthetic.env").is_file():
            raise AssertionError("config template missing")
        probe = scratch / "probe.py"
        probe.write_text(PROBE)
        runner = scratch / "run.sh"
        runner.write_text(
            "#!/bin/sh\nset -eu\n"
            "python3 %s\n%s\ncd %s\n%s j4.py selftest\n"
            % (
                sh_quote(probe),
                sh_quote(bundle / "install.sh"),
                sh_quote(bundle / "app"),
                sh_quote(bundle / "venv" / "bin" / "python"),
            ))
        os.chmod(runner, 0o755)
        user = subprocess.check_output(["id", "-un"], text=True).strip()
        # Loopback stays up so the self-test can bind 127.0.0.1. There is
        # no default route, so the earlier probe cannot reach an index.
        proc = _run([
            "sudo", "-n", "unshare", "-n", "sh", "-c",
            "ip link set lo up && runuser -u %s -- sh %s"
            % (sh_quote(user), sh_quote(runner)),
        ], capture_output=True)
        output = proc.stdout
        if "network-blocked" not in output:
            raise AssertionError(output[-800:])
        if "offline_install ok" not in output:
            raise AssertionError(output[-800:])
        if "SELFTEST OK:" not in output:
            raise AssertionError(output[-800:])
        size = tarball.stat().st_size
        wheels = sorted(path.name for path in (bundle / "wheels").iterdir())
        print(output, end="" if output.endswith("\n") else "\n", flush=True)
        print(
            "offline_install_test ok bytes=%d wheels=%d native_bytes=%d"
            % (
                size,
                len(wheels),
                (bundle / "native" / "mysql_arrow_reader").stat().st_size,
            ),
            flush=True)


def sh_quote(path):
    text = str(path)
    return "'" + text.replace("'", "'\"'\"'") + "'"


if __name__ == "__main__":
    main()
