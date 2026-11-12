#!/usr/bin/env python3
"""Build an offline install bundle outside the git tree.

The tarball is not committed. It contains the pinned Python wheels, the
native reader when it has already been built, config templates, the
synthetic application tree, and an install script that refuses the index.
"""
from pathlib import Path
import argparse
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]

OPTIONAL = (
    "rocksdict==0.3.29",
    "minio==7.2.20",
    "psycopg2-binary==2.9.9",
)

INSTALL = """#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
export PIP_NO_INDEX=1
export PIP_DISABLE_PIP_VERSION_CHECK=1
python3 -m venv "$ROOT/venv"
"$ROOT/venv/bin/pip" install --no-index --find-links "$ROOT/wheels" \\
    -r "$ROOT/requirements.txt"
mkdir -p "$ROOT/app/build/native"
if [ -d "$ROOT/native" ]; then
    cp -a "$ROOT/native/." "$ROOT/app/build/native/"
fi
echo "offline_install ok" 
"""


def _run(command, **kwargs):
    proc = subprocess.run(command, check=False, text=True, **kwargs)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)
    return proc


def _requirements():
    lines = []
    seen = set()
    for raw in (ROOT / "requirements.txt").read_text().splitlines():
        text = raw.strip()
        if not text or text.startswith("#"):
            continue
        lines.append(text)
        seen.add(text.split("==", 1)[0].lower())
    for item in OPTIONAL:
        name = item.split("==", 1)[0].lower()
        if name not in seen and name.replace("-", "_") not in seen:
            lines.append(item)
    return "\n".join(lines) + "\n"


def _copy_native(destination):
    found = []
    destination.mkdir(parents=True)
    reader = ROOT / "build" / "native" / "mysql_arrow_reader"
    if reader.is_file():
        shutil.copy2(reader, destination / "mysql_arrow_reader")
        found.append("mysql_arrow_reader")
    for candidate in (
        ROOT / "build" / "native" / "stream_hot_rs.so",
        Path("/tmp/stream_hot_rs.so"),
    ):
        if candidate.is_file():
            shutil.copy2(candidate, destination / "stream_hot_rs.so")
            found.append("stream_hot_rs.so")
            break
    return found


def build(tarball):
    tarball = Path(tarball).resolve()
    if ROOT in tarball.parents or tarball == ROOT:
        raise SystemExit("refusing to write the bundle inside the repository")
    requirements = _requirements()
    with tempfile.TemporaryDirectory(prefix="m2s-offline-src-") as scratch:
        stage = Path(scratch)
        (stage / "requirements.txt").write_text(requirements)
        (stage / "install.sh").write_text(INSTALL)
        os.chmod(stage / "install.sh", 0o755)
        wheels = stage / "wheels"
        wheels.mkdir()
        _run([
            sys.executable, "-m", "pip", "download",
            "--only-binary=:all:",
            "-r", str(stage / "requirements.txt"),
            "-d", str(wheels),
        ])
        app = stage / "app"
        app.mkdir()
        archive = subprocess.run(
            ["git", "archive", "--format=tar", "HEAD"],
            cwd=ROOT, check=True, capture_output=True)
        subprocess.run(
            ["tar", "-x", "-C", str(app)],
            input=archive.stdout, check=True)
        native = _copy_native(stage / "native")
        templates = stage / "templates"
        shutil.copytree(ROOT / "deploy", templates / "deploy")
        (stage / "README.txt").write_text(
            "Offline bundle. Install with ./install.sh inside a network "
            "namespace. Wheels, the native reader, and deploy templates "
            "are in this directory. No database passwords are included.\n")
        tarball.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(tarball, "w:gz") as tar:
            tar.add(stage, arcname="m2s-offline")
        wheel_files = sorted(path.name for path in wheels.iterdir())
    size = tarball.stat().st_size
    print(
        "offline_bundle path=%s bytes=%d wheels=%d native=%s"
        % (tarball, size, len(wheel_files), ",".join(native) or "none"),
        flush=True)
    for name in wheel_files:
        print("offline_wheel %s" % name, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tarball")
    args = parser.parse_args()
    build(args.tarball)


if __name__ == "__main__":
    main()
