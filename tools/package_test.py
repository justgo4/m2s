#!/usr/bin/env python3
"""Render the Helm chart, build the image, and pack the offline tarball."""
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def _run(command):
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise AssertionError(proc.stderr[-800:] or proc.stdout[-800:])
    return proc.stdout


def main():
    helm = os.environ.get("HELM_BIN", "/tmp/linux-amd64/helm")
    rendered = _run([helm, "template", "m2s", str(ROOT / "deploy" / "helm" / "m2s")])
    if "m2s:synthetic" not in rendered or "password" in rendered.lower():
        raise AssertionError(rendered[:500])
    image = _run([
        "sudo", "docker", "build", "-t", "m2s:synthetic", str(ROOT / "deploy")])
    inspected = _run([
        "sudo", "docker", "image", "inspect", "m2s:synthetic",
        "--format", "{{.Id}} {{.Size}}"])
    image_id, image_size = inspected.split()
    with tempfile.TemporaryDirectory(prefix="m2s-offline-") as directory:
        bundle = Path(directory) / "m2s-offline.tgz"
        with tarfile.open(bundle, "w:gz") as tar:
            tar.add(ROOT / "deploy", arcname="deploy")
        names = subprocess.run(
            ["tar", "-tzf", str(bundle)], capture_output=True, text=True, check=True).stdout
        if "deploy/synthetic.env" not in names or "deploy/Dockerfile" not in names:
            raise AssertionError(names)
        size = bundle.stat().st_size
    print(
        "package_test ok helm_bytes=%d image=%s image_bytes=%s offline_bytes=%d"
        % (len(rendered.encode()), image_id, image_size, size),
        flush=True)
    if image.strip():
        pass


if __name__ == "__main__":
    main()
