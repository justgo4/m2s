#!/usr/bin/env python3
"""Review tracked public files without printing any matched secret values."""
import argparse
import ast
import base64
import gzip
import hashlib
import ipaddress
import os
from pathlib import Path
import re
import subprocess


SECRET_KEYS = {"password", "passwd", "secret", "token", "api_key"}
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{30,}\b"),
    re.compile(r"\bAKIA[A-Z0-9]{16}\b"),
    re.compile(r"https?://[^\s/@]+:[^\s/@]+@"),
)
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")


def tracked_files(root):
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True)
    return [Path(os.fsdecode(item)) for item in result.stdout.split(b"\0") if item]


def secret_key(value):
    value = value.lower()
    return value in SECRET_KEYS or value.endswith("_password")


def placeholder(value, path):
    if value == "" or value.startswith("CHANGE_ME_"):
        return True
    return path.name == "cdc_selftest.py" and value in {"p", "test"}


def python_credentials(text, path):
    findings = []
    tree = ast.parse(text, filename=str(path))
    for node in ast.walk(tree):
        pairs = []
        if isinstance(node, ast.Dict):
            pairs = [(key.value, value) for key, value in zip(node.keys, node.values)
                     if isinstance(key, ast.Constant) and isinstance(key.value, str)]
        elif isinstance(node, ast.Call):
            pairs = [(item.arg, item.value) for item in node.keywords if item.arg]
        elif isinstance(node, ast.Assign):
            pairs = [(target.id, node.value) for target in node.targets
                     if isinstance(target, ast.Name)]
        for key, value in pairs:
            if (secret_key(key) and isinstance(value, ast.Constant)
                    and isinstance(value.value, str) and not placeholder(value.value, path)):
                findings.append((value.lineno, "literal credential"))
    return findings


def scan_text(text, path, forbidden):
    findings = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if any(pattern.search(line) for pattern in SECRET_PATTERNS):
            findings.append((line_no, "credential/key pattern"))
        if any(value.casefold() in line.casefold() for value in forbidden):
            findings.append((line_no, "private marker"))
        for match in IPV4.finditer(line):
            try:
                address = ipaddress.ip_address(match.group())
            except ValueError:
                continue
            if not address.is_loopback and not address.is_unspecified:
                findings.append((line_no, "embedded network address"))
        sql_secret = re.search(
            r"(?i)\b(?:password|\w+_password)\s*=\s*'([^']*)'", line)
        if sql_secret and not placeholder(sql_secret.group(1), path):
            findings.append((line_no, "literal SQL credential"))
    return findings


def review(root, paths, forbidden=()):
    findings = []
    for path in paths:
        name = path.as_posix()
        if (path.is_absolute() or ".." in path.parts or (root / path).is_symlink()):
            findings.append((name, 0, "unsafe publication path"))
            continue
        if (path.name in {"setup.sql", ".env", "core"}
                or path.name.startswith((".env.", "core."))
                or any(part in {"runtime", "secrets", "credentials"} for part in path.parts)
                or re.search(r"\.(?:sqlite3|db|log|jsonl)(?:[.-].*)?$", path.name)):
            findings.append((name, 0, "runtime/private file"))
            continue
        payload = (root / path).read_bytes()
        if name.endswith(".gz.b64"):
            decoded = gzip.decompress(base64.b64decode(payload, validate=True))
            text = "\n".join(item.decode("ascii") for item in re.findall(rb"[ -~]{5,}", decoded))
            findings.extend((name, line, why) for line, why in scan_text(text, path, forbidden))
            if re.search(rb"/(?:home|world|workspace)/", decoded):
                findings.append((name, 0, "build/deployment path in binary"))
            continue
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            expected = REVIEWED_BINARIES.get(name)
            actual = hashlib.sha256(payload).hexdigest()
            if expected != actual:
                findings.append((name, 0, "unreviewed binary file"))
            continue
        findings.extend((name, line, why) for line, why in scan_text(text, path, forbidden))
        if path.suffix == ".py":
            findings.extend((name, line, why) for line, why in python_credentials(text, path))
    return sorted(set(findings))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    root = args.root.resolve()
    paths = tracked_files(root)
    if not paths:
        raise SystemExit("privacy check requires a nonempty tracked publication set")
    # Optional local-only markers are never included in diagnostics or artifacts.
    forbidden = tuple(item for item in os.environ.get("M2S_FORBIDDEN_STRINGS", "").splitlines() if item)
    findings = review(root, paths, forbidden)
    for path, line, why in findings:
        print(f"PRIVACY FAIL {path}:{line}: {why}", flush=True)
    print(f"PRIVACY {'FAIL' if findings else 'PASS'} files={len(paths)} findings={len(findings)}", flush=True)
    return int(bool(findings))


if __name__ == "__main__":
    raise SystemExit(main())
