#!/usr/bin/env python3
"""Read aggregate runner availability without publishing host names or labels."""
import argparse
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.request


def inventory(repository, token):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository or ""):
        raise ValueError("repository must be owner/name")
    result = dict(format_version=1, repository=repository, accessible=False,
                  availability_known=False, total_registered=None)
    if not token:
        return dict(result, reason="token_not_configured")
    request = urllib.request.Request(
        "https://api.github.com/repos/" + repository + "/actions/runners?per_page=100",
        headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28"})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read(1024*1024))
    except urllib.error.HTTPError as exc:
        # Do not dump response bodies, credentials or any private host metadata.
        return dict(result, reason="management_api_unavailable", http_status=exc.code)
    except (OSError, ValueError) as exc:
        return dict(result, reason="network_or_response_error")
    total = payload.get("total_count") if isinstance(payload, dict) else None
    runners = payload.get("runners") if isinstance(payload, dict) else None
    if not isinstance(total, int) or isinstance(total, bool) or total < 0 or not isinstance(runners, list):
        return dict(result, reason="unexpected_response")
    if any(not isinstance(row, dict) for row in runners):
        return dict(result, reason="unexpected_response")
    return dict(result, accessible=True, availability_known=True, reason="queried",
                total_registered=total, listed=len(runners), complete=len(runners) == total,
                listed_online_idle=sum(row.get("status") == "online" and row.get("busy") is False
                                       for row in runners))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = inventory(args.repository, os.environ.get("GITHUB_TOKEN"))
    text = json.dumps(value, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text, end="", flush=True)
    # Unavailable API is unknown availability, not a failed database contract.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
