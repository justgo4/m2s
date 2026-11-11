#!/usr/bin/env python3
"""Kafka produce and consume match, and a committed group does not replay."""
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import kafka_connector


def main():
    topic = "m2s_" + uuid.uuid4().hex[:8]
    sent = ["alpha", "beta", "gamma"]
    kafka_connector.create_topic(topic)
    kafka_connector.produce(topic, sent)
    read = kafka_connector.consume(topic, len(sent))
    if read != sent:
        raise AssertionError(dict(sent=sent, read=read))
    group = "m2s_group_" + topic
    first = kafka_connector.consume(topic, len(sent), group=group)
    if first != sent:
        raise AssertionError(dict(group=first))
    try:
        second = kafka_connector.consume(topic, 1, group=group, timeout=8)
    except subprocess.TimeoutExpired:
        second = []
    if second:
        raise AssertionError("committed offsets were replayed: %s" % second)
    print(
        "kafka_connector_test ok semantic=%s records=%d topic=%s"
        % (kafka_connector.SEMANTIC, len(sent), topic),
        flush=True)


if __name__ == "__main__":
    main()
