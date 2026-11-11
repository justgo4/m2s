#!/usr/bin/env python3
"""Produce and consume records on a real Kafka broker.

The measured grade is at_least_once: a consumer group that has committed
its offsets does not see those records again. The log itself still returns
them to a new group that starts at the beginning.
"""
import os
import subprocess


SEMANTIC = "at_least_once"


def _bin(name):
    home = os.environ.get("KAFKA_HOME", "/tmp/kafka_2.13-4.1.2")
    return os.path.join(home, "bin", name)


def _bootstrap():
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def create_topic(topic):
    subprocess.run(
        [_bin("kafka-topics.sh"), "--create", "--if-not-exists",
         "--topic", topic, "--bootstrap-server", _bootstrap(),
         "--partitions", "1", "--replication-factor", "1"],
        check=True, capture_output=True, text=True)


def produce(topic, records):
    subprocess.run(
        [_bin("kafka-console-producer.sh"), "--topic", topic,
         "--bootstrap-server", _bootstrap()],
        input="".join(str(record) + "\n" for record in records),
        text=True, check=True, capture_output=True)


def consume(topic, count, group=None, timeout=20):
    command = [
        _bin("kafka-console-consumer.sh"), "--topic", topic,
        "--bootstrap-server", _bootstrap(),
        "--max-messages", str(int(count)),
    ]
    if group is None:
        command.append("--from-beginning")
    else:
        command.extend(["--group", group, "--from-beginning"])
    proc = subprocess.run(
        command, capture_output=True, text=True, timeout=timeout, check=False)
    lines = [line for line in proc.stdout.splitlines() if line and not line.startswith("WARNING")]
    if proc.returncode not in (0, None) and len(lines) < count and group is None:
        raise RuntimeError(proc.stderr[-500:])
    return lines
