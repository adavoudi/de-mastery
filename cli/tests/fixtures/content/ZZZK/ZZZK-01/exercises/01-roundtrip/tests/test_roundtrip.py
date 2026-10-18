"""Tests for the Kafka round-trip self-test exercise.

Uses the native `kafka_broker` fixture (no Docker). Each test creates a UNIQUE
topic so parallel/shuffled runs never interfere. The oracle (G6) is an
independent Python tally of the same records.
"""

import uuid

import pytest


def _unique_topic():
    return f"selftest-{uuid.uuid4().hex[:8]}"


def _records():
    # keys a, b, c with known multiplicities: a=3, b=2, c=1
    return [
        ("a", "1"), ("b", "2"), ("a", "3"), ("c", "4"), ("a", "5"), ("b", "6"),
    ]


def test_counts_match_oracle(kafka_broker, submission):
    topic = _unique_topic()
    kafka_broker.create_topic(topic, partitions=3, replication_factor=1)
    records = _records()

    result = submission.publish_and_count(kafka_broker.bootstrap_servers, topic,
                                          records)

    # independent oracle: tally by key in pure Python
    oracle = {}
    for k, _ in records:
        oracle[k] = oracle.get(k, 0) + 1
    assert result == oracle


def test_total_messages_read(kafka_broker, submission):
    topic = _unique_topic()
    kafka_broker.create_topic(topic, partitions=2, replication_factor=1)
    records = _records()
    result = submission.publish_and_count(kafka_broker.bootstrap_servers, topic,
                                          records)
    assert sum(result.values()) == len(records)


def test_single_key(kafka_broker, submission):
    topic = _unique_topic()
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    records = [("solo", str(i)) for i in range(4)]
    result = submission.publish_and_count(kafka_broker.bootstrap_servers, topic,
                                          records)
    assert result == {"solo": 4}
