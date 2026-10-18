"""Tests for KFK-01 replay-and-rebuild.

Uses the native session ``kafka_broker`` (no Docker). Each test creates a UNIQUE
topic so shuffled/parallel runs never interfere. The oracle (G6) is an
independent pure-Python fold over the same events.

We produce with EXPLICIT partitions and EXPLICIT timestamps so the log layout is
deterministic and the offset-/timestamp-replay boundaries are known exactly.
"""

import time
import uuid

import pytest


def _topic():
    return f"account-balances-{uuid.uuid4().hex[:8]}"


# A fixed event script: (partition, offset-within-partition order, key, value, ts_ms).
# Base timestamp is injected per test so timestamps are monotonic within a
# partition. Keys repeat so "last value wins" matters; the boundary record for
# the timestamp test is the latest value of key "a".
def _events(base):
    # IMPORTANT: each key lives on exactly ONE partition, so the folded result
    # is deterministic regardless of cross-partition poll interleaving (Kafka
    # guarantees order only WITHIN a partition).
    # partition 0: a=1 (t0), a=2 (t2), b=5 (t4)   -> a->"2", b->"5"
    # partition 1: c=7 (t1), c=8 (t3), d=4 (t5)   -> c->"8", d->"4"
    return [
        (0, "a", "1", base + 0),
        (1, "c", "7", base + 100),
        (0, "a", "2", base + 200),
        (1, "c", "8", base + 300),
        (0, "b", "5", base + 400),
        (1, "d", "4", base + 500),
    ]


def _produce(kafka_broker, topic, events):
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": kafka_broker.bootstrap_servers})
    for part, key, val, ts in events:
        p.produce(topic, key=key, value=val, partition=part, timestamp=ts)
    p.flush(15)


def _oracle(events, *, start_offsets=None, since_ms=None):
    """Independent fold. events are in production order; per partition that order
    is also offset order (we produce them that way)."""
    # assign per-partition offsets in production order
    by_part = {}
    enriched = []
    for part, key, val, ts in events:
        off = by_part.get(part, 0)
        by_part[part] = off + 1
        enriched.append((part, off, key, val, ts))
    table = {}
    for part in sorted({e[0] for e in enriched}):
        for p, off, key, val, ts in enriched:
            if p != part:
                continue
            if start_offsets is not None and off < start_offsets.get(part, 0):
                continue
            if since_ms is not None and ts < since_ms:
                continue
            table[key] = val
    return table


def test_full_replay_from_zero(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=2, replication_factor=1)
    base = int(time.time() * 1000)
    events = _events(base)
    _produce(kafka_broker, topic, events)

    start = {0: 0, 1: 0}
    result = submission.rebuild_from_offset(
        kafka_broker.bootstrap_servers, topic, start)
    assert result == _oracle(events, start_offsets=start)


def test_partial_replay_from_offset(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=2, replication_factor=1)
    base = int(time.time() * 1000)
    events = _events(base)
    _produce(kafka_broker, topic, events)

    # Start p0 at offset 1 (skip a=1), p1 at offset 1 (skip a=9).
    start = {0: 1, 1: 1}
    result = submission.rebuild_from_offset(
        kafka_broker.bootstrap_servers, topic, start)
    assert result == _oracle(events, start_offsets=start)


def test_partial_replay_drops_a_key(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    base = int(time.time() * 1000)
    # single partition: x=1, x=2, y=3  -> start at offset 2 drops x entirely
    events = [(0, "x", "1", base), (0, "x", "2", base + 10),
              (0, "y", "3", base + 20)]
    _produce(kafka_broker, topic, events)
    start = {0: 2}
    result = submission.rebuild_from_offset(
        kafka_broker.bootstrap_servers, topic, start)
    assert result == {"y": "3"}            # x is gone -> kills 'ignore-start'


def test_last_value_wins(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    base = int(time.time() * 1000)
    events = [(0, "k", "first", base), (0, "k", "mid", base + 10),
              (0, "k", "last", base + 20)]
    _produce(kafka_broker, topic, events)
    result = submission.rebuild_from_offset(
        kafka_broker.bootstrap_servers, topic, {0: 0})
    assert result == {"k": "last"}         # kills 'first-wins'


def test_replay_from_timestamp(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=2, replication_factor=1)
    base = int(time.time() * 1000)
    events = _events(base)
    _produce(kafka_broker, topic, events)

    # since = base+200: includes p0 a=2(t+200), b=5(t+400) and p1 c=7(t+300),
    # a=4(t+500). The boundary record on p0 is a=2 at exactly t+200 (>=), and on
    # p1 the first >= is c=7 at t+300. Dropping either boundary (off-by-one)
    # changes the result -> kills 'ts-off-by-one'.
    since = base + 200
    result = submission.rebuild_from_timestamp(
        kafka_broker.bootstrap_servers, topic, since)
    assert result == _oracle(events, since_ms=since)


def test_timestamp_boundary_is_inclusive(kafka_broker, submission):
    topic = _topic()
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    base = int(time.time() * 1000)
    # one key whose ONLY event is exactly at the boundary; must be included.
    events = [(0, "old", "0", base), (0, "z", "9", base + 1000)]
    _produce(kafka_broker, topic, events)
    result = submission.rebuild_from_timestamp(
        kafka_broker.bootstrap_servers, topic, base + 1000)
    assert result == {"z": "9"}            # inclusive >= ; kills 'ts-off-by-one'
