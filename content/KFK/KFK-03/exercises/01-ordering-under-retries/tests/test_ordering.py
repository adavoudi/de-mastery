"""Tests for KFK-03 ordering-under-retries.

Two layers:
  * A pure-config layer checks tune_for_throughput() against the rule oracle
    (no cluster) — this fails for the starter and passes for the solution, so
    G1/G2 are reachable even without Kafka.
  * A live layer produces an interleaved keyed stream to a MULTI-partition topic
    on the session broker and asserts per-key order is preserved and there are
    no duplicates. We never assert throughput numbers.
"""

import uuid

import pytest

_COMPRESSIONS = {"lz4", "zstd", "snappy", "gzip"}


# --------------------------------------------------------------------------- #
# config rule oracle (no cluster)
# --------------------------------------------------------------------------- #
def _check_config(conf):
    """Independent rule oracle: returns a list of violations (empty == valid)."""
    v = []
    if conf.get("enable.idempotence") is not True:
        v.append("enable.idempotence must be True")
    if str(conf.get("acks")) != "all":
        v.append("acks must be 'all'")
    mif = conf.get("max.in.flight.requests.per.connection")
    if not isinstance(mif, int) or mif > 5 or mif < 1:
        v.append("max.in.flight must be an int in 1..5")
    linger = conf.get("linger.ms")
    if not isinstance(linger, (int, float)) or linger <= 0:
        v.append("linger.ms must be positive")
    batch = conf.get("batch.size")
    if not isinstance(batch, int) or batch <= 16384:
        v.append("batch.size must be > 16384")
    if conf.get("compression.type") not in _COMPRESSIONS:
        v.append("compression.type must be one of lz4/zstd/snappy/gzip")
    return v


def test_config_is_valid(submission):
    conf = submission.tune_for_throughput()
    assert "bootstrap.servers" not in conf
    violations = _check_config(conf)
    assert violations == [], violations


def test_config_rules_spelled_out(submission):
    conf = submission.tune_for_throughput()
    # kills 'idempotence-off' and 'inflight-6' individually
    assert conf.get("enable.idempotence") is True
    assert conf.get("max.in.flight.requests.per.connection") <= 5
    assert str(conf.get("acks")) == "all"


# --------------------------------------------------------------------------- #
# live per-key ordering (multi-partition topic, session broker)
# --------------------------------------------------------------------------- #
def _consume_all(bootstrap_servers, topic, expected_total):
    import time

    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap_servers,
                  "group.id": f"kfk03-{uuid.uuid4().hex}",
                  "enable.auto.commit": False})
    md = c.list_topics(topic, timeout=10)
    parts = sorted(md.topics[topic].partitions.keys())
    highs = {}
    assigns = []
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        highs[p] = hi
        assigns.append(TopicPartition(topic, p, lo))
    c.assign(assigns)
    # Collect per-key value sequences in the order each key's records are seen
    # within its partition (poll returns records in offset order per partition).
    seqs = {}
    seen = 0
    target = sum(highs.values())
    deadline = time.time() + 20
    while seen < target and time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        key = m.key().decode() if m.key() is not None else None
        seqs.setdefault(key, []).append(m.value().decode())
        seen += 1
    c.close()
    return seqs, seen


def test_per_key_order_preserved(kafka_broker, submission):
    topic = f"orders-{uuid.uuid4().hex[:8]}"
    kafka_broker.create_topic(topic, partitions=4, replication_factor=1)

    # Three keys, interleaved; each key's values carry a monotonically
    # increasing index so order is machine-checkable.
    keys = ["a", "b", "c"]
    records = []
    for i in range(10):
        for k in keys:
            records.append((k, f"{k}-{i}"))
    submission.publish_ordered(kafka_broker.bootstrap_servers, topic, records)

    seqs, total = _consume_all(kafka_broker.bootstrap_servers, topic, len(records))
    assert total == len(records)                    # no loss, no duplicate count

    for k in keys:
        got = seqs.get(k)
        assert got is not None, f"key {k} missing"
        expected = [f"{k}-{i}" for i in range(10)]
        # Per-key order must match production order exactly. For the 'no-key'
        # mutant the values scatter across partitions and this sequence breaks.
        assert got == expected, (k, got)


def test_no_duplicates(kafka_broker, submission):
    topic = f"orders-{uuid.uuid4().hex[:8]}"
    kafka_broker.create_topic(topic, partitions=2, replication_factor=1)
    records = [("k", f"v{i}") for i in range(20)]
    submission.publish_ordered(kafka_broker.bootstrap_servers, topic, records)
    seqs, total = _consume_all(kafka_broker.bootstrap_servers, topic, 20)
    assert total == 20
    assert seqs["k"] == [f"v{i}" for i in range(20)]
