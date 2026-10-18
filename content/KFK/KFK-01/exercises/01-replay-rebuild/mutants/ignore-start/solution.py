"""Mutant: ignore-start — always replays from offset 0 regardless of the
requested start, so a partial replay returns the full-history table."""

import time
import uuid


def _consumer(bootstrap_servers):
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk01m1-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })


def _fold_from_zero(consumer, topic):
    from confluent_kafka import TopicPartition

    md = consumer.list_topics(topic, timeout=10)
    parts = sorted(md.topics[topic].partitions.keys())
    highs = {}
    assigns = []
    for p in parts:
        _lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        highs[p] = hi
        assigns.append(TopicPartition(topic, p, 0))  # BUG: always 0
    consumer.assign(assigns)
    table = {}
    total = sum(highs.values())
    seen = 0
    deadline = time.time() + 30
    while seen < total and time.time() < deadline:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        if m.key() is not None:
            table[m.key().decode()] = m.value().decode()
        seen += 1
    return table


def rebuild_from_offset(bootstrap_servers, topic, start_offsets):
    c = _consumer(bootstrap_servers)
    try:
        return _fold_from_zero(c, topic)
    finally:
        c.close()


def rebuild_from_timestamp(bootstrap_servers, topic, since_ms):
    c = _consumer(bootstrap_servers)
    try:
        return _fold_from_zero(c, topic)
    finally:
        c.close()
