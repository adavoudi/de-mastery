"""Mutant: first-wins — keeps the FIRST value seen per key instead of the last,
so the rebuilt 'current' table is wrong whenever a key has >1 event."""

import time
import uuid


def _consumer(bootstrap_servers):
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk01m2-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })


def _fold(consumer, topic, start_by_partition):
    from confluent_kafka import TopicPartition

    highs = {}
    assigns = []
    for p, start in start_by_partition.items():
        _lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        highs[p] = hi
        assigns.append(TopicPartition(topic, p, start))
    consumer.assign(assigns)
    done = {p: s >= highs[p] for p, s in start_by_partition.items()}
    table = {}
    deadline = time.time() + 30
    while not all(done.values()) and time.time() < deadline:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        p = m.partition()
        if m.key() is not None:
            k = m.key().decode()
            if k not in table:            # BUG: first value wins, not last
                table[k] = m.value().decode()
        if m.offset() + 1 >= highs[p]:
            done[p] = True
    return table


def rebuild_from_offset(bootstrap_servers, topic, start_offsets):
    c = _consumer(bootstrap_servers)
    try:
        return _fold(c, topic, dict(start_offsets))
    finally:
        c.close()


def rebuild_from_timestamp(bootstrap_servers, topic, since_ms):
    from confluent_kafka import TopicPartition

    c = _consumer(bootstrap_servers)
    try:
        md = c.list_topics(topic, timeout=10)
        parts = sorted(md.topics[topic].partitions.keys())
        resolved = c.offsets_for_times(
            [TopicPartition(topic, p, since_ms) for p in parts], timeout=10)
        start = {}
        for tp in resolved:
            start[tp.partition] = tp.offset if tp.offset >= 0 else 0
        return _fold(c, topic, start)
    finally:
        c.close()
