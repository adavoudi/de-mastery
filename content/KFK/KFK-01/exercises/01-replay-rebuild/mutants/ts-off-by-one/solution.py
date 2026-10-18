"""Mutant: ts-off-by-one — for timestamp replay, starts at
offsets_for_times()+1, dropping the FIRST record at/after since_ms. The
boundary event is lost, so that key's value is stale (or missing)."""

import time
import uuid


def _consumer(bootstrap_servers):
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk01m3-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })


def _fold(consumer, topic, start_by_partition):
    from confluent_kafka import TopicPartition

    highs = {}
    assigns = []
    for p, start in start_by_partition.items():
        _lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        highs[p] = hi
        assigns.append(TopicPartition(topic, p, min(start, hi)))
    consumer.assign(assigns)
    done = {p: min(s, highs[p]) >= highs[p] for p, s in start_by_partition.items()}
    table = {}
    deadline = time.time() + 30
    while not all(done.values()) and time.time() < deadline:
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        p = m.partition()
        if m.key() is not None:
            table[m.key().decode()] = m.value().decode()
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
            if tp.offset < 0:
                _lo, hi = c.get_watermark_offsets(
                    TopicPartition(topic, tp.partition), timeout=10)
                start[tp.partition] = hi
            else:
                start[tp.partition] = tp.offset + 1   # BUG: skip boundary record
        return _fold(c, topic, start)
    finally:
        c.close()
