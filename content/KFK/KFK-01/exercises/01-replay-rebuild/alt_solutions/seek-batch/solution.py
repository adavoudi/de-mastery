"""Alt 1 (genuinely different): assign partitions AT their start offset (no
seek), and drain with the batch consume() API, counting down a known remaining
total instead of a per-partition 'done' flag."""

import time
import uuid


def _consumer(bootstrap_servers):
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk01alt-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })


def _fold(consumer, topic, start_by_partition):
    from confluent_kafka import TopicPartition

    assigns = []
    remaining = 0
    for p, start in start_by_partition.items():
        _lo, hi = consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=10)
        clamped = min(start, hi)
        assigns.append(TopicPartition(topic, p, clamped))
        remaining += max(0, hi - clamped)
    consumer.assign(assigns)

    table: dict[str, str] = {}
    deadline = time.time() + 30
    while remaining > 0 and time.time() < deadline:
        for m in consumer.consume(num_messages=remaining, timeout=1.0):
            if m is None or m.error():
                continue
            if m.key() is not None:
                table[m.key().decode()] = m.value().decode()
            remaining -= 1
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
                start[tp.partition] = tp.offset
        return _fold(c, topic, start)
    finally:
        c.close()
