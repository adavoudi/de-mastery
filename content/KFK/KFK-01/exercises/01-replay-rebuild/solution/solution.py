"""Reference solution: replay per-partition from a start offset (or a
timestamp-derived offset) up to the high watermark, folding into a
latest-value-per-key table."""

import uuid


def _consumer(bootstrap_servers):
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk01-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })


def _partitions(consumer, topic):
    md = consumer.list_topics(topic, timeout=10)
    return sorted(md.topics[topic].partitions.keys())


def _fold(consumer, topic, start_by_partition):
    """start_by_partition: {partition: start_offset}. Reads [start, high) per
    partition, folding last-value-wins into a dict."""
    from confluent_kafka import TopicPartition

    highs = {}
    assigns = []
    for p, start in start_by_partition.items():
        _lo, hi = consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=10)
        highs[p] = hi
        assigns.append(TopicPartition(topic, p, start))
    consumer.assign(assigns)

    done = {p: start >= highs[p] for p, start in start_by_partition.items()}
    table: dict[str, str] = {}
    import time
    deadline = time.time() + 30
    while not all(done.values()) and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        p = msg.partition()
        if msg.key() is not None:
            table[msg.key().decode()] = msg.value().decode()
        if msg.offset() + 1 >= highs[p]:
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
        parts = _partitions(c, topic)
        tps = [TopicPartition(topic, p, since_ms) for p in parts]
        resolved = c.offsets_for_times(tps, timeout=10)
        start = {}
        for tp in resolved:
            # offset == -1 means no record at/after since_ms: start at the end.
            if tp.offset < 0:
                _lo, hi = c.get_watermark_offsets(
                    TopicPartition(topic, tp.partition), timeout=10)
                start[tp.partition] = hi
            else:
                start[tp.partition] = tp.offset
        return _fold(c, topic, start)
    finally:
        c.close()
