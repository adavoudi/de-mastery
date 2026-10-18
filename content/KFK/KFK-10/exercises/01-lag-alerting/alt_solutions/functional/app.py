"""Alt solution 2 — pure functions via functools.reduce / sorting, and
measure_group_lag that assigns the partitions and queries positions explicitly.
"""

from __future__ import annotations

import uuid
from functools import reduce


def copy_batch(bootstrap_servers, src_topic, dst_topic, group_id,
               max_records=1000, poll_timeout=0.5, idle_rounds=6):
    from confluent_kafka import Consumer, Producer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "transactional.id": f"copier-{uuid.uuid4()}",
    })
    producer.init_transactions()
    consumer.subscribe([src_topic])

    copied = 0
    try:
        producer.begin_transaction()
        pending = {}  # partition -> last offset consumed
        idle = 0
        # Count idle polls only AFTER partitions are assigned, so a slow
        # rebalance never looks like an empty topic (deterministic under
        # shuffled parallel runs). No warmup poll discards data.
        while copied < max_records and idle < idle_rounds:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                if consumer.assignment():
                    idle += 1
                continue
            if msg.error():
                continue
            idle = 0
            producer.produce(dst_topic, key=msg.key(), value=msg.value())
            pending[msg.partition()] = msg.offset()
            copied += 1

        if copied:
            offsets = [TopicPartition(src_topic, p, off + 1)
                       for p, off in pending.items()]
            producer.send_offsets_to_transaction(
                offsets, consumer.consumer_group_metadata())
            producer.commit_transaction()
        else:
            producer.abort_transaction()
    finally:
        consumer.close()
    return copied


def _clamp_committed(c):
    return c if (c is not None and c >= 0) else 0


def compute_lag(committed, end_offsets):
    result = {}
    for p in sorted(end_offsets):
        delta = end_offsets[p] - _clamp_committed(committed.get(p))
        result[p] = delta if delta > 0 else 0
    return result


def total_lag(lag):
    return reduce(lambda a, b: a + b, lag.values(), 0)


def evaluate_alerts(lag_by_partition, warn_threshold, page_threshold):
    severity = 0  # 0 ok, 1 warn, 2 page
    for v in lag_by_partition.values():
        if v >= page_threshold:
            return "page"
        if v >= warn_threshold:
            severity = max(severity, 1)
    return ["ok", "warn", "page"][severity]


def measure_group_lag(bootstrap_servers, topic, group_id, timeout=15.0):
    import time

    from confluent_kafka import Consumer, KafkaException, TopicPartition

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    try:
        md = c.list_topics(topic, timeout=timeout)
        parts = sorted(md.topics[topic].partitions.keys())
        deadline = time.time() + timeout
        while True:
            try:
                committed = {tp.partition: tp.offset
                             for tp in c.committed(
                                 [TopicPartition(topic, p) for p in parts],
                                 timeout=timeout)}
                break
            except KafkaException:
                if time.time() >= deadline:
                    raise
                time.sleep(0.3)
        end = {}
        for p in parts:
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p),
                                              timeout=timeout, cached=False)
            end[p] = hi
    finally:
        c.close()
    return compute_lag(committed, end)
