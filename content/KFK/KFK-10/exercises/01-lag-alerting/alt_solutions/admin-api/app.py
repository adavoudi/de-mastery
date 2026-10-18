"""Alt solution 1 — compute lag via the AdminClient offset APIs (not the
consumer client), and the pure functions written in a different style.
"""

from __future__ import annotations

import uuid


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


def compute_lag(committed, end_offsets):
    # comprehension style; normalise committed first
    norm = {p: (committed.get(p) if (committed.get(p) or -1) >= 0 else 0)
            for p in end_offsets}
    return {p: max(0, end_offsets[p] - norm[p]) for p in end_offsets}


def total_lag(lag):
    acc = 0
    for v in lag.values():
        acc += v
    return acc


def evaluate_alerts(lag_by_partition, warn_threshold, page_threshold):
    pages = [v for v in lag_by_partition.values() if v >= page_threshold]
    if pages:
        return "page"
    warns = [v for v in lag_by_partition.values() if v >= warn_threshold]
    return "warn" if warns else "ok"


def measure_group_lag(bootstrap_servers, topic, group_id, timeout=15.0):
    import time

    from confluent_kafka import (ConsumerGroupTopicPartitions, KafkaException,
                                 TopicPartition)
    from confluent_kafka.admin import AdminClient, OffsetSpec

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    md = admin.list_topics(topic, timeout=timeout)
    parts = list(md.topics[topic].partitions.keys())

    # committed offsets via the admin group-offsets API (retry the transient
    # NOT_COORDINATOR seen right after group-coordinator churn)
    committed = {}
    req = [ConsumerGroupTopicPartitions(group_id)]
    deadline = time.time() + timeout
    while True:
        try:
            res = admin.list_consumer_group_offsets(req)[group_id].result()
            break
        except KafkaException:
            if time.time() >= deadline:
                raise
            time.sleep(0.3)
    present = {tp.partition: tp.offset for tp in res.topic_partitions}
    for p in parts:
        committed[p] = present.get(p, -1)

    # end offsets via list_offsets(latest)
    reqs = {TopicPartition(topic, p): OffsetSpec.latest() for p in parts}
    futs = admin.list_offsets(reqs)
    end = {tp.partition: f.result().offset for tp, f in futs.items()}

    return compute_lag(committed, end)
