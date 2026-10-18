"""Starter — KFK-10. The copier app is given and works. Your job is to add the
observability functions. Each stub below MUST be implemented (they currently
fail the tests: gate G1).
"""

from __future__ import annotations

import uuid


# ---------------------------------------------------------------------------
# PROVIDED: a transactional (exactly-once) copier. Do not change it.
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# TODO: implement these.
# ---------------------------------------------------------------------------
def compute_lag(committed, end_offsets):
    """Return {partition: lag} where lag = end_offset - committed_offset.
    A partition with no committed offset (None or < 0) is fully unread.
    Lag must never be negative."""
    # TODO
    return {}


def total_lag(lag):
    """Return the sum of per-partition lag."""
    # TODO
    return 0


def evaluate_alerts(lag_by_partition, warn_threshold, page_threshold):
    """Return 'page' / 'warn' / 'ok'. Highest severity wins; inclusive."""
    # TODO
    return "ok"


def measure_group_lag(bootstrap_servers, topic, group_id, timeout=15.0):
    """Read the group's committed offsets and the partitions' end offsets from
    the broker and return lag per partition (reuse compute_lag)."""
    # TODO
    return {}
