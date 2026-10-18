"""Reference solution — KFK-10 client metrics + consumer-lag alerting.

A self-contained transactional copier app (``copy_batch``) plus the observability
functions the exercise grades:

* ``compute_lag``       — pure: end_offset - committed_offset per partition.
* ``total_lag``         — pure: sum across partitions.
* ``evaluate_alerts``   — pure: ok / warn / page from per-partition lag + thresholds.
* ``measure_group_lag`` — reads a group's committed offsets and the partitions'
                          end offsets from a live broker and returns lag per partition.

The copier itself is shipped here (NOT imported from KFK-05) so this exercise is
fully self-contained.
"""

from __future__ import annotations

import uuid


# ---------------------------------------------------------------------------
# The app under observation: a transactional (exactly-once) copier.
# Reads up to ``max_records`` records from src_topic and copies them to
# dst_topic inside a single Kafka transaction, committing the source offsets
# as part of that transaction (read-process-write). Returns the number copied.
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
# Pure observability functions (graded with plain data — no broker).
# ---------------------------------------------------------------------------
def compute_lag(committed, end_offsets):
    """Lag per partition = end_offset - committed_offset.

    ``committed[p]`` may be None or negative (no committed offset yet) — then the
    whole partition is unread, so lag == end_offset. Lag is never negative.
    """
    lag = {}
    for p, end in end_offsets.items():
        c = committed.get(p)
        if c is None or c < 0:
            c = 0
        lag[p] = max(0, end - c)
    return lag


def total_lag(lag):
    """Sum of per-partition lag."""
    return sum(lag.values())


def evaluate_alerts(lag_by_partition, warn_threshold, page_threshold):
    """Highest severity wins. Thresholds are inclusive.

    Returns 'page' if any partition lag >= page_threshold, else 'warn' if any
    >= warn_threshold, else 'ok'.
    """
    worst = max(lag_by_partition.values(), default=0)
    if worst >= page_threshold:
        return "page"
    if worst >= warn_threshold:
        return "warn"
    return "ok"


def measure_group_lag(bootstrap_servers, topic, group_id, timeout=15.0):
    """Read a group's committed offsets and the partitions' end offsets from a
    live broker, then return lag per partition via ``compute_lag``.

    Does not join the group for consumption — only reads committed + end offsets.
    """
    from confluent_kafka import Consumer, KafkaException, TopicPartition
    import time

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    try:
        md = c.list_topics(topic, timeout=timeout)
        parts = list(md.topics[topic].partitions.keys())
        tps = [TopicPartition(topic, p) for p in parts]

        # The group coordinator may still be settling right after churn, which
        # surfaces as a transient NOT_COORDINATOR; retry briefly.
        committed = {}
        deadline = time.time() + timeout
        while True:
            try:
                for tp in c.committed(tps, timeout=timeout):
                    committed[tp.partition] = tp.offset  # -1001 => none
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
