"""Reference solution: the durability decision table as pure functions, plus a
live produce-then-read-back verifier."""

import time
import uuid


def write_accepted(acks, min_isr, isr_size):
    acks = str(acks)
    if isr_size < 1:
        return False
    if acks in ("0", "1"):
        return True
    # acks == "all"
    return isr_size >= min_isr


def durable_after_one_more_loss(acks, min_isr, isr_size):
    acks = str(acks)
    if not write_accepted(acks, min_isr, isr_size):
        return False
    # Only acks=all guarantees followers have it; it must have been accepted
    # into an ISR of at least 2 (min_isr>=2 and isr_size>=min_isr).
    return acks == "all" and min_isr >= 2 and isr_size >= 2


def produce_and_verify(bootstrap_servers, topic, records, *, acks):
    from confluent_kafka import Consumer, Producer

    delivered = {"n": 0}

    def _cb(err, _msg):
        if err is None:
            delivered["n"] += 1

    p = Producer({
        "bootstrap.servers": bootstrap_servers,
        "acks": str(acks),
        "retries": 5,
        "message.timeout.ms": 10000,
    })
    for key, value in records:
        p.produce(topic, key=key, value=value, callback=_cb)
    p.flush(15)

    if str(acks) == "0":
        return delivered["n"]

    # Count ALL durably-readable records in the topic from the beginning, by
    # draining each partition to its high watermark.
    from confluent_kafka import TopicPartition

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk02-verify-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        md = c.list_topics(topic, timeout=10)
        parts = sorted(md.topics[topic].partitions.keys())
        highs = {}
        assigns = []
        for part in parts:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, part), timeout=10)
            highs[part] = hi
            assigns.append(TopicPartition(topic, part, lo))
        c.assign(assigns)
        target = sum(highs[p] for p in parts)
        seen = 0
        deadline = time.time() + 20
        while seen < target and time.time() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            seen += 1
        return seen
    finally:
        c.close()
