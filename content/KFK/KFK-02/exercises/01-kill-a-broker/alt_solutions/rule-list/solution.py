"""Alt 1 (genuinely different): predictor via an explicit rule list; read-back
via the batch consume() API."""

import time
import uuid


def write_accepted(acks, min_isr, isr_size):
    acks = str(acks)
    if isr_size < 1:
        return False
    rules = {
        "0": True,
        "1": True,
        "all": isr_size >= min_isr,
    }
    return bool(rules.get(acks, False))


def durable_after_one_more_loss(acks, min_isr, isr_size):
    acks = str(acks)
    if not write_accepted(acks, min_isr, isr_size):
        return False
    # Durable against one more loss <=> known to be on >= 2 replicas.
    effective_copies = isr_size if acks == "all" else 1
    required = max(min_isr if acks == "all" else 1, 1)
    guaranteed_copies = min(effective_copies, required) if acks == "all" else 1
    return guaranteed_copies >= 2


def produce_and_verify(bootstrap_servers, topic, records, *, acks):
    from confluent_kafka import Consumer, Producer

    cnt = {"n": 0}

    def _cb(err, _m):
        if err is None:
            cnt["n"] += 1

    p = Producer({"bootstrap.servers": bootstrap_servers, "acks": str(acks),
                  "retries": 5, "message.timeout.ms": 10000})
    for k, v in records:
        p.produce(topic, key=k, value=v, callback=_cb)
    p.flush(15)
    if str(acks) == "0":
        return cnt["n"]

    from confluent_kafka import TopicPartition

    c = Consumer({"bootstrap.servers": bootstrap_servers,
                  "group.id": f"kfk02a-{uuid.uuid4().hex}",
                  "enable.auto.commit": False})
    md = c.list_topics(topic, timeout=10)
    parts = sorted(md.topics[topic].partitions.keys())
    highs = {}
    assigns = []
    for part in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, part), timeout=10)
        highs[part] = hi
        assigns.append(TopicPartition(topic, part, lo))
    c.assign(assigns)
    target = sum(highs.values())
    seen = 0
    deadline = time.time() + 20
    while seen < target and time.time() < deadline:
        for m in c.consume(num_messages=max(1, target - seen), timeout=1.0):
            if m is not None and not m.error():
                seen += 1
    c.close()
    return seen
