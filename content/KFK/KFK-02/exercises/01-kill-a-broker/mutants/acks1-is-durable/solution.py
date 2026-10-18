"""Mutant: acks1-is-durable — treats acks=1 (leader-only) as surviving one more
broker loss, the classic 'my acked write vanished' bug."""

import time
import uuid


def write_accepted(acks, min_isr, isr_size):
    acks = str(acks)
    if isr_size < 1:
        return False
    if acks in ("0", "1"):
        return True
    return isr_size >= min_isr


def durable_after_one_more_loss(acks, min_isr, isr_size):
    acks = str(acks)
    if not write_accepted(acks, min_isr, isr_size):
        return False
    # BUG: counts acks='1' as durable against one more loss.
    if acks in ("1", "all") and min_isr >= 2 and isr_size >= 2:
        return True
    return acks == "1" and isr_size >= 1


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
    c = Consumer({"bootstrap.servers": bootstrap_servers,
                  "group.id": f"kfk02m1-{uuid.uuid4().hex}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    seen = 0
    deadline = time.time() + 20
    while seen < cnt["n"] and time.time() < deadline:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        seen += 1
    c.close()
    return seen
