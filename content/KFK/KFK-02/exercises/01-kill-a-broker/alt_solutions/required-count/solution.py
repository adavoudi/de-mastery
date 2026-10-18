"""Alt 2 (genuinely different): predictor expressed as 'required acks count vs
available', and read-back by summing (high-low) watermark offsets across all
partitions instead of polling records."""

import uuid


def _required_copies(acks, min_isr):
    acks = str(acks)
    if acks == "0":
        return 0        # not even leader confirmation required
    if acks == "1":
        return 1
    return min_isr      # acks == "all"


def write_accepted(acks, min_isr, isr_size):
    # Leader must be reachable for any write; acks=all additionally needs min_isr.
    if isr_size < 1:
        return False
    return isr_size >= _required_copies(acks, min_isr)


def durable_after_one_more_loss(acks, min_isr, isr_size):
    if not write_accepted(acks, min_isr, isr_size):
        return False
    # The number of replicas KNOWN to hold the record when accepted.
    known = _required_copies(acks, min_isr) if str(acks) == "all" else 1
    return known >= 2


def produce_and_verify(bootstrap_servers, topic, records, *, acks):
    from confluent_kafka import Consumer, Producer, TopicPartition

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
                  "group.id": f"kfk02b-{uuid.uuid4().hex}"})
    try:
        md = c.list_topics(topic, timeout=10)
        parts = list(md.topics[topic].partitions)
        total = 0
        import time as _t
        for part in parts:
            # During a failover the watermark query can transiently report
            # NOT_LEADER until the new leader is known; retry briefly.
            deadline = _t.time() + 15
            while True:
                try:
                    lo, hi = c.get_watermark_offsets(
                        TopicPartition(topic, part), timeout=10)
                    total += hi - lo
                    break
                except Exception:  # noqa: BLE001
                    if _t.time() > deadline:
                        raise
                    _t.sleep(0.3)
        return total
    finally:
        c.close()
