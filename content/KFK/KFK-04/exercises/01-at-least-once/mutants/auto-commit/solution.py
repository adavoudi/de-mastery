"""Mutant: auto-commit — enables auto-commit with a tiny interval and commits
nothing manually, so offsets advance on a timer regardless of whether sink()
finished. A crash loses the in-flight records (at-most-once)."""

import time


def process_batch(bootstrap_servers, topic, group_id, max_records, sink):
    from confluent_kafka import Consumer

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": True,            # BUG
        "auto.commit.interval.ms": 5,
        "auto.offset.reset": "earliest",
    })
    c.subscribe([topic])
    processed = 0
    try:
        deadline = time.time() + 15
        while processed < max_records and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            k = msg.key().decode() if msg.key() is not None else None
            v = msg.value().decode() if msg.value() is not None else None
            # auto-commit may already have advanced the offset past this record
            time.sleep(0.02)
            sink(k, v)
            processed += 1
            deadline = time.time() + 15
    finally:
        c.close()
    return processed
