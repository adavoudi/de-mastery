"""Mutant: commit-before-sink — commits the offset BEFORE doing the work, so a
crash in sink() loses that record (at-most-once)."""

import time


def process_batch(bootstrap_servers, topic, group_id, max_records, sink):
    from confluent_kafka import Consumer

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
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
            c.commit(message=msg, asynchronous=False)   # BUG: commit before work
            sink(k, v)
            processed += 1
            deadline = time.time() + 15
    finally:
        c.close()
    return processed
