"""Reference solution: at-least-once consumer. Process (sink) first, commit
only on success. A sink exception propagates with the offset uncommitted."""

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
        idle_deadline = time.time() + 15
        while processed < max_records and time.time() < idle_deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode() if msg.key() is not None else None
            value = msg.value().decode() if msg.value() is not None else None
            # 1) do the work; may raise -> propagates WITHOUT committing (crash)
            sink(key, value)
            # 2) only on success, commit this record's offset
            c.commit(message=msg, asynchronous=False)
            processed += 1
            idle_deadline = time.time() + 15
    finally:
        c.close()
    return processed
