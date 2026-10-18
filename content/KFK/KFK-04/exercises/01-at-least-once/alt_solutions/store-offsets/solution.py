"""Alt 2 (genuinely different): use store_offsets() to record progress after a
successful sink, then commit the stored offsets. enable.auto.offset.store=False
ensures nothing is stored (hence committed) until the work succeeds."""

import time


def process_batch(bootstrap_servers, topic, group_id, max_records, sink):
    from confluent_kafka import Consumer

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
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
            sink(k, v)                     # work first; may raise -> no store
            c.store_offsets(message=msg)   # mark this record done
            c.commit(asynchronous=False)   # commit the stored offset
            processed += 1
            deadline = time.time() + 15
    finally:
        c.close()
    return processed
