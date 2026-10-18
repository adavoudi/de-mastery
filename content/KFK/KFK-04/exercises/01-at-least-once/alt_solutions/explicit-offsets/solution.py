"""Alt 1 (genuinely different): commit by building an explicit TopicPartition
offset list (offset = msg.offset() + 1) instead of commit(message=...)."""

import time


def process_batch(bootstrap_servers, topic, group_id, max_records, sink):
    from confluent_kafka import Consumer, TopicPartition

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
            sink(k, v)
            c.commit(offsets=[TopicPartition(msg.topic(), msg.partition(),
                                             msg.offset() + 1)],
                     asynchronous=False)
            processed += 1
            deadline = time.time() + 15
    finally:
        c.close()
    return processed
