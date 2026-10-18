"""Mutant: tallies by VALUE instead of KEY — wrong grouping dimension."""

import time
import uuid


def publish_and_count(bootstrap_servers, topic, records):
    from confluent_kafka import Consumer, Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for key, value in records:
        producer.produce(topic, key=key, value=value)
    producer.flush(15)

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"selftest-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    counts: dict[str, int] = {}
    seen = 0
    deadline = time.time() + 20
    while seen < len(records) and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        # BUG: counts by the message VALUE, not the key.
        v = msg.value().decode() if msg.value() is not None else ""
        counts[v] = counts.get(v, 0) + 1
        seen += 1
    consumer.close()
    return counts
