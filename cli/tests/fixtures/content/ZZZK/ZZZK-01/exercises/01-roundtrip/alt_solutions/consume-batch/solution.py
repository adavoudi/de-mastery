"""Alt 2 (genuinely different): use Consumer.consume() batch API and tally with
a plain dict comprehension over the drained messages."""

import time
import uuid


def publish_and_count(bootstrap_servers, topic, records):
    from confluent_kafka import Consumer, Producer

    p = Producer({"bootstrap.servers": bootstrap_servers})
    for key, value in records:
        p.produce(topic, key=key, value=value)
    p.flush(15)

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"selftest-batch-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([topic])
    drained = []
    deadline = time.time() + 20
    while len(drained) < len(records) and time.time() < deadline:
        msgs = c.consume(num_messages=len(records) - len(drained), timeout=1.0)
        for m in msgs:
            if m is not None and not m.error():
                drained.append(m)
    c.close()

    counts: dict[str, int] = {}
    for m in drained:
        k = m.key().decode() if m.key() is not None else ""
        counts[k] = counts.get(k, 0) + 1
    return counts
