"""Alt 1 (genuinely different): produce with a delivery callback that waits for
all acks, then consume via an explicit per-partition assign + Counter."""

import time
import uuid
from collections import Counter


def publish_and_count(bootstrap_servers, topic, records):
    from confluent_kafka import Consumer, Producer, TopicPartition

    delivered = {"n": 0}

    def _cb(err, msg):
        if err is None:
            delivered["n"] += 1

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for key, value in records:
        producer.produce(topic, key=key, value=value, on_delivery=_cb)
    producer.flush(15)

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"selftest-alt-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    md = consumer.list_topics(topic, timeout=10)
    parts = list(md.topics[topic].partitions.keys())
    consumer.assign([TopicPartition(topic, p, 0) for p in parts])

    counts = Counter()
    seen = 0
    deadline = time.time() + 20
    while seen < len(records) and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        k = msg.key().decode() if msg.key() is not None else ""
        counts[k] += 1
        seen += 1
    consumer.close()
    return dict(counts)
