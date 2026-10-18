"""Alt 2 (different consume path): add the field with a default (like the
reference), but read back using an explicit partition assignment + batch
consume() across all partitions instead of a subscribe/poll loop."""

import io
import uuid


def base_schema():
    return {
        "type": "record",
        "name": "Order",
        "namespace": "de.commerce",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "amount", "type": "double"},
        ],
    }


def evolved_schema():
    s = base_schema()
    return {**s, "fields": s["fields"] + [
        {"name": "currency", "type": "string", "default": "USD"},
    ]}


def produce_and_read(bootstrap_servers, topic, orders):
    import time

    from confluent_kafka import Consumer, Producer, TopicPartition
    from fastavro import schemaless_reader, schemaless_writer
    from fastavro.schema import parse_schema

    new_schema = parse_schema(dict(evolved_schema()))
    old_schema = parse_schema(dict(base_schema()))

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for oid, amount, currency in orders:
        buf = io.BytesIO()
        schemaless_writer(buf, new_schema, {"id": oid, "amount": amount,
                                            "currency": currency})
        producer.produce(topic, key=str(oid), value=buf.getvalue())
    producer.flush(15)

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk06alt2-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    md = consumer.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p, 0) for p in md.partitions]
    consumer.assign(parts)

    raw = []
    deadline = time.time() + 20
    while len(raw) < len(orders) and time.time() < deadline:
        msgs = consumer.consume(num_messages=len(orders), timeout=1.0)
        for msg in msgs:
            if msg is None or msg.error():
                continue
            raw.append(msg.value())
    consumer.close()

    def dec(reader_schema):
        return [schemaless_reader(io.BytesIO(d), new_schema, reader_schema)
                for d in raw]

    new_reader = sorted(dec(new_schema), key=lambda r: r["id"])
    old_reader = sorted(dec(old_schema), key=lambda r: r["id"])
    return {"new_reader": new_reader, "old_reader": old_reader}
