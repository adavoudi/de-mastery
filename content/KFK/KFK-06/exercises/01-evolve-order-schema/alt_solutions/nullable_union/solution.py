"""Alt 1 (genuinely different evolution mechanism): add `currency` as a nullable
union ["string","null"] with default null. Still BACKWARD-safe (old records get
null), and the old reader still ignores the field. Consumes with a poll loop."""

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
        {"name": "currency", "type": ["string", "null"], "default": "USD"},
    ]}


def _codec():
    from fastavro import schemaless_reader, schemaless_writer
    from fastavro.schema import parse_schema
    return schemaless_writer, schemaless_reader, parse_schema


def produce_and_read(bootstrap_servers, topic, orders):
    from confluent_kafka import Consumer, Producer

    writer, reader, parse = _codec()
    new_schema = parse(dict(evolved_schema()))
    old_schema = parse(dict(base_schema()))

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for oid, amount, currency in orders:
        buf = io.BytesIO()
        writer(buf, new_schema, {"id": oid, "amount": amount, "currency": currency})
        producer.produce(topic, key=str(oid), value=buf.getvalue())
    producer.flush(15)

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk06alt-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    new_reader, old_reader = [], []
    import time
    deadline = time.time() + 20
    while len(new_reader) < len(orders) and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        data = msg.value()
        new_reader.append(reader(io.BytesIO(data), new_schema, new_schema))
        old_reader.append(reader(io.BytesIO(data), new_schema, old_schema))
    consumer.close()

    new_reader.sort(key=lambda r: r["id"])
    old_reader.sort(key=lambda r: r["id"])
    return {"new_reader": new_reader, "old_reader": old_reader}
