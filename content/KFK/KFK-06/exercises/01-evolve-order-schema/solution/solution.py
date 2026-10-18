"""Reference solution: add `currency` with a default (BACKWARD-safe), serialize
with the evolved schema, and read back with both the new and the old reader."""

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
    # New field MUST carry a default so an old record (no currency) can be read
    # with the new schema -> BACKWARD compatible.
    s = {**s, "fields": s["fields"] + [
        {"name": "currency", "type": "string", "default": "USD"},
    ]}
    return s


def _encode(schema, record):
    from fastavro import schemaless_writer
    from fastavro.schema import parse_schema

    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(dict(schema)), record)
    return buf.getvalue()


def _decode(writer_schema, reader_schema, data):
    from fastavro import schemaless_reader
    from fastavro.schema import parse_schema

    return schemaless_reader(io.BytesIO(data), parse_schema(dict(writer_schema)),
                             parse_schema(dict(reader_schema)))


def produce_and_read(bootstrap_servers, topic, orders):
    from confluent_kafka import Consumer, Producer

    new_schema = evolved_schema()
    old_schema = base_schema()

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for oid, amount, currency in orders:
        record = {"id": oid, "amount": amount, "currency": currency}
        producer.produce(topic, key=str(oid), value=_encode(new_schema, record))
    producer.flush(15)

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk06-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    raw = []
    import time
    deadline = time.time() + 20
    while len(raw) < len(orders) and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        raw.append(msg.value())
    consumer.close()

    new_reader = [_decode(new_schema, new_schema, d) for d in raw]
    old_reader = [_decode(new_schema, old_schema, d) for d in raw]
    new_reader.sort(key=lambda r: r["id"])
    old_reader.sort(key=lambda r: r["id"])
    return {"new_reader": new_reader, "old_reader": old_reader}
