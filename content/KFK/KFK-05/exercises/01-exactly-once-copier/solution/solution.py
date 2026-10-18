"""Reference solution: transactional exactly-once copier (commit) and a
commit-then-abort variant for the atomicity invariant."""

import time


def _build(bootstrap_servers, group_id, transactional_id):
    from confluent_kafka import Consumer, Producer

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "transactional.id": transactional_id,
        "enable.idempotence": True,
    })
    producer.init_transactions(30)
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    return producer, consumer


def _drain(consumer, src_topic, dst_topic, producer, max_records):
    consumer.subscribe([src_topic])
    copied = 0
    deadline = time.time() + 20
    while copied < max_records and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        producer.produce(dst_topic, key=msg.key(), value=msg.value())
        copied += 1
    return copied


def copy_exactly_once(bootstrap_servers, src_topic, dst_topic, group_id,
                      transactional_id, max_records):
    producer, consumer = _build(bootstrap_servers, group_id, transactional_id)
    try:
        producer.begin_transaction()
        copied = _drain(consumer, src_topic, dst_topic, producer, max_records)
        producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.consumer_group_metadata(), 30)
        producer.commit_transaction(30)
        return copied
    finally:
        consumer.close()


def copy_then_abort(bootstrap_servers, src_topic, dst_topic, group_id,
                    transactional_id, max_records):
    producer, consumer = _build(bootstrap_servers, group_id, transactional_id)
    try:
        producer.begin_transaction()
        produced = _drain(consumer, src_topic, dst_topic, producer, max_records)
        producer.abort_transaction(30)   # all-or-nothing: nothing becomes visible
        return produced
    finally:
        consumer.close()
