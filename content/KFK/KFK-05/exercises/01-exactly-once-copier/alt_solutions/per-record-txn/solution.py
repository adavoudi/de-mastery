"""Alt 1 (genuinely different): one transaction PER record. Each record is
produced and its offset committed in its own atomic transaction. Still exactly
-once (each input reflected once); just finer-grained."""

import time


def _build(bootstrap_servers, group_id, transactional_id, src_topic):
    from confluent_kafka import Consumer, Producer

    p = Producer({"bootstrap.servers": bootstrap_servers,
                  "transactional.id": transactional_id,
                  "enable.idempotence": True})
    p.init_transactions(30)
    c = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                  "enable.auto.commit": False, "auto.offset.reset": "earliest",
                  "isolation.level": "read_committed"})
    c.subscribe([src_topic])
    return p, c


def copy_exactly_once(bootstrap_servers, src_topic, dst_topic, group_id,
                      transactional_id, max_records):
    p, c = _build(bootstrap_servers, group_id, transactional_id, src_topic)
    copied = 0
    try:
        deadline = time.time() + 20
        while copied < max_records and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            p.begin_transaction()
            p.produce(dst_topic, key=msg.key(), value=msg.value())
            p.send_offsets_to_transaction(
                c.position(c.assignment()), c.consumer_group_metadata(), 30)
            p.commit_transaction(30)
            copied += 1
            deadline = time.time() + 20
        return copied
    finally:
        c.close()


def copy_then_abort(bootstrap_servers, src_topic, dst_topic, group_id,
                    transactional_id, max_records):
    p, c = _build(bootstrap_servers, group_id, transactional_id, src_topic)
    produced = 0
    try:
        p.begin_transaction()
        deadline = time.time() + 20
        while produced < max_records and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            p.produce(dst_topic, key=msg.key(), value=msg.value())
            produced += 1
            deadline = time.time() + 20
        p.abort_transaction(30)
        return produced
    finally:
        c.close()
