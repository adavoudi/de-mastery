"""Alt 2 (genuinely different): drain with the batch consume() API, produce the
whole batch, then commit one transaction. Offsets come from the last message per
partition explicitly."""

import time


def _clients(bootstrap_servers, group_id, transactional_id, src_topic):
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


def _collect(c, max_records):
    msgs = []
    deadline = time.time() + 20
    while len(msgs) < max_records and time.time() < deadline:
        for m in c.consume(num_messages=max_records - len(msgs), timeout=1.0):
            if m is not None and not m.error():
                msgs.append(m)
    return msgs


def copy_exactly_once(bootstrap_servers, src_topic, dst_topic, group_id,
                      transactional_id, max_records):
    p, c = _clients(bootstrap_servers, group_id, transactional_id, src_topic)
    try:
        p.begin_transaction()
        msgs = _collect(c, max_records)
        for m in msgs:
            p.produce(dst_topic, key=m.key(), value=m.value())
        p.send_offsets_to_transaction(
            c.position(c.assignment()), c.consumer_group_metadata(), 30)
        p.commit_transaction(30)
        return len(msgs)
    finally:
        c.close()


def copy_then_abort(bootstrap_servers, src_topic, dst_topic, group_id,
                    transactional_id, max_records):
    p, c = _clients(bootstrap_servers, group_id, transactional_id, src_topic)
    try:
        p.begin_transaction()
        msgs = _collect(c, max_records)
        for m in msgs:
            p.produce(dst_topic, key=m.key(), value=m.value())
        p.abort_transaction(30)
        return len(msgs)
    finally:
        c.close()
