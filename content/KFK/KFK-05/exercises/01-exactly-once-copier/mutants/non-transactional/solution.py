"""Mutant: non-transactional — uses a plain (non-transactional) producer and
commits consumer offsets directly. copy_then_abort has no real transaction to
abort, so its 'aborted' records are actually written and VISIBLE to a
read_committed consumer -> breaks atomicity."""

import time


def _clients(bootstrap_servers, group_id, src_topic):
    from confluent_kafka import Consumer, Producer

    p = Producer({"bootstrap.servers": bootstrap_servers,
                  "enable.idempotence": True})     # no transactional.id
    c = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                  "enable.auto.commit": False, "auto.offset.reset": "earliest",
                  "isolation.level": "read_committed"})
    c.subscribe([src_topic])
    return p, c


def copy_exactly_once(bootstrap_servers, src_topic, dst_topic, group_id,
                      transactional_id, max_records):
    p, c = _clients(bootstrap_servers, group_id, src_topic)
    copied = 0
    try:
        deadline = time.time() + 20
        while copied < max_records and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            p.produce(dst_topic, key=msg.key(), value=msg.value())
            copied += 1
            deadline = time.time() + 20
        p.flush(20)
        c.commit(asynchronous=False)
        return copied
    finally:
        c.close()


def copy_then_abort(bootstrap_servers, src_topic, dst_topic, group_id,
                    transactional_id, max_records):
    p, c = _clients(bootstrap_servers, group_id, src_topic)
    produced = 0
    try:
        deadline = time.time() + 20
        while produced < max_records and time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            p.produce(dst_topic, key=msg.key(), value=msg.value())
            produced += 1
            deadline = time.time() + 20
        p.flush(20)   # BUG: no transaction -> records are visible, not aborted
        return produced
    finally:
        c.close()
