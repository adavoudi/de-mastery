"""Alt 2 (genuinely different): drain the WHOLE log once into per-partition
record lists (ordered by offset), then slice by offset or by timestamp purely in
the application and fold. No seek/offsets_for_times — the slicing is done on the
in-memory records, which is a legitimate (if less efficient) way to get the same
table."""

import time
import uuid


def _drain_all(bootstrap_servers, topic):
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"kfk01alt2-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
    })
    try:
        md = c.list_topics(topic, timeout=10)
        parts = sorted(md.topics[topic].partitions.keys())
        highs = {}
        assigns = []
        for p in parts:
            _lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            highs[p] = hi
            assigns.append(TopicPartition(topic, p, 0))
        c.assign(assigns)
        records = {p: [] for p in parts}
        total = sum(highs.values())
        seen = 0
        deadline = time.time() + 30
        while seen < total and time.time() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            records[m.partition()].append(
                (m.offset(), m.timestamp()[1],
                 None if m.key() is None else m.key().decode(),
                 None if m.value() is None else m.value().decode()))
            seen += 1
        for p in records:
            records[p].sort(key=lambda r: r[0])
        return records
    finally:
        c.close()


def _fold_slices(records, predicate):
    table: dict[str, str] = {}
    for p in sorted(records):
        for off, ts, key, val in records[p]:
            if key is None or not predicate(p, off, ts):
                continue
            table[key] = val
    return table


def rebuild_from_offset(bootstrap_servers, topic, start_offsets):
    records = _drain_all(bootstrap_servers, topic)
    starts = dict(start_offsets)
    return _fold_slices(records, lambda p, off, ts: off >= starts.get(p, 0))


def rebuild_from_timestamp(bootstrap_servers, topic, since_ms):
    records = _drain_all(bootstrap_servers, topic)
    return _fold_slices(records, lambda p, off, ts: ts >= since_ms)
