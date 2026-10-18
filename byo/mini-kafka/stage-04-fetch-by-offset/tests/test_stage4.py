"""Stage 4 — Fetch by offset.

Black-box: a real librdkafka Consumer with MANUAL assignment reads back exactly
what was produced, from offset 0 and from a mid-log offset. We also check
ListOffsets (earliest/latest) which librdkafka uses to resolve ``earliest`` /
``latest`` and the end of the log.
"""
from _broker_harness import (produce_sync, consume_from,
                             produce_separate_batches,
                             raw_fetch_first_base_offset)


def test_consume_all_from_zero(broker):
    topic = "fetch"
    recs = [(f"k{i}".encode(), f"v{i}".encode()) for i in range(8)]
    produce_sync(broker.bootstrap, topic, recs)
    got = consume_from(broker.bootstrap, topic, 0, 0, 8)
    assert [(k, v) for k, v, _ in got] == recs
    assert [o for _, _, o in got] == list(range(8))


def test_consume_from_mid_offset(broker):
    topic = "fetchmid"
    recs = [(None, f"m{i}".encode()) for i in range(10)]
    produce_sync(broker.bootstrap, topic, recs)
    got = consume_from(broker.bootstrap, topic, 0, 5, 5)
    assert [v for _, v, _ in got] == [f"m{i}".encode() for i in range(5, 10)]
    assert [o for _, _, o in got] == [5, 6, 7, 8, 9]


def test_list_offsets_earliest_latest(broker):
    """A real client's end-offset query (list offsets) must report 0 earliest
    and N latest after producing N records."""
    from confluent_kafka import Consumer, TopicPartition
    import uuid
    topic = "fetchlo"
    produce_sync(broker.bootstrap, topic, [(None, b"z")] * 7)
    c = Consumer({"bootstrap.servers": broker.bootstrap,
                  "group.id": "g-" + uuid.uuid4().hex[:8],
                  "enable.auto.commit": False})
    try:
        low, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=5)
        assert low == 0, f"earliest should be 0, got {low}"
        assert high == 7, f"latest should be 7, got {high}"
    finally:
        c.close()


def test_fetch_past_end_returns_nothing(broker):
    topic = "fetchend"
    produce_sync(broker.bootstrap, topic, [(None, b"a"), (None, b"b")])
    got = consume_from(broker.bootstrap, topic, 0, 2, 1, timeout=3)
    assert got == [], "fetching at the log end offset must return no records"


def test_raw_fetch_honours_offset(broker):
    """With one batch per record, a raw Fetch at offset 4 must return a batch
    whose first record batch base offset is exactly 4 (the broker uses the
    index/scan to skip the earlier batches, not rewind to 0)."""
    topic = "fetchraw"
    produce_separate_batches(broker.bootstrap, topic,
                             [(None, f"r{i}".encode()) for i in range(8)])
    base = raw_fetch_first_base_offset(broker, topic, 0, 4)
    assert base is not None, "fetch at offset 4 returned no record batch"
    assert base == 4, f"fetch at offset 4 must start at batch 4, got {base}"
    base0 = raw_fetch_first_base_offset(broker, topic, 0, 0)
    assert base0 == 0


def test_last_record_is_delivered(broker):
    """Fetching starting exactly at the last offset must deliver that one
    record (guards against an off-by-one that drops the tail)."""
    topic = "fetchlast"
    produce_sync(broker.bootstrap, topic, [(None, f"L{i}".encode()) for i in range(6)])
    got = consume_from(broker.bootstrap, topic, 0, 5, 1)
    assert [v for _, v, _ in got] == [b"L5"], f"last record not delivered, got {got}"
    # also: reading all 6 from the start yields the last one too
    allrecs = consume_from(broker.bootstrap, topic, 0, 0, 6)
    assert [o for _, _, o in allrecs] == [0, 1, 2, 3, 4, 5]
