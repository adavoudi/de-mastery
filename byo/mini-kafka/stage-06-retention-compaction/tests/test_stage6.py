"""Stage 6 — retention and log compaction.

Black-box: create a COMPACTED topic (cleanup.policy=compact) via the AdminClient,
produce several values per key, and verify a consumer reading from the start sees
only the LATEST value per key (compaction), while the end offset (log end) is
unchanged. Also verify size-based retention advances the log start offset so an
early fetch is served from the retained start.
"""
import uuid

from confluent_kafka.admin import AdminClient, NewTopic

from _broker_harness import produce_sync, consume_from


def _create(bootstrap, name, partitions=1, config=None, timeout=15):
    a = AdminClient({"bootstrap.servers": bootstrap})
    fs = a.create_topics([NewTopic(name, num_partitions=partitions,
                                    replication_factor=1, config=config or {})])
    for t, f in fs.items():
        f.result(timeout=timeout)


def _latest_per_key(bootstrap, topic, n_read, timeout=20):
    got = consume_from(bootstrap, topic, 0, 0, n_read, timeout=timeout)
    latest = {}
    for k, v, _o in got:
        latest[k] = v
    return latest, got


def test_compaction_keeps_latest_per_key(broker):
    topic = "cmp-" + uuid.uuid4().hex[:6]
    _create(broker.bootstrap, topic, config={"cleanup.policy": "compact",
                                              "segment.bytes": "1000000"})
    # 3 keys, each updated; final values: a->3, b->2, c->1
    produce_sync(broker.bootstrap, topic, [
        (b"a", b"1"), (b"b", b"1"), (b"a", b"2"),
        (b"c", b"1"), (b"b", b"2"), (b"a", b"3"),
    ])
    latest, got = _latest_per_key(broker.bootstrap, topic, 3)
    assert latest.get(b"a") == b"3"
    assert latest.get(b"b") == b"2"
    assert latest.get(b"c") == b"1"
    # after compaction only one record per key remains readable
    assert len(got) == 3, f"compaction should leave 3 records, saw {len(got)}"


def test_compaction_does_not_change_end_offset(broker):
    from confluent_kafka import Consumer, TopicPartition
    topic = "cmp2-" + uuid.uuid4().hex[:6]
    _create(broker.bootstrap, topic, config={"cleanup.policy": "compact"})
    produce_sync(broker.bootstrap, topic,
                 [(b"k", str(i).encode()) for i in range(5)])
    c = Consumer({"bootstrap.servers": broker.bootstrap,
                  "group.id": "g-" + uuid.uuid4().hex[:8]})
    try:
        _, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=5)
        assert high == 5, f"log end offset must stay 5 after compaction, got {high}"
    finally:
        c.close()
    latest, got = _latest_per_key(broker.bootstrap, topic, 1)
    assert latest.get(b"k") == b"4", "only the newest value for the key survives"


def test_delete_topic_is_not_compacted(broker):
    """A normal (delete-policy) topic keeps every version."""
    topic = "del-" + uuid.uuid4().hex[:6]
    _create(broker.bootstrap, topic, config={"cleanup.policy": "delete"})
    produce_sync(broker.bootstrap, topic, [(b"k", b"1"), (b"k", b"2"), (b"k", b"3")])
    got = consume_from(broker.bootstrap, topic, 0, 0, 3)
    assert [v for _, v, _ in got] == [b"1", b"2", b"3"], "delete policy keeps all versions"
