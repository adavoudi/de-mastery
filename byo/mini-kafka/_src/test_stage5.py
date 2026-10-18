"""Stage 5 — Metadata + multiple topics / partitions.

Black-box: using the AdminClient's metadata, a topic must report the partition
count the broker tracks; producing to a specific partition and consuming that
same partition must be isolated from other partitions and other topics.
"""
import uuid

from _broker_harness import produce_sync, consume_from


def _topic_metadata(bootstrap, topic, timeout=5):
    from confluent_kafka.admin import AdminClient
    a = AdminClient({"bootstrap.servers": bootstrap})
    md = a.list_topics(topic=topic, timeout=timeout)
    return md.topics[topic]


def test_two_topics_are_isolated(broker):
    produce_sync(broker.bootstrap, "ta", [(None, b"a0"), (None, b"a1")])
    produce_sync(broker.bootstrap, "tb", [(None, b"b0")])
    a = consume_from(broker.bootstrap, "ta", 0, 0, 2)
    b = consume_from(broker.bootstrap, "tb", 0, 0, 1)
    assert [v for _, v, _ in a] == [b"a0", b"a1"]
    assert [v for _, v, _ in b] == [b"b0"]


def test_metadata_reports_partitions(broker):
    """Produce to partition 2 of a 3-partition topic; metadata must list >=3
    partitions and the records must be readable from partition 2 only."""
    topic = "multi"
    from confluent_kafka.admin import AdminClient, NewTopic
    a = AdminClient({"bootstrap.servers": broker.bootstrap})
    a.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(15)
    produce_sync(broker.bootstrap, topic, [(None, b"p2a"), (None, b"p2b")], partition=2)
    md = _topic_metadata(broker.bootstrap, topic)
    assert len(md.partitions) >= 3, f"topic must have >=3 partitions, got {len(md.partitions)}"
    got2 = consume_from(broker.bootstrap, topic, 2, 0, 2)
    assert [v for _, v, _ in got2] == [b"p2a", b"p2b"]
    # partition 0 should be empty
    got0 = consume_from(broker.bootstrap, topic, 0, 0, 1, timeout=4)
    assert got0 == []


def test_per_partition_offsets_independent(broker):
    topic = "indep"
    from confluent_kafka.admin import AdminClient, NewTopic
    a = AdminClient({"bootstrap.servers": broker.bootstrap})
    a.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])[topic].result(15)
    produce_sync(broker.bootstrap, topic, [(None, b"x0"), (None, b"x1")], partition=0)
    produce_sync(broker.bootstrap, topic, [(None, b"y0")], partition=1)
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": broker.bootstrap,
                  "group.id": "g-" + uuid.uuid4().hex[:8],
                  "enable.auto.commit": False})
    try:
        _, h0 = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=5)
        _, h1 = c.get_watermark_offsets(TopicPartition(topic, 1), timeout=5)
        assert h0 == 2 and h1 == 1, f"independent end offsets expected (2,1) got ({h0},{h1})"
    finally:
        c.close()
