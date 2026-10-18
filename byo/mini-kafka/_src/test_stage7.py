"""Stage 7 — consumer-group offsets (OffsetCommit / OffsetFetch).

Black-box: a real librdkafka Consumer commits an offset for a (group, topic,
partition) and a *fresh* consumer in the same group fetches it back. An unknown
group fetches -1 (no committed offset). Uses manual assignment so the group
protocol (JoinGroup) is not required — only OffsetCommit/OffsetFetch +
FindCoordinator.
"""
import uuid

from confluent_kafka import Consumer, TopicPartition

from _broker_harness import produce_sync


def _consumer(bootstrap, group):
    return Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})


def test_commit_then_fetch_same_group(broker):
    topic = "oc-" + uuid.uuid4().hex[:6]
    group = "g-" + uuid.uuid4().hex[:6]
    produce_sync(broker.bootstrap, topic, [(None, b"x")] * 10)
    c1 = _consumer(broker.bootstrap, group)
    c1.assign([TopicPartition(topic, 0, 0)])
    c1.commit(offsets=[TopicPartition(topic, 0, 7)], asynchronous=False)
    c1.close()

    c2 = _consumer(broker.bootstrap, group)
    committed = c2.committed([TopicPartition(topic, 0)], timeout=5)
    c2.close()
    assert committed[0].offset == 7, f"expected committed offset 7, got {committed[0].offset}"


def test_unknown_group_has_no_offset(broker):
    topic = "oc2-" + uuid.uuid4().hex[:6]
    produce_sync(broker.bootstrap, topic, [(None, b"y")] * 3)
    c = _consumer(broker.bootstrap, "nobody-" + uuid.uuid4().hex[:6])
    committed = c.committed([TopicPartition(topic, 0)], timeout=5)
    c.close()
    # confluent-kafka reports an unset committed offset as OFFSET_INVALID (-1001)
    assert committed[0].offset in (-1, -1001), \
        f"unknown group must have no committed offset, got {committed[0].offset}"


def test_offsets_isolated_between_groups(broker):
    topic = "oc3-" + uuid.uuid4().hex[:6]
    produce_sync(broker.bootstrap, topic, [(None, b"z")] * 10)
    ga, gb = "ga-" + uuid.uuid4().hex[:6], "gb-" + uuid.uuid4().hex[:6]
    ca = _consumer(broker.bootstrap, ga)
    ca.commit(offsets=[TopicPartition(topic, 0, 4)], asynchronous=False)
    ca.close()
    cb = _consumer(broker.bootstrap, gb)
    cb.commit(offsets=[TopicPartition(topic, 0, 9)], asynchronous=False)
    comm_a = _consumer(broker.bootstrap, ga).committed([TopicPartition(topic, 0)], timeout=5)
    comm_b = cb.committed([TopicPartition(topic, 0)], timeout=5)
    cb.close()
    assert comm_a[0].offset == 4
    assert comm_b[0].offset == 9
