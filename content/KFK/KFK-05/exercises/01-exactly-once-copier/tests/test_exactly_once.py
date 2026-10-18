"""Tests for KFK-05 exactly-once transactional copier.

Uses the session ``kafka_broker`` (no Docker). Unique topics + ids per test. The
oracle (G6) is the set/multiset of source values: the committed destination must
equal it exactly (no loss, no duplicate). The abort test asserts all-or-nothing.

We drain the destination with a read_committed consumer using an idle timeout
(NOT watermark offsets): aborted/committed transaction markers advance the log
end offset, so counting readable *records* is the correct, deterministic check.
"""

import uuid

import pytest


def _ids():
    s = uuid.uuid4().hex[:8]
    return (f"src-{s}", f"dst-{s}", f"grp-{s}", f"txn-{s}")


def _produce(kafka_broker, topic, values):
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": kafka_broker.bootstrap_servers})
    for v in values:
        p.produce(topic, key="k", value=v)
    p.flush(15)


def _read_committed(bootstrap_servers, topic, expected_at_most, idle_s=5):
    """Drain a topic with a read_committed consumer; stop after `idle_s` with no
    new record, or once expected_at_most are read. Returns the list of values."""
    import time

    from confluent_kafka import Consumer

    c = Consumer({"bootstrap.servers": bootstrap_servers,
                  "group.id": f"verify-{uuid.uuid4().hex}",
                  "enable.auto.commit": False,
                  "auto.offset.reset": "earliest",
                  "isolation.level": "read_committed"})
    c.subscribe([topic])
    vals = []
    last_new = time.time()
    hard_deadline = time.time() + 25
    while time.time() < hard_deadline:
        if len(vals) >= expected_at_most and expected_at_most >= 0:
            break
        msg = c.poll(1.0)
        if msg is None or msg.error():
            if time.time() - last_new > idle_s:
                break
            continue
        vals.append(msg.value().decode())
        last_new = time.time()
    c.close()
    return vals


def test_exactly_once_ledger_invariants(kafka_broker, submission):
    src, dst, grp, txn = _ids()
    kafka_broker.create_topic(src, partitions=1, replication_factor=1)
    kafka_broker.create_topic(dst, partitions=1, replication_factor=1)
    values = [f"v{i}" for i in range(8)]
    _produce(kafka_broker, src, values)

    copied = submission.copy_exactly_once(
        kafka_broker.bootstrap_servers, src, dst, grp, txn, len(values))
    assert copied == len(values)

    got = _read_committed(kafka_broker.bootstrap_servers, dst, len(values))
    # No loss: every source value present. No duplicate: each exactly once.
    assert sorted(got) == sorted(values)                 # kills 'double-produce'
    assert len(got) == len(set(got)) == len(values)      # no dup, no loss


def test_abort_is_all_or_nothing(kafka_broker, submission):
    src, dst, grp, txn = _ids()
    kafka_broker.create_topic(src, partitions=1, replication_factor=1)
    kafka_broker.create_topic(dst, partitions=1, replication_factor=1)
    values = [f"v{i}" for i in range(6)]
    _produce(kafka_broker, src, values)

    produced = submission.copy_then_abort(
        kafka_broker.bootstrap_servers, src, dst, grp, txn, len(values))
    assert produced == len(values)

    # A read_committed consumer must see NONE of the aborted records.
    got = _read_committed(kafka_broker.bootstrap_servers, dst,
                          expected_at_most=-1, idle_s=4)
    assert got == [], got        # kills 'abort-is-commit' and 'non-transactional'


def test_commit_then_more_committed_only(kafka_broker, submission):
    """After an aborted batch, a subsequent committed copy of the SAME source
    makes exactly the source set visible (abort left nothing behind)."""
    src, dst, grp, txn = _ids()
    kafka_broker.create_topic(src, partitions=1, replication_factor=1)
    kafka_broker.create_topic(dst, partitions=1, replication_factor=1)
    values = [f"v{i}" for i in range(5)]
    _produce(kafka_broker, src, values)

    # abort first (consumes nothing durably because offsets aren't committed)
    submission.copy_then_abort(
        kafka_broker.bootstrap_servers, src, dst, grp, txn, len(values))
    # now a clean copy with the SAME group resumes from the start and commits
    submission.copy_exactly_once(
        kafka_broker.bootstrap_servers, src, dst, grp, txn, len(values))

    got = _read_committed(kafka_broker.bootstrap_servers, dst, len(values))
    assert sorted(got) == sorted(values)
    assert len(got) == len(values)       # no duplicate from the aborted attempt
