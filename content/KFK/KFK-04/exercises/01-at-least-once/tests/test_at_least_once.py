"""Tests for KFK-04 at-least-once with manual commits.

Uses the session ``kafka_broker`` (no Docker). A single-partition topic makes
the crash point deterministic. We model a crash by a sink that raises on a
chosen record; the resuming run (same group.id) must redeliver from the last
committed offset — no record skipped (at-least-once).
"""

import uuid

import pytest


class _Crash(RuntimeError):
    pass


def _produce(kafka_broker, topic, n):
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": kafka_broker.bootstrap_servers})
    for i in range(n):
        p.produce(topic, key="k", value=str(i))
    p.flush(15)


def test_no_loss_across_crash(kafka_broker, submission):
    topic = f"events-{uuid.uuid4().hex[:8]}"
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    group = f"grp-{uuid.uuid4().hex[:8]}"
    n = 10
    _produce(kafka_broker, topic, n)

    delivered = []          # every value handed to a sink (incl. the crashed one)
    crash_at = 4            # raise when processing the 5th record (value "4")

    def crashing_sink(key, value):
        delivered.append(value)
        if value == str(crash_at):
            raise _Crash("boom")

    # Phase 1: crashes partway. The crash must propagate out of process_batch.
    with pytest.raises(Exception):
        submission.process_batch(
            kafka_broker.bootstrap_servers, topic, group, n, crashing_sink)

    # The crashing record was attempted but (correctly) NOT committed.
    assert str(crash_at) in delivered

    # Phase 2: resume with a normal sink; must pick up from the last commit.
    resumed = []

    def ok_sink(key, value):
        resumed.append(value)

    submission.process_batch(
        kafka_broker.bootstrap_servers, topic, group, n, ok_sink)

    # No loss: every produced value handled at least once across both runs.
    handled = set(delivered) | set(resumed)
    assert handled == {str(i) for i in range(n)}

    # At-least-once: the crashed record was redelivered in phase 2 (so it was
    # delivered more than once overall). A commit-before-work / auto-commit
    # consumer would have skipped it -> this assertion fails for those mutants.
    assert str(crash_at) in resumed


def test_clean_run_processes_everything(kafka_broker, submission):
    topic = f"events-{uuid.uuid4().hex[:8]}"
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    group = f"grp-{uuid.uuid4().hex[:8]}"
    n = 6
    _produce(kafka_broker, topic, n)

    got = []
    count = submission.process_batch(
        kafka_broker.bootstrap_servers, topic, group, n,
        lambda k, v: got.append(v))
    assert count == n
    assert got == [str(i) for i in range(n)]


def test_resume_reads_only_new_after_clean_commit(kafka_broker, submission):
    """After a clean run commits everything, a second run of the same group with
    freshly produced records reads ONLY the new ones (committed offset honored)."""
    topic = f"events-{uuid.uuid4().hex[:8]}"
    kafka_broker.create_topic(topic, partitions=1, replication_factor=1)
    group = f"grp-{uuid.uuid4().hex[:8]}"
    _produce(kafka_broker, topic, 4)

    first = []
    submission.process_batch(kafka_broker.bootstrap_servers, topic, group, 4,
                             lambda k, v: first.append(v))
    assert first == [str(i) for i in range(4)]

    # produce 3 more; a resuming consumer must see only these.
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": kafka_broker.bootstrap_servers})
    for i in range(4, 7):
        p.produce(topic, key="k", value=str(i))
    p.flush(15)

    second = []
    submission.process_batch(kafka_broker.bootstrap_servers, topic, group, 3,
                             lambda k, v: second.append(v))
    assert second == ["4", "5", "6"]
