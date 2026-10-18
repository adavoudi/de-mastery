"""Tests for KFK-02 kill-a-broker.

Two layers:
  * A pure-function layer checks the durability predictors against the full
    decision table (the oracle), with NO cluster. This is the no-Docker graded
    part that fails for the starter and passes for the solution (gate G1/G2
    reachable even if Kafka is missing).
  * A live layer starts a native 3-broker RF=3 / min.insync.replicas=2 cluster,
    kills a broker, and verifies the cluster still serves and that
    produce_and_verify reads back exactly the durably-committed records.

Each live test builds its own cluster via the kafka_cluster factory and uses a
unique topic. Readiness is polled (AdminClient), never slept.
"""

import time
import uuid

import pytest

ACKS = ["0", "1", "all"]


def _oracle_accepted(acks, min_isr, isr_size):
    if isr_size < 1:
        return False
    if acks in ("0", "1"):
        return True
    return isr_size >= min_isr


def _oracle_durable(acks, min_isr, isr_size):
    if not _oracle_accepted(acks, min_isr, isr_size):
        return False
    return acks == "all" and min_isr >= 2 and isr_size >= 2


# --------------------------------------------------------------------------- #
# pure-function layer (no cluster)
# --------------------------------------------------------------------------- #
def test_write_accepted_matches_table(submission):
    for acks in ACKS:
        for min_isr in (1, 2, 3):
            for isr_size in (0, 1, 2, 3):
                got = submission.write_accepted(acks, min_isr, isr_size)
                exp = _oracle_accepted(acks, min_isr, isr_size)
                assert got == exp, (acks, min_isr, isr_size, got, exp)


def test_durability_matches_table(submission):
    for acks in ACKS:
        for min_isr in (1, 2, 3):
            for isr_size in (0, 1, 2, 3):
                got = submission.durable_after_one_more_loss(acks, min_isr, isr_size)
                exp = _oracle_durable(acks, min_isr, isr_size)
                assert got == exp, (acks, min_isr, isr_size, got, exp)


def test_key_cases_spelled_out(submission):
    # acks=all, misr=2, ISR=2: accepted AND durable against one more loss.
    assert submission.write_accepted("all", 2, 2) is True
    assert submission.durable_after_one_more_loss("all", 2, 2) is True
    # acks=all, misr=2, ISR=1: rejected (NOT_ENOUGH_REPLICAS).
    assert submission.write_accepted("all", 2, 1) is False
    # acks=1 is accepted on the leader but never durable against one more loss.
    assert submission.write_accepted("1", 2, 2) is True
    assert submission.durable_after_one_more_loss("1", 2, 2) is False
    # acks=all with misr=1 can be accepted on one replica -> not durable.
    assert submission.write_accepted("all", 1, 1) is True
    assert submission.durable_after_one_more_loss("all", 1, 1) is False


# --------------------------------------------------------------------------- #
# live layer (native 3-broker cluster)
# --------------------------------------------------------------------------- #
def _wait_brokers(cluster, n, timeout=60):
    from confluent_kafka.admin import AdminClient

    a = AdminClient({"bootstrap.servers": cluster.bootstrap_servers})
    deadline = time.time() + timeout
    while time.time() < deadline:
        md = a.list_topics(timeout=5)
        if len(md.brokers) == n:
            return
        time.sleep(0.25)
    raise AssertionError(f"brokers != {n} within {timeout}s")


def test_live_durability_and_failover(kafka_cluster, submission):
    """One 3-broker cluster exercises: (a) read-back counts all committed
    records (kills verify-trusts-attempts), (b) healthy acks=all is durable,
    (c) after killing a broker the cluster re-elects and still serves."""
    c = kafka_cluster(n_brokers=3)

    # Create ALL topics while the cluster is healthy so every partition has a
    # full RF=3 replica set and a stable leader BEFORE we induce a failure.
    topic = f"durable-{uuid.uuid4().hex[:8]}"
    topic2 = f"healthy-{uuid.uuid4().hex[:8]}"
    topic3 = f"survive-{uuid.uuid4().hex[:8]}"
    for t, parts in ((topic, 3), (topic2, 3), (topic3, 1)):
        c.create_topic(t, partitions=parts, replication_factor=3,
                       config={"min.insync.replicas": "2"})

    # (a) pre-seed M, produce N with acks=all, read back M+N.
    from confluent_kafka import Producer
    pre = Producer({"bootstrap.servers": c.bootstrap_servers, "acks": "all"})
    for i in range(3):
        pre.produce(topic, key="seed", value=f"s{i}")
    pre.flush(15)
    records = [("k", str(i)) for i in range(5)]
    assert submission.produce_and_verify(
        c.bootstrap_servers, topic, records, acks="all") == 3 + 5

    # (b) healthy acks=all on a fresh topic reads back fully.
    assert submission.produce_and_verify(
        c.bootstrap_servers, topic2, [("k", str(i)) for i in range(10)],
        acks="all") == 10

    # (c) kill a broker that is NOT topic3's leader, so topic3's single-partition
    #     leader never changes (fully deterministic). The ISR shrinks 3->2 which
    #     still satisfies min.insync.replicas=2, so writes keep succeeding.
    from confluent_kafka.admin import AdminClient
    a = AdminClient({"bootstrap.servers": c.bootstrap_servers})
    md = a.list_topics(topic3, timeout=10)
    p0 = md.topics[topic3].partitions[0]
    leader = p0.leader
    victim = next(r for r in p0.replicas if r != leader)
    c.kill_broker(victim)
    assert c.num_alive() == 2
    _wait_brokers(c, 2)

    # Write-readiness probe: block until the cluster actually accepts a write to
    # topic3 again (metadata caches have converged on the surviving leader),
    # so the graded call below is deterministic rather than racing a refresh.
    from confluent_kafka import Producer as _P
    _probe = _P({"bootstrap.servers": c.bootstrap_servers, "acks": "1",
                 "retries": 10, "message.timeout.ms": 15000})
    _ok = {"n": 0}
    for _ in range(20):
        _ok["n"] = 0
        _probe.produce(topic3, key="ready", value="1",
                       callback=lambda e, m: _ok.__setitem__("n", _ok["n"] + (e is None)))
        _probe.flush(15)
        if _ok["n"] == 1:
            break
    assert _ok["n"] == 1, "cluster did not become write-ready after failover"

    recs = [("k" + str(i % 3), str(i)) for i in range(12)]
    # topic3 now also holds the single 'ready' probe record, so expect 12 + 1.
    assert submission.produce_and_verify(
        c.bootstrap_servers, topic3, recs, acks="1") == 12 + 1
    c.start_broker(victim)
    assert c.num_alive() == 3
