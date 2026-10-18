"""Tests for KFK-14 / 01-broken-clusters.

Four live scenarios (native multi-broker clusters, no Docker) and two pure-config
scenarios. Each test deterministically CREATES the fault, asserts the learner's
diagnose names it, applies the learner's fix, and asserts health is restored.

Live scenarios use a fresh small cluster each (independent + deterministic). The
pure-config scenarios make G1 reachable on a bare machine (no broker needed).
Unique topics/groups per test keep shuffled runs safe.
"""

import time
import uuid

from confluent_kafka import Producer, TopicPartition


def _uid():
    return uuid.uuid4().hex[:8]


def _poll_until(fn, ok, timeout=20.0, interval=0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if ok(last):
            return last
        time.sleep(interval)
    return last


# ===========================================================================
# pure config: advertised.listeners
# ===========================================================================
def test_advertised_listeners_diagnose_and_fix(submission):
    props = {"advertised.listeners": "PLAINTEXT://0.0.0.0:9092"}
    d = submission.diagnose_advertised_listeners(props, "kafka-1.internal")
    assert d["issue"] == "wrong_advertised_listeners"
    fixed = submission.fix_advertised_listeners(props, "kafka-1.internal")
    assert fixed["advertised.listeners"] == "PLAINTEXT://kafka-1.internal:9092"
    # after fix, diagnosis is healthy
    assert submission.diagnose_advertised_listeners(
        fixed, "kafka-1.internal")["issue"] == "healthy"


def test_advertised_listeners_healthy_passthrough(submission):
    props = {"advertised.listeners": "PLAINTEXT://host-a:9092,SASL://host-a:9094"}
    assert submission.diagnose_advertised_listeners(props, "host-a")["issue"] == "healthy"


# ===========================================================================
# pure config: reassignment throttle
# ===========================================================================
def test_throttle_diagnose_and_fix(submission):
    cfg = {"leader.replication.throttled.rate": "1024",
           "follower.replication.throttled.rate": "1024"}
    floor = 10 * 1024 * 1024
    d = submission.diagnose_throttle(cfg, floor)
    assert d["issue"] == "throttle_too_low"
    fixed = submission.fix_throttle(cfg, floor)
    assert submission.diagnose_throttle(fixed, floor)["issue"] == "healthy"
    assert int(fixed["leader.replication.throttled.rate"]) >= floor


def test_throttle_healthy(submission):
    floor = 1024
    cfg = {"leader.replication.throttled.rate": "100000",
           "follower.replication.throttled.rate": "100000"}
    assert submission.diagnose_throttle(cfg, floor)["issue"] == "healthy"


# ===========================================================================
# live: broker-loss cluster — under-replicated partitions AND min.isr violation
# share ONE cluster (kill broker 3 once) to stay within the time budget.
# ===========================================================================
def test_broker_loss_urp_and_min_isr(kafka_cluster, submission):
    c = kafka_cluster(n_brokers=3)
    urp_topic = f"urp-{_uid()}"
    isr_topic = f"isr-{_uid()}"
    c.create_topic(urp_topic, partitions=3, replication_factor=3)
    c.create_topic(isr_topic, partitions=1, replication_factor=3,
                   config={"min.insync.replicas": "3"})
    a = c.admin()

    c.kill_broker(3)

    # --- under-replicated ---
    d = _poll_until(lambda: submission.diagnose_under_replicated(a, urp_topic),
                    lambda r: r["issue"] == "under_replicated")
    assert d["issue"] == "under_replicated"
    assert d["under_replicated_partitions"]

    # --- min.isr violation (still broker 3 down) ---
    dm = _poll_until(lambda: submission.diagnose_min_isr(a, isr_topic),
                     lambda r: r["issue"] == "min_isr_too_high")
    assert dm["issue"] == "min_isr_too_high"

    def produce():
        p = Producer({"bootstrap.servers": c.bootstrap_servers, "acks": "all",
                      "message.timeout.ms": 5000})
        errs = []
        p.produce(isr_topic, value=b"x", on_delivery=lambda e, m: errs.append(e))
        p.flush(7)
        return errs[0] if errs else None

    assert produce() is not None, "expected acks=all to fail before fix"
    submission.fix_min_isr(c, isr_topic, 2)
    healthy_isr = _poll_until(lambda: submission.diagnose_min_isr(a, isr_topic),
                              lambda r: r["issue"] == "healthy", timeout=20.0)
    assert healthy_isr["issue"] == "healthy"
    assert produce() is None, "acks=all should succeed after lowering min.isr"

    # --- fix the URP by restoring the broker ---
    submission.fix_under_replicated(c, 3)
    healthy = _poll_until(lambda: submission.diagnose_under_replicated(a, urp_topic),
                          lambda r: r["issue"] == "healthy", timeout=30.0)
    assert healthy["issue"] == "healthy", f"URP not restored: {healthy}"


# ===========================================================================
# live: healthy cluster — retention misconfig AND lagging group (no kills)
# share ONE cluster to stay within the time budget.
# ===========================================================================
def _commit_at(bs, topic, group, offset):
    from confluent_kafka import Consumer, KafkaException

    cons = Consumer({"bootstrap.servers": bs, "group.id": group,
                     "enable.auto.commit": False})
    try:
        deadline = time.time() + 15
        while True:
            try:
                cons.commit(offsets=[TopicPartition(topic, 0, offset)],
                            asynchronous=False)
                return
            except KafkaException:
                if time.time() >= deadline:
                    raise
                time.sleep(0.3)
    finally:
        cons.close()


def test_retention_and_lag(kafka_cluster, submission):
    c = kafka_cluster(n_brokers=3)
    bs = c.bootstrap_servers
    a = c.admin()

    # --- retention too low ---
    ret_topic = f"ret-{_uid()}"
    floor = 60 * 60 * 1000  # 1 hour
    c.create_topic(ret_topic, partitions=1, replication_factor=3,
                   config={"retention.ms": "1000"})
    d = submission.diagnose_retention(a, ret_topic, floor)
    assert d["issue"] == "retention_too_low", d
    submission.fix_retention(c, ret_topic, floor)
    healthy = _poll_until(lambda: submission.diagnose_retention(a, ret_topic, floor),
                          lambda r: r["issue"] == "healthy", timeout=15.0)
    assert healthy["issue"] == "healthy" and healthy["retention_ms"] >= floor

    # --- lagging group ---
    lag_topic = f"lag-{_uid()}"
    group = f"grp-{_uid()}"
    c.create_topic(lag_topic, partitions=1, replication_factor=3)
    p = Producer({"bootstrap.servers": bs})
    for i in range(10):
        p.produce(lag_topic, value=str(i).encode())
    p.flush(15)

    _commit_at(bs, lag_topic, group, 2)  # behind the end (10)
    dl = _poll_until(lambda: submission.diagnose_lag(bs, lag_topic, group),
                     lambda r: r["issue"] == "lagging")
    assert dl["issue"] == "lagging" and dl["total_lag"] == 8, dl

    submission.fix_lag(c, lag_topic, group)
    healthy_lag = _poll_until(lambda: submission.diagnose_lag(bs, lag_topic, group),
                              lambda r: r["issue"] == "healthy", timeout=20.0)
    assert healthy_lag["issue"] == "healthy", f"lag not drained: {healthy_lag}"
