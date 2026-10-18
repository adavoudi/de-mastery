"""Tests for KFK-10 / 01-lag-alerting.

Two layers:
  * Pure functions (compute_lag / total_lag / evaluate_alerts) vs an independent
    Python oracle — no broker, so G1 is reachable on a bare machine.
  * measure_group_lag against the native session `kafka_broker` (no Docker):
    produce, copy some, and assert the reported lag equals the true backlog.

Every broker test uses a UNIQUE topic + group so shuffled/parallel runs never
interfere.
"""

import uuid

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


def _uid():
    return uuid.uuid4().hex[:8]


# --------------------------- pure: compute_lag -----------------------------
def _oracle_lag(committed, end):
    out = {}
    for p, e in end.items():
        c = committed.get(p)
        if c is None or c < 0:
            c = 0
        out[p] = max(0, e - c)
    return out


def test_compute_lag_basic(submission):
    end = {0: 10, 1: 5, 2: 7}
    committed = {0: 4, 1: 5, 2: -1001}
    assert submission.compute_lag(committed, end) == {0: 6, 1: 0, 2: 7}


def test_compute_lag_no_committed_is_full_backlog(submission):
    end = {0: 12}
    assert submission.compute_lag({}, end) == {0: 12}
    assert submission.compute_lag({0: None}, end) == {0: 12}


def test_compute_lag_never_negative(submission):
    # committed can momentarily exceed a stale end snapshot; lag floors at 0.
    assert submission.compute_lag({0: 20}, {0: 10}) == {0: 0}


@settings(max_examples=60, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.dictionaries(
    st.integers(min_value=0, max_value=11),
    st.tuples(st.integers(min_value=0, max_value=1000),
              st.integers(min_value=-1001, max_value=1000)),
    min_size=1, max_size=12))
def test_compute_lag_matches_oracle(submission, data):
    end = {p: v[0] for p, v in data.items()}
    committed = {p: v[1] for p, v in data.items()}
    expected = _oracle_lag(committed, end)
    assert submission.compute_lag(committed, end) == expected
    assert submission.total_lag(expected) == sum(expected.values())


# --------------------------- pure: evaluate_alerts -------------------------
def test_alerts_ok_warn_page_boundaries(submission):
    f = submission.evaluate_alerts
    assert f({0: 0, 1: 0}, 100, 1000) == "ok"
    assert f({0: 99}, 100, 1000) == "ok"          # just below warn
    assert f({0: 100}, 100, 1000) == "warn"       # exactly at warn (inclusive)
    assert f({0: 500, 1: 10}, 100, 1000) == "warn"
    assert f({0: 1000}, 100, 1000) == "page"      # exactly at page (inclusive)
    assert f({0: 50, 1: 5000}, 100, 1000) == "page"  # any partition triggers


def test_alerts_highest_severity_wins(submission):
    # one warn + one page => page
    assert submission.evaluate_alerts({0: 150, 1: 2000}, 100, 1000) == "page"


@settings(max_examples=60, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.dictionaries(st.integers(0, 8), st.integers(0, 5000),
                       min_size=1, max_size=9),
       st.integers(1, 1000), st.integers(1001, 5000))
def test_alerts_match_oracle(submission, lag, warn, page):
    worst = max(lag.values())
    expected = "page" if worst >= page else "warn" if worst >= warn else "ok"
    assert submission.evaluate_alerts(lag, warn, page) == expected


# --------------------------- broker: measure_group_lag ---------------------
def _produce(bootstrap, topic, n):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap})
    for i in range(n):
        p.produce(topic, key=b"k", value=str(i).encode())
    p.flush(15)


def test_measure_lag_after_partial_copy(kafka_broker, submission):
    bs = kafka_broker.bootstrap_servers
    src = f"src-{_uid()}"
    dst = f"dst-{_uid()}"
    group = f"grp-{_uid()}"
    # single partition keeps offset accounting unambiguous
    kafka_broker.create_topic(src, partitions=1, replication_factor=1)
    kafka_broker.create_topic(dst, partitions=1, replication_factor=1)

    _produce(bs, src, 10)
    copied = submission.copy_batch(bs, src, dst, group, max_records=4)
    assert copied == 4

    lag = submission.measure_group_lag(bs, src, group)
    # 10 produced, 4 committed by the copier => backlog of 6
    assert submission.total_lag(lag) == 6


def test_measure_lag_rises_then_drains(kafka_broker, submission):
    bs = kafka_broker.bootstrap_servers
    src = f"src-{_uid()}"
    dst = f"dst-{_uid()}"
    group = f"grp-{_uid()}"
    kafka_broker.create_topic(src, partitions=1, replication_factor=1)
    kafka_broker.create_topic(dst, partitions=1, replication_factor=1)

    _produce(bs, src, 5)
    submission.copy_batch(bs, src, dst, group, max_records=5)
    assert submission.total_lag(submission.measure_group_lag(bs, src, group)) == 0

    # new data arrives but is not consumed => lag rises
    _produce(bs, src, 7)
    assert submission.total_lag(submission.measure_group_lag(bs, src, group)) == 7

    # drain it => back to 0
    submission.copy_batch(bs, src, dst, group, max_records=100)
    assert submission.total_lag(submission.measure_group_lag(bs, src, group)) == 0


def test_measure_lag_no_commit_is_full_backlog(kafka_broker, submission):
    bs = kafka_broker.bootstrap_servers
    src = f"src-{_uid()}"
    group = f"grp-{_uid()}"   # never used by any consumer
    kafka_broker.create_topic(src, partitions=1, replication_factor=1)
    _produce(bs, src, 8)
    lag = submission.measure_group_lag(bs, src, group)
    assert submission.total_lag(lag) == 8
