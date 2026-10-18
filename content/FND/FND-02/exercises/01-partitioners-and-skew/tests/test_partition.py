"""Tests for FND-02/01-partitioners-and-skew.

Property-based (Hypothesis) where natural: partitioner range, determinism, key
stability, balance, and range monotonicity. Example-based tests pin the skew signal
and the skew_report math.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

P_VALUES = st.integers(min_value=1, max_value=16)
KEYS = st.lists(st.integers(min_value=-50, max_value=50), max_size=200)
STR_KEYS = st.lists(st.text(max_size=6), max_size=200)


# ---------------------------------------------------------------------------
# hash_partition properties
# ---------------------------------------------------------------------------
@settings(max_examples=100, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(keys=KEYS, p=P_VALUES)
def test_hash_assigns_in_range(submission, keys, p):
    out = submission.hash_partition(keys, p)
    assert len(out) == len(keys)
    assert all(0 <= a < p for a in out), f"index out of [0,{p}): {out}"


@settings(max_examples=100, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(keys=STR_KEYS, p=P_VALUES)
def test_hash_is_key_stable(submission, keys, p):
    out = submission.hash_partition(keys, p)
    seen = {}
    for k, a in zip(keys, out):
        if k in seen:
            assert seen[k] == a, f"same key {k!r} mapped to {seen[k]} and {a}"
        seen[k] = a


@settings(max_examples=50, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(keys=KEYS, p=P_VALUES)
def test_hash_is_deterministic(submission, keys, p):
    assert submission.hash_partition(keys, p) == submission.hash_partition(keys, p)


def test_hash_is_reasonably_balanced_on_distinct_keys(submission):
    keys = list(range(10_000))
    p = 8
    rep = submission.skew_report(submission.hash_partition(keys, p), p)
    # Distinct keys over a good hash: each partition within ~15% of the mean.
    assert rep["skew_ratio"] < 1.15, rep


# ---------------------------------------------------------------------------
# range_partition properties
# ---------------------------------------------------------------------------
@settings(max_examples=100, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(keys=KEYS, p=P_VALUES)
def test_range_assigns_in_range(submission, keys, p):
    out = submission.range_partition(keys, p)
    assert len(out) == len(keys)
    assert all(0 <= a < p for a in out)


@settings(max_examples=100, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(keys=st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1,
                     max_size=300),
       p=st.integers(min_value=1, max_value=12))
def test_range_is_monotonic(submission, keys, p):
    out = submission.range_partition(keys, p)
    pairs = sorted(zip(keys, out), key=lambda t: t[0])
    last = -1
    for k, part in pairs:
        assert part >= last, f"non-monotonic: key {k} -> partition {part} < {last}"
        last = part


# ---------------------------------------------------------------------------
# skew detection (the lesson's signal) + skew_report math
# ---------------------------------------------------------------------------
def test_hot_key_produces_high_skew_under_hashing(submission):
    # 90% of rows share one hot key; the rest are distinct.
    keys = ["HOT"] * 900 + [f"k{i}" for i in range(100)]
    p = 8
    rep = submission.skew_report(submission.hash_partition(keys, p), p)
    # One partition holds ~900+ rows; mean is ~125 -> ratio well above 2.
    assert rep["skew_ratio"] > 2.0, rep


def test_skew_report_math_balanced(submission):
    rep = submission.skew_report([0, 1, 2, 3, 0, 1, 2, 3], 4)
    assert rep["counts"] == [2, 2, 2, 2]
    assert rep["max"] == 2
    assert rep["mean"] == 2.0
    assert rep["skew_ratio"] == 1.0


def test_skew_report_counts_empty_partitions_in_mean(submission):
    # 3 of 4 partitions empty; mean must use p=4, not the 1 non-empty partition.
    rep = submission.skew_report([0, 0, 0, 0], 4)
    assert rep["counts"] == [4, 0, 0, 0]
    assert rep["mean"] == 1.0           # 4 rows / 4 partitions
    assert rep["skew_ratio"] == 4.0     # max 4 / mean 1


def test_skew_report_empty_input(submission):
    rep = submission.skew_report([], 4)
    assert rep["counts"] == [0, 0, 0, 0]
    assert rep["skew_ratio"] == 1.0
