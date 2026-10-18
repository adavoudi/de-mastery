"""Tests for FND-04/01-watermarks.

Example-based tests pin the watermark values and the on_time/late_accepted/dropped
labels on a hand-built out-of-order stream. A Hypothesis property enforces watermark
monotonicity across arbitrary arrival orders.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

NEG_INF = float("-inf")


def _recompute(events, allowed_lateness, grace=None):
    """Independent oracle for the expected classification (G6 spirit)."""
    g = allowed_lateness if grace is None else grace
    max_seen = None
    wm = NEG_INF
    rows = []
    for t in events:
        w_before = wm
        if w_before == NEG_INF or t > w_before:
            label = "on_time"
        elif t >= w_before - g:
            label = "late_accepted"
        else:
            label = "dropped"
        rows.append({"event_time": t, "watermark_before": w_before, "label": label})
        max_seen = t if max_seen is None else max(max_seen, t)
        wm = max(wm, max_seen - allowed_lateness)
    return rows


def test_observe_returns_lagged_watermark(submission):
    gen = submission.WatermarkGenerator(5)
    assert gen.current() == NEG_INF
    assert gen.observe(100) == 95
    assert gen.observe(102) == 97
    # A late event does NOT lower the watermark.
    assert gen.observe(90) == 97


def test_first_event_is_always_on_time(submission):
    rows = submission.classify_events([42], 5)
    assert rows[0]["label"] == "on_time"
    assert rows[0]["watermark_before"] == NEG_INF


def test_handbuilt_stream_matches_oracle(submission):
    events = [100, 102, 101, 90, 108, 101, 85]
    got = submission.classify_events(events, 5)
    exp = _recompute(events, 5)
    assert got == exp
    labels = [r["label"] for r in got]
    # The stream exercises all three categories.
    assert "on_time" in labels and "late_accepted" in labels and "dropped" in labels


def test_zero_lateness_frontier_event_is_on_time(submission):
    # With lateness=0, an event strictly greater than the current watermark is
    # on_time. This fails if you classify AFTER observing (it would look late).
    rows = submission.classify_events([100, 105], 0)
    assert rows[1]["event_time"] == 105
    assert rows[1]["label"] == "on_time"
    assert rows[1]["watermark_before"] == 100


def test_late_within_grace_is_accepted_not_dropped(submission):
    # 108 advances W to 103 (lateness=5); then 100 is within grace [98,103] -> accepted.
    rows = submission.classify_events([108, 100], 5)
    assert rows[1]["label"] == "late_accepted"


@settings(max_examples=200, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(events=st.lists(st.integers(min_value=-100, max_value=100), min_size=1,
                       max_size=200),
       lateness=st.integers(min_value=0, max_value=20))
def test_watermark_is_monotonic(submission, events, lateness):
    gen = submission.WatermarkGenerator(lateness)
    last = NEG_INF
    for t in events:
        w = gen.observe(t)
        assert w >= last, f"watermark decreased from {last} to {w} on event {t}"
        last = w


@settings(max_examples=150, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(events=st.lists(st.integers(min_value=-50, max_value=50), min_size=1,
                       max_size=120),
       lateness=st.integers(min_value=0, max_value=10))
def test_classify_matches_oracle_property(submission, events, lateness):
    assert submission.classify_events(events, lateness) == _recompute(events, lateness)
