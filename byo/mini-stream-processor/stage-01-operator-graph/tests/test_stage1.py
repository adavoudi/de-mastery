"""Stage 1 — operator graph + push-based execution.

Black-box against the public API via the `submission` fixture. Oracle: an
independent Python list comprehension over the same inputs (G6).
"""


def _events(submission, n):
    return [submission.Event(value=i) for i in range(n)]


def test_identity_source_sink(submission):
    env = submission.Environment()
    recs = _events(submission, 5)
    out = submission.collect(env.source(recs).sink())
    assert [e.value for e in out] == [0, 1, 2, 3, 4]


def test_map_doubles(submission):
    env = submission.Environment()
    recs = _events(submission, 6)
    out = submission.collect(
        env.source(recs).map(lambda e: submission.Event(value=e.value * 2)).sink())
    assert [e.value for e in out] == [0, 2, 4, 6, 8, 10]


def test_filter_keeps_predicate(submission):
    env = submission.Environment()
    recs = _events(submission, 10)
    out = submission.collect(
        env.source(recs).filter(lambda e: e.value % 2 == 0).sink())
    assert [e.value for e in out] == [0, 2, 4, 6, 8]


def test_chained_map_filter_matches_oracle(submission):
    env = submission.Environment()
    recs = _events(submission, 10)
    out = submission.collect(
        env.source(recs)
        .map(lambda e: submission.Event(value=e.value * 3))
        .filter(lambda e: e.value % 2 == 0)
        .sink())
    oracle = [i * 3 for i in range(10) if (i * 3) % 2 == 0]
    assert [e.value for e in out] == oracle


def test_order_is_preserved(submission):
    """Push-based: records flow one at a time, downstream order == source order."""
    env = submission.Environment()
    recs = [submission.Event(value=v) for v in [5, 1, 4, 1, 3]]
    out = submission.collect(env.source(recs).map(lambda e: e).sink())
    assert [e.value for e in out] == [5, 1, 4, 1, 3]


def test_markers_do_not_reach_sink(submission):
    """Watermark/Barrier markers flow through but are dropped at the Sink."""
    env = submission.Environment()
    recs = [submission.Event(value=1), submission.Watermark(10),
            submission.Event(value=2), submission.Barrier(1),
            submission.Event(value=3)]
    out = submission.collect(env.source(recs).sink())
    assert [e.value for e in out] == [1, 2, 3]


def test_no_sink_returns_data_events(submission):
    env = submission.Environment()
    recs = [submission.Event(value=1), submission.Watermark(5),
            submission.Event(value=2)]
    out = submission.collect(env.source(recs).map(lambda e: e))
    # markers excluded; the two data events remain
    assert [e.value for e in out] == [1, 2]


def test_empty_source(submission):
    env = submission.Environment()
    out = submission.collect(env.source([]).map(lambda e: e).sink())
    assert out == []
