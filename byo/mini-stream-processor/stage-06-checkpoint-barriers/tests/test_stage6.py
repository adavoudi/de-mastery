"""Stage 6 — checkpoint barriers (Chandy-Lamport / Flink ABS).

A barrier flows through the operator; the operator snapshots its state exactly
when the barrier passes, together with the source offset consumed so far. The
snapshot is CONSISTENT: it reflects precisely the records before the barrier.
Oracle: an independent prefix fold up to each barrier's offset.
"""


def _events(submission, pairs):
    return [submission.Event(key=k, value=v) for k, v in pairs]


def _prefix_sum(pairs, offset):
    acc = {}
    for k, v in pairs[:offset]:
        acc[k] = acc.get(k, 0) + v
    return acc


def test_inject_barriers_schedule(submission):
    recs = _events(submission, [("a", 1)] * 6)
    marked = submission.inject_barriers(recs, every=2)
    kinds = [type(x).__name__ for x in marked]
    # a Barrier after every 2 Events
    assert kinds == ["Event", "Event", "Barrier",
                     "Event", "Event", "Barrier",
                     "Event", "Event", "Barrier"]
    bids = [x.id for x in marked if isinstance(x, submission.Barrier)]
    assert bids == [1, 2, 3]


def test_outputs_are_running_sum(submission):
    pairs = [("a", 1), ("a", 2), ("a", 3)]
    outs, _ = submission.run_with_checkpoints(
        _events(submission, pairs), lambda e: e.key, barrier_every=2)
    assert outs == [("a", 1), ("a", 3), ("a", 6)]


def test_snapshot_is_consistent_with_offset(submission):
    pairs = [("a", 1), ("b", 10), ("a", 2), ("b", 20), ("a", 3), ("b", 30)]
    outs, cps = submission.run_with_checkpoints(
        _events(submission, pairs), lambda e: e.key, barrier_every=2)
    assert set(cps) == {1, 2, 3}
    for cid, cp in cps.items():
        # the operator state at the barrier equals the fold over the first
        # `source_offset` records — a consistent cut (G6 oracle)
        assert cp.operator_states["sum"] == _prefix_sum(pairs, cp.source_offset)
    # offsets advance exactly 2 per barrier
    assert [cps[c].source_offset for c in (1, 2, 3)] == [2, 4, 6]


def test_snapshot_captures_before_barrier_only(submission):
    """State in checkpoint N must NOT include records that come after barrier N."""
    pairs = [("a", 1), ("a", 1), ("a", 1), ("a", 1)]
    _, cps = submission.run_with_checkpoints(
        _events(submission, pairs), lambda e: e.key, barrier_every=2)
    assert cps[1].operator_states["sum"] == {"a": 2}   # not 3 or 4
    assert cps[2].operator_states["sum"] == {"a": 4}


def test_barrier_forwarded_downstream(submission):
    """StatefulSumOperator forwards the Barrier (so it can reach later ops)."""
    op = submission.StatefulSumOperator(lambda e: e.key)
    seen = []
    op.process(submission.Event(key="a", value=1), seen.append)
    op.process(submission.Barrier(7), seen.append)
    assert any(isinstance(x, submission.Barrier) and x.id == 7 for x in seen)
    assert op.checkpoints[7] == {"a": 1}
