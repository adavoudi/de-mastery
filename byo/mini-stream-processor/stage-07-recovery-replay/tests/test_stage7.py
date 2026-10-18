"""Stage 7 — recovery from a checkpoint with a replayable source (exactly-once).

The job snapshots state + source offset at each barrier. It CRASHES
deterministically after exactly ``crash_after`` records (a seeded fault
schedule — no randomness, no sleeps). On recovery it restores the last completed
checkpoint and REPLAYS the source from that checkpoint's offset.

G6 invariant: for EVERY crash point, the recovered final state must equal the
batch oracle (a clean fold over all records) — no double counting of the records
between the checkpoint and the crash, and no loss.
"""


def _batch_oracle(pairs):
    acc = {}
    for k, v in pairs:
        acc[k] = acc.get(k, 0) + v
    return acc


def _events(submission, pairs):
    return [submission.Event(key=k, value=v) for k, v in pairs]


def test_replayable_source_from_offset(submission):
    recs = _events(submission, [("a", i) for i in range(5)])
    src = submission.ReplayableSource(recs)
    assert [e.value for e in src.replay_from(0)] == [0, 1, 2, 3, 4]
    assert [e.value for e in src.replay_from(2)] == [2, 3, 4]
    assert src.replay_from(5) == []


def test_no_crash_equals_oracle(submission):
    pairs = [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5)]
    got = submission.run_with_recovery(
        _events(submission, pairs), lambda e: e.key,
        barrier_every=2, crash_after=10 ** 9)
    assert got == _batch_oracle(pairs)


def test_recovery_exactly_once_every_crash_point(submission):
    """The core G6 test: inject a crash after EACH possible record count and
    assert the recovered output equals the batch oracle every time."""
    pairs = [("a" if i % 2 == 0 else "b", (i % 5) + 1) for i in range(12)]
    oracle = _batch_oracle(pairs)
    for crash_after in range(0, len(pairs) + 1):
        for every in (1, 2, 3, 4):
            got = submission.run_with_recovery(
                _events(submission, pairs), lambda e: e.key,
                barrier_every=every, crash_after=crash_after)
            assert got == oracle, (crash_after, every, got, oracle)


def test_recovery_does_not_double_count(submission):
    """Records between the last checkpoint and the crash must be replayed, not
    double-applied. With barrier_every=3 and a crash right before a barrier,
    naive 'resume from where we crashed' or 'replay from 0 onto kept state'
    would both be wrong."""
    pairs = [("a", 1)] * 7
    oracle = _batch_oracle(pairs)   # {"a": 7}
    for crash_after in range(0, 8):
        got = submission.run_with_recovery(
            _events(submission, pairs), lambda e: e.key,
            barrier_every=3, crash_after=crash_after)
        assert got == oracle, (crash_after, got)


def test_no_checkpoint_yet_replays_from_start(submission):
    """Crash before the first barrier -> no checkpoint -> replay from offset 0."""
    pairs = [("a", 1), ("a", 1)]
    got = submission.run_with_recovery(
        _events(submission, pairs), lambda e: e.key,
        barrier_every=5, crash_after=1)
    assert got == _batch_oracle(pairs)
