"""Stage 8 (optional) — transactional (2PC) sink: end-to-end exactly-once.

The sink stages writes into a per-checkpoint transaction, PRE-COMMITS on each
barrier (durable but invisible) and COMMITS on the checkpoint-complete
notification (making the records visible). On recovery, prepared-but-not-notified
transactions are ABORTED and the source is replayed from the last completed
checkpoint, so each input appears in the visible output exactly once.

G6 invariant: for every crash point and every notification lag, the committed
output equals the batch oracle (every input exactly once; no duplicates; no loss).
"""


def _records(submission, n):
    return [submission.Event(key="k%d" % (i % 3), value=i, ts=i)
            for i in range(n)]


def _oracle(submission, n):
    return sorted((e.key, e.value, e.ts) for e in _records(submission, n))


def test_transactional_sink_two_phase(submission):
    s = submission.TransactionalSink()
    s.write(("a", 1))
    s.write(("a", 2))
    s.pre_commit(1)                 # prepared, not visible
    assert s.committed == []
    s.write(("a", 3))
    s.pre_commit(2)
    s.commit(1)                     # cid 1 becomes visible
    assert s.committed == [("a", 1), ("a", 2)]
    s.commit(2)
    assert s.committed == [("a", 1), ("a", 2), ("a", 3)]


def test_commit_is_idempotent(submission):
    s = submission.TransactionalSink()
    s.write(("a", 1))
    s.pre_commit(1)
    s.commit(1)
    s.commit(1)                     # duplicate notification -> no double write
    assert s.committed == [("a", 1)]


def test_abort_discards_prepared(submission):
    s = submission.TransactionalSink()
    s.write(("a", 1))
    s.pre_commit(1)
    s.abort(1)
    s.commit(1)                     # aborted txn cannot commit
    assert s.committed == []


def test_clean_run_commits_everything_once(submission):
    n = 10
    got = submission.run_exactly_once_sink(
        _records(submission, n), barrier_every=3, crash_after=10 ** 9)
    assert got == _oracle(submission, n)


def test_eos_every_crash_point_and_lag(submission):
    """The core G6 test: crash after each record count, with commit lag 0 and 1;
    the committed output must equal the batch oracle every time (no dup/no loss,
    uncommitted staging aborted and replayed)."""
    n = 11
    oracle = _oracle(submission, n)
    for crash_after in range(0, n + 1):
        for every in (2, 3, 4):
            for lag in (0, 1):
                got = submission.run_exactly_once_sink(
                    _records(submission, n), barrier_every=every,
                    crash_after=crash_after, commit_lag=lag)
                assert got == oracle, (crash_after, every, lag, got)


def test_no_duplicates_in_committed(submission):
    n = 9
    got = submission.run_exactly_once_sink(
        _records(submission, n), barrier_every=2, crash_after=5, commit_lag=1)
    assert len(got) == len(set(got))        # no duplicate records
    assert got == _oracle(submission, n)    # and exactly the batch oracle
