"""Stage 2 — keyed partitioning across workers (keyBy).

A key is routed to exactly one worker by a STABLE hash, so a keyed reduce gives
the same answer regardless of parallelism (the key-group invariant). Oracle: an
independent per-key fold (G6). A Hypothesis property checks determinism and
parallelism-invariance across random keyed streams.
"""

from collections import defaultdict

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


def _events(submission, pairs):
    return [submission.Event(key=k, value=v) for k, v in pairs]


def _oracle_sum(pairs):
    acc = defaultdict(int)
    for k, v in pairs:
        acc[k] += v
    return dict(acc)


def test_keyed_sum_matches_oracle(submission):
    env = submission.Environment()
    pairs = [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5), ("a", 6)]
    ks = env.source(_events(submission, pairs)).key_by(lambda e: e.key)
    out = submission.run_parallel(ks.reduce(lambda a, b: a + b, parallelism=1))
    assert out == _oracle_sum(pairs)


def test_parallelism_invariance(submission):
    env = submission.Environment()
    pairs = [("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5), ("a", 6),
             ("d", 7), ("e", 8), ("f", 9)]
    ks = env.source(_events(submission, pairs)).key_by(lambda e: e.key)
    r1 = submission.run_parallel(ks.reduce(lambda a, b: a + b, parallelism=1))
    r3 = submission.run_parallel(ks.reduce(lambda a, b: a + b, parallelism=3))
    r7 = submission.run_parallel(ks.reduce(lambda a, b: a + b, parallelism=7))
    assert r1 == r3 == r7 == _oracle_sum(pairs)


def test_partition_of_is_in_range_and_stable(submission):
    for n in (1, 2, 4, 8):
        parts = {submission.partition_of(k, n) for k in ["a", "b", "c", "x", 1, 2]}
        assert all(0 <= p < n for p in parts)
    # stable across calls (not salted like Python's hash())
    assert submission.partition_of("acme", 8) == submission.partition_of("acme", 8)


def test_each_key_on_one_worker(submission):
    keys = ["a", "b", "c", "d", "e", "f", "g", "h"]
    asg = submission.worker_assignment(keys, 4)
    # every key maps to a single, in-range worker
    assert set(asg) == set(keys)
    assert all(0 <= w < 4 for w in asg.values())
    # and matches partition_of
    assert all(asg[k] == submission.partition_of(k, 4) for k in keys)


def test_stable_hash_cross_process(submission):
    """stable_hash AND partition routing must NOT depend on PYTHONHASHSEED (run
    in a child with a different seed and compare)."""
    import os
    import subprocess
    import sys
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]);"
        "import processor;"
        "print(processor.stable_hash('acme'));"
        "print(processor.partition_of('acme', 8));"
        "print(sorted(processor.worker_assignment("
        "['a','b','c','d','e','f','g','h'], 4).items()))"
    )
    env = dict(os.environ, PYTHONHASHSEED="12345")
    r = subprocess.run(
        [sys.executable, "-c", code, os.environ["DE_SUBMISSION_DIR"]],
        capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    lines = r.stdout.strip().splitlines()
    assert int(lines[0]) == submission.stable_hash("acme")
    # partition routing is identical across the differently-seeded interpreter
    assert int(lines[1]) == submission.partition_of("acme", 8)
    keys = ["a", "b", "c", "d", "e", "f", "g", "h"]
    assert lines[2] == repr(sorted(submission.worker_assignment(keys, 4).items()))


@settings(max_examples=30, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(pairs=st.lists(
    st.tuples(st.sampled_from(["a", "b", "c", "d", "e"]),
              st.integers(min_value=-5, max_value=5)),
    min_size=0, max_size=40))
def test_property_invariance(submission, pairs):
    env = submission.Environment()
    ks = env.source(_events(submission, pairs)).key_by(lambda e: e.key)
    r1 = submission.run_parallel(ks.reduce(lambda a, b: a + b, parallelism=1))
    r5 = submission.run_parallel(ks.reduce(lambda a, b: a + b, parallelism=5))
    assert r1 == r5 == _oracle_sum(pairs)
