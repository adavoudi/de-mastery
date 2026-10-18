"""Tests for LKH-02 / 01-retry-commit.

Deterministic concurrency: a `before_commit` hook commits a COMPETING writer's
change AFTER the learner reads but BEFORE the learner writes, forcing a genuine
conflict. The oracle is the SERIAL execution of the same deltas; a correct
refresh-and-reapply loop must match it (no lost update). No JVM, no Docker.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo

START = 100


@pytest.fixture
def acct(pyiceberg_catalog):
    cat = pyiceberg_catalog
    cat.create_namespace("bank")
    schema = pa.schema([("id", pa.int64()), ("bal", pa.int64())])
    t = cat.create_table("bank.acct", schema=schema)
    t.append(pa.table({"id": [1], "bal": [START]}))
    return cat, "bank.acct"


def _balance(cat, ident):
    t = cat.load_table(ident)
    return t.scan(row_filter=EqualTo("id", 1)).to_arrow().column("bal").to_pylist()[0]


def _competitor(cat, ident, delta, on_attempts):
    """before_commit hook: a competing writer commits `delta` on each listed attempt
    (between the learner's read and the learner's write), forcing a conflict."""
    def hook(attempt):
        if attempt in on_attempts:
            ct = cat.load_table(ident)
            cur = ct.scan(row_filter=EqualTo("id", 1)).to_arrow().column("bal").to_pylist()[0]
            ct.overwrite(
                pa.table({"id": [1], "bal": [cur + delta]}, schema=ct.schema().as_arrow()),
                overwrite_filter=EqualTo("id", 1),
            )
    return hook


def test_no_conflict_applies_once(submission, acct):
    cat, ident = acct
    out = submission.commit_with_retry(cat, ident, lambda b: b + 5)
    assert out == START + 5
    assert _balance(cat, ident) == START + 5


def test_read_balance(submission, acct):
    cat, ident = acct
    assert submission.read_balance(cat.load_table(ident)) == START


def test_single_conflict_no_lost_update(submission, acct):
    """Competitor commits +10 between the learner's read and write on attempt 0;
    learner adds +5. Serial oracle = 115 (no lost update)."""
    cat, ident = acct
    hook = _competitor(cat, ident, 10, on_attempts={0})
    out = submission.commit_with_retry(cat, ident, lambda b: b + 5, before_commit=hook)
    assert _balance(cat, ident) == START + 10 + 5, "lost update: refresh/reapply failed"
    assert out == START + 10 + 5


def test_two_conflicts_then_success(submission, acct):
    """Competitor wins attempts 0 and 1 (+10 each); learner still lands +5 last.
    Serial oracle = 125."""
    cat, ident = acct
    hook = _competitor(cat, ident, 10, on_attempts={0, 1})
    out = submission.commit_with_retry(
        cat, ident, lambda b: b + 5, max_retries=5, before_commit=hook
    )
    assert _balance(cat, ident) == START + 10 + 10 + 5
    assert out == START + 10 + 10 + 5


def test_matches_serial_oracle(submission, acct):
    """General invariant: concurrent-with-retry == serial execution of the deltas."""
    cat, ident = acct
    oracle = START + 10 + 5
    hook = _competitor(cat, ident, 10, on_attempts={0})
    submission.commit_with_retry(cat, ident, lambda b: b + 5, before_commit=hook)
    assert _balance(cat, ident) == oracle


def test_exhausted_retries_raise(submission, acct):
    """If every attempt conflicts, the final CommitFailedException must propagate
    (not be swallowed, not return a stale value)."""
    cat, ident = acct
    hook = _competitor(cat, ident, 1, on_attempts=set(range(100)))
    with pytest.raises(CommitFailedException):
        submission.commit_with_retry(
            cat, ident, lambda b: b + 5, max_retries=3, before_commit=hook
        )
