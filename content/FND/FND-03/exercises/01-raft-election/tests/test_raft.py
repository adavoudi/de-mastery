"""Tests for FND-03/01-raft-election.

Example tests pin each vote rule and the strict-majority election. A Hypothesis
property encodes Raft's core safety invariant: at most one leader per term when votes
are threaded through single-vote-per-term node states.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st


def _node(nid, term=0, voted_for=None, llt=0, lli=0):
    return {"id": nid, "current_term": term, "voted_for": voted_for,
            "last_log_term": llt, "last_log_index": lli}


def _req(cand, term, llt=0, lli=0):
    return {"candidate_id": cand, "term": term,
            "last_log_term": llt, "last_log_index": lli}


# ---------------------------------------------------------------------------
# grant_vote rules
# ---------------------------------------------------------------------------
def test_denies_stale_term(submission):
    granted, _ = submission.grant_vote(_node("n1", term=5), _req("c", term=3))
    assert granted is False


def test_grants_fresh_vote(submission):
    granted, state = submission.grant_vote(_node("n1", term=1), _req("c", term=2))
    assert granted is True
    assert state["voted_for"] == "c"
    assert state["current_term"] == 2


def test_denies_second_vote_other_candidate_same_term(submission):
    node = _node("n1", term=4, voted_for="a")
    granted, _ = submission.grant_vote(node, _req("b", term=4))
    assert granted is False


def test_regrant_same_candidate_is_idempotent(submission):
    node = _node("n1", term=4, voted_for="a")
    granted, state = submission.grant_vote(node, _req("a", term=4))
    assert granted is True
    assert state["voted_for"] == "a"


def test_denies_when_candidate_log_is_stale(submission):
    # Node has (term=3, index=2); candidate has (term=2, index=99) -> staler by term.
    node = _node("n1", term=1, llt=3, lli=2)
    granted, _ = submission.grant_vote(node, _req("c", term=5, llt=2, lli=99))
    assert granted is False


def test_log_comparison_uses_term_then_index(submission):
    # Higher last_log_term wins even with a lower index.
    node = _node("n1", term=1, llt=2, lli=100)
    granted, _ = submission.grant_vote(node, _req("c", term=5, llt=3, lli=1))
    assert granted is True


def test_does_not_mutate_input(submission):
    node = _node("n1", term=1)
    before = dict(node)
    submission.grant_vote(node, _req("c", term=9))
    assert node == before, "grant_vote must not mutate its input node"


# ---------------------------------------------------------------------------
# elect_leader strict majority
# ---------------------------------------------------------------------------
def test_majority_elects(submission):
    nodes = [_node(f"n{i}", term=1) for i in range(5)]
    res = submission.elect_leader(nodes, _req("n0", term=2))
    assert res["needed"] == 3
    assert res["votes"] == 5
    assert res["elected"] is True


def test_tie_does_not_elect(submission):
    # 4 nodes; two already voted for a rival this term -> candidate gets only 2.
    nodes = [
        _node("n0", term=2),
        _node("n1", term=2),
        _node("n2", term=2, voted_for="rival"),
        _node("n3", term=2, voted_for="rival"),
    ]
    res = submission.elect_leader(nodes, _req("c", term=2))
    assert res["needed"] == 3
    assert res["votes"] == 2
    assert res["elected"] is False


# ---------------------------------------------------------------------------
# Safety invariant (Hypothesis): at most one leader per term
# ---------------------------------------------------------------------------
cluster = st.integers(min_value=1, max_value=7)
logval = st.integers(min_value=0, max_value=5)


@settings(max_examples=200, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(n=cluster, term=st.integers(min_value=1, max_value=5),
       a_llt=logval, a_lli=logval, b_llt=logval, b_lli=logval)
def test_at_most_one_leader_per_term(submission, n, term, a_llt, a_lli, b_llt, b_lli):
    nodes = [_node(f"n{i}", term=term, voted_for=None, llt=1, lli=1)
             for i in range(n)]
    # Candidate A runs; thread the resulting (voted) states into candidate B's run,
    # so single-vote-per-term is honored across both elections in the same term.
    ra = submission.elect_leader(nodes, _req("A", term=term, llt=a_llt, lli=a_lli))
    rb = submission.elect_leader(ra["nodes"], _req("B", term=term, llt=b_llt, lli=b_lli))
    assert not (ra["elected"] and rb["elected"]), (
        f"two leaders in term {term}: A={ra['votes']} B={rb['votes']} of {n}"
    )
