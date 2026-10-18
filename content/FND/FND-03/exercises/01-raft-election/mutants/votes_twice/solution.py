"""Mutant: never checks voted_for, so a node can vote for two candidates in one term.
This breaks the single-leader-per-term safety invariant."""

from __future__ import annotations

import copy


def grant_vote(node, request):
    n = copy.deepcopy(node)
    rt = request["term"]
    if rt < n["current_term"]:
        return False, n
    if rt > n["current_term"]:
        n["current_term"] = rt
        n["voted_for"] = None
    # BUG: no "already voted for someone else" check.
    if (request["last_log_term"], request["last_log_index"]) < (
            n["last_log_term"], n["last_log_index"]):
        return False, n
    n["voted_for"] = request["candidate_id"]
    n["current_term"] = rt
    return True, n


def elect_leader(nodes, request):
    needed = len(nodes) // 2 + 1
    votes = 0
    updated = []
    for node in nodes:
        g, s = grant_vote(node, request)
        votes += 1 if g else 0
        updated.append(s)
    return {"elected": votes >= needed, "votes": votes, "needed": needed,
            "term": request["term"], "nodes": updated}
