"""Mutant: up-to-date check compares only last_log_index, ignoring last_log_term.
A candidate with a longer but STALER log (lower last term) is wrongly judged current
-- this can elect a leader missing committed entries."""

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
    cand = request["candidate_id"]
    if n["voted_for"] is not None and n["voted_for"] != cand:
        return False, n
    # BUG: ignores last_log_term; compares index alone.
    if request["last_log_index"] < n["last_log_index"]:
        return False, n
    n["voted_for"] = cand
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
