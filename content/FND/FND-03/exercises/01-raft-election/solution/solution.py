"""Reference solution for FND-03/01-raft-election.

Implements the safety-critical core of Raft leader election as pure functions.
"""

from __future__ import annotations

import copy


def grant_vote(node, request):
    n = copy.deepcopy(node)
    req_term = request["term"]

    # Rule 1: stale term -> deny (do not change term).
    if req_term < n["current_term"]:
        return False, n

    # Rule 2: adopt a newer term, resetting the per-term vote.
    if req_term > n["current_term"]:
        n["current_term"] = req_term
        n["voted_for"] = None

    cand = request["candidate_id"]

    # Rule 3: already voted for someone else this term -> deny.
    if n["voted_for"] is not None and n["voted_for"] != cand:
        return False, n

    # Rule 4: candidate log must be at least as up to date as ours.
    cand_log = (request["last_log_term"], request["last_log_index"])
    my_log = (n["last_log_term"], n["last_log_index"])
    if cand_log < my_log:
        return False, n

    # Rule 5: grant (idempotent for the same candidate in the same term).
    n["voted_for"] = cand
    n["current_term"] = req_term
    return True, n


def elect_leader(nodes, request):
    term = request["term"]
    needed = len(nodes) // 2 + 1
    votes = 0
    updated = []
    for node in nodes:
        granted, new_state = grant_vote(node, request)
        if granted:
            votes += 1
        updated.append(new_state)
    return {
        "elected": votes >= needed,
        "votes": int(votes),
        "needed": int(needed),
        "term": int(term),
        "nodes": updated,
    }
