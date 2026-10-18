"""Mutant: uses votes >= len(nodes)//2 (a plurality / allows ties) instead of a
strict majority. On 4 nodes, 2 votes would wrongly elect — split-brain risk."""

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
    if (request["last_log_term"], request["last_log_index"]) < (
            n["last_log_term"], n["last_log_index"]):
        return False, n
    n["voted_for"] = cand
    n["current_term"] = rt
    return True, n


def elect_leader(nodes, request):
    # BUG: needed is a plurality, not a strict majority.
    needed = len(nodes) // 2
    votes = 0
    updated = []
    for node in nodes:
        g, s = grant_vote(node, request)
        votes += 1 if g else 0
        updated.append(s)
    return {"elected": votes >= needed, "votes": votes, "needed": needed,
            "term": request["term"], "nodes": updated}
