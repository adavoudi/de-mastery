"""Alt solution 1 for FND-03: purely functional with dict spread and an explicit
up_to_date helper. No deepcopy; builds a new dict each step."""

from __future__ import annotations


def _at_least_as_current(req, node) -> bool:
    return (req["last_log_term"], req["last_log_index"]) >= (
        node["last_log_term"], node["last_log_index"])


def grant_vote(node, request):
    req_term = request["term"]
    if req_term < node["current_term"]:
        return False, {**node}

    # Normalize to the request term, resetting voted_for on a term bump.
    if req_term > node["current_term"]:
        node = {**node, "current_term": req_term, "voted_for": None}

    cand = request["candidate_id"]
    voted = node.get("voted_for")
    if voted is not None and voted != cand:
        return False, {**node}

    if not _at_least_as_current(request, node):
        return False, {**node}

    return True, {**node, "voted_for": cand, "current_term": req_term}


def elect_leader(nodes, request):
    needed = len(nodes) // 2 + 1
    results = [grant_vote(n, request) for n in nodes]
    votes = sum(1 for granted, _ in results if granted)
    return {
        "elected": votes >= needed,
        "votes": votes,
        "needed": needed,
        "term": request["term"],
        "nodes": [state for _, state in results],
    }
