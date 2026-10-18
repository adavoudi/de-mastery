"""Alt solution 2 for FND-03: an internal _Voter class encapsulates the per-term
decision; the module-level functions wrap it. Genuinely different structure."""

from __future__ import annotations


class _Voter:
    def __init__(self, state: dict):
        self.term = state["current_term"]
        self.voted_for = state["voted_for"]
        self.llt = state["last_log_term"]
        self.lli = state["last_log_index"]
        self.id = state["id"]

    def consider(self, request):
        rt = request["term"]
        if rt < self.term:
            return False
        if rt > self.term:
            self.term = rt
            self.voted_for = None
        cand = request["candidate_id"]
        if self.voted_for not in (None, cand):
            return False
        if (request["last_log_term"], request["last_log_index"]) < (self.llt, self.lli):
            return False
        self.voted_for = cand
        self.term = rt
        return True

    def state(self) -> dict:
        return {"id": self.id, "current_term": self.term,
                "voted_for": self.voted_for,
                "last_log_term": self.llt, "last_log_index": self.lli}


def grant_vote(node, request):
    v = _Voter(node)
    granted = v.consider(request)
    return granted, v.state()


def elect_leader(nodes, request):
    needed = len(nodes) // 2 + 1
    votes = 0
    updated = []
    for n in nodes:
        v = _Voter(n)
        if v.consider(request):
            votes += 1
        updated.append(v.state())
    return {"elected": votes >= needed, "votes": votes, "needed": needed,
            "term": request["term"], "nodes": updated}
