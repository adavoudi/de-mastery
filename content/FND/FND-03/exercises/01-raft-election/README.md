# Toy Raft leader election (vote rules + safety)

You will implement the **safety-critical core** of Raft leader election: the vote-granting rule and
the majority election decision. No networking, no timers — pure functions over explicit state, so
the invariant "**at most one leader per term**" is testable directly.

## State shapes

A node's persistent election state is a dict:
```python
{"id": "n1", "current_term": 3, "voted_for": None,   # or a candidate id
 "last_log_term": 2, "last_log_index": 7}
```
A vote request from a candidate:
```python
{"candidate_id": "n2", "term": 4, "last_log_term": 2, "last_log_index": 7}
```

## Functions to implement in `solution.py`

### 1. `grant_vote(node, request) -> tuple[bool, dict]`
Decide whether `node` grants its vote, following Raft's rules **in order**:
1. If `request["term"] < node["current_term"]`: **deny** (stale term).
2. Adopt the newer term: if `request["term"] > node["current_term"]`, the node moves to that term
   and its `voted_for` resets to `None` for the new term.
3. If the node has already voted for a *different* candidate **in this (request) term**: **deny**.
4. If the candidate's log is **not** at least as up to date as the node's — i.e. the pair
   `(request["last_log_term"], request["last_log_index"])` is strictly less than the node's
   `(last_log_term, last_log_index)` — **deny**.
5. Otherwise **grant**: record `voted_for = candidate_id` and `current_term = request["term"]`.

Return `(granted: bool, new_node_state: dict)`. Do **not** mutate the input node in place; return a
new/updated copy. Granting again to the *same* candidate in the same term is idempotent (still a
grant).

### 2. `elect_leader(nodes, request) -> dict`
Given a list of node states and one candidate's vote request, ask **every** node (the candidate
counts as a self-vote) and decide the election. Return:
```python
{"elected": bool, "votes": int, "needed": int, "term": int,
 "nodes": [<updated node state>, ...]}   # same order as input
```
where `needed = len(nodes) // 2 + 1` (strict majority) and `elected = votes >= needed`. The
`nodes` list is each input node *after* applying its vote decision (so a second election in the
same term can be threaded through these updated states — a node that already voted won't vote
again). The candidate's self-vote updates the candidate's own node state too, if it is in `nodes`.

## What the tests check

- The five vote rules, each in isolation (stale term, already-voted, log-not-current, term
  adoption, idempotent re-grant).
- `elect_leader` requires a **strict majority** (2-of-4 does NOT elect; a tie never wins).
- **Safety invariant (Hypothesis):** for random clusters, terms, logs and *two* competing
  candidates asked against the same single-vote-per-term node set, **at most one** can win the
  same term.
- Log comparison uses the `(term, index)` pair, so a higher last-log-term beats a higher index.

Constraints: pure Python, deterministic, no mutation of inputs. `lab: none`.
