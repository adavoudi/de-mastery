# Toy Raft leader election (vote rules + safety)

You will implement the **safety-critical core** of Raft leader election. It has two parts: the
vote-granting rule and the majority election decision. There is no networking and there are no
timers. The functions are pure and work on explicit state. So you can test the invariant "**at
most one leader per term**" directly.

## State shapes

The persistent election state of a node is a dict:
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
Decide whether `node` grants its vote. Follow the rules of Raft **in this order**:
1. If `request["term"] < node["current_term"]`: **deny** (stale term).
2. Adopt the newer term. If `request["term"] > node["current_term"]`, the node moves to that
   term. Its `voted_for` resets to `None` for the new term.
3. If the node already voted for a *different* candidate **in this (request) term**: **deny**.
4. If the log of the candidate is **not** at least as up to date as the log of the node, **deny**.
   This is the case when the pair `(request["last_log_term"], request["last_log_index"])` is
   strictly less than the pair `(last_log_term, last_log_index)` of the node.
5. Otherwise **grant**: record `voted_for = candidate_id` and `current_term = request["term"]`.

Return `(granted: bool, new_node_state: dict)`. Do **not** mutate the input node in place. Return
a new or updated copy. A second grant to the *same* candidate in the same term is idempotent
(it is still a grant).

### 2. `elect_leader(nodes, request) -> dict`
You get a list of node states and the vote request of one candidate. Ask **every** node. The
candidate counts as a self-vote. Then decide the election. Return:
```python
{"elected": bool, "votes": int, "needed": int, "term": int,
 "nodes": [<updated node state>, ...]}   # same order as input
```
Here `needed = len(nodes) // 2 + 1` (strict majority) and `elected = votes >= needed`.

The `nodes` list holds each input node *after* it applies its vote decision. So you can pass
these updated states to a second election in the same term. A node that already voted will not
vote again. The self-vote of the candidate also updates the node state of the candidate, if the
candidate is in `nodes`.

## What the tests check

- The five vote rules, each alone (stale term, already-voted, log-not-current, term adoption,
  idempotent re-grant).
- `elect_leader` requires a **strict majority**. A 2-of-4 vote does NOT elect, and a tie never
  wins.
- **Safety invariant (Hypothesis):** the tests use random clusters, terms and logs. They ask
  *two* competing candidates against the same node set, where each node has one vote per term.
  **At most one** candidate can win the same term.
- The log comparison uses the `(term, index)` pair. So a higher last-log-term beats a higher
  index.

Constraints: pure Python, deterministic, no mutation of inputs. `lab: none`.
