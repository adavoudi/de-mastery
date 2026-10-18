"""Mini aggregator — cumulative learner codebase (starter).

Implement each function as you reach its stage. The starter versions are
deliberately unimplemented so stage tests fail (gate G1).
"""


def scan(rows):
    """Stage 1: return the input rows as a list of dicts (a trivial table scan)."""
    raise NotImplementedError


def group_sum(rows, key, val):
    """Stage 2: group ``rows`` by ``key`` and sum ``val``.

    Return a list of ``{key: k, val: total}`` dicts (one per group).
    """
    raise NotImplementedError
