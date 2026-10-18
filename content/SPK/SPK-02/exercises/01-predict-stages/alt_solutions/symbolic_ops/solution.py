"""SPK-02 / 01-predict-stages — alt: list the wide operators symbolically and count them.

Different again: each query is described by the sequence of wide operators it contains;
'join' expands to two shuffles because broadcast is disabled.
"""

_WIDE_OPS = {
    "q1_filter_select": [],
    "q2_group_count": ["groupBy"],
    "q3_group_then_sort": ["groupBy", "orderBy"],
    "q4_distinct": ["distinct"],
    "q5_join": ["join"],
    "q6_join_then_group": ["join", "groupBy"],
}

# How many shuffle boundaries each wide operator introduces here.
_COST = {"groupBy": 1, "orderBy": 1, "distinct": 1, "join": 2}


def predicted_stages():
    out = {}
    for q, ops in _WIDE_OPS.items():
        shuffles = sum(_COST[op] for op in ops)
        out[q] = shuffles + 1
    return out
