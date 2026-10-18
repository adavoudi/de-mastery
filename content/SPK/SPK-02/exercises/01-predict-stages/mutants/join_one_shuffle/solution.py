"""Mutant: treats a join as a single shuffle (assumes a broadcast / one side)."""


def predicted_stages():
    # WRONG: a broadcast-disabled join shuffles BOTH sides (2 shuffles), not 1.
    return {
        "q1_filter_select": 1,
        "q2_group_count": 2,
        "q3_group_then_sort": 3,
        "q4_distinct": 2,
        "q5_join": 2,            # should be 3
        "q6_join_then_group": 3,  # should be 4
    }
