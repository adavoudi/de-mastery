"""Mutant: reports shuffle counts, forgetting the final stage (+1)."""


def predicted_stages():
    # WRONG: stages = shuffles, missing the +1 for the final (result) stage.
    return {
        "q1_filter_select": 0,
        "q2_group_count": 1,
        "q3_group_then_sort": 2,
        "q4_distinct": 1,
        "q5_join": 2,
        "q6_join_then_group": 3,
    }
