"""Mutant: believes orderBy and distinct are narrow (no shuffle)."""


def predicted_stages():
    # WRONG: both orderBy and distinct are WIDE (they shuffle). Here they're treated as
    # narrow, so q3 and q4 are under-counted.
    return {
        "q1_filter_select": 1,
        "q2_group_count": 2,
        "q3_group_then_sort": 2,  # should be 3 (orderBy shuffles)
        "q4_distinct": 1,          # should be 2 (distinct shuffles)
        "q5_join": 3,
        "q6_join_then_group": 4,
    }
