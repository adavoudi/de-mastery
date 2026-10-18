"""SPK-02 / 01-predict-stages — reference solution.

Stages = shuffles + 1 for a linear job. Broadcast is disabled, so joins shuffle both
sides.
"""


def predicted_stages():
    return {
        "q1_filter_select": 1,   # 0 shuffles (narrow only)
        "q2_group_count": 2,     # 1 shuffle (groupBy)
        "q3_group_then_sort": 3, # 2 shuffles (groupBy + orderBy)
        "q4_distinct": 2,        # 1 shuffle (distinct)
        "q5_join": 3,            # 2 shuffles (join, both sides)
        "q6_join_then_group": 4, # 3 shuffles (join x2 + groupBy)
    }
