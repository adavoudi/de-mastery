"""SPK-02 / 01-predict-stages — alt: derive stages from a shuffle-count table.

Genuinely different construction: list the wide transformations per query, then apply
stages = shuffles + 1 programmatically instead of writing the final numbers.
"""

# number of shuffles (wide transformations) on the path to the action
_SHUFFLES = {
    "q1_filter_select": 0,   # narrow only
    "q2_group_count": 1,     # groupBy
    "q3_group_then_sort": 2, # groupBy, orderBy
    "q4_distinct": 1,        # distinct
    "q5_join": 2,            # join shuffles both sides (broadcast off)
    "q6_join_then_group": 3, # join (2) + groupBy (1)
}


def predicted_stages():
    return {q: n + 1 for q, n in _SHUFFLES.items()}
