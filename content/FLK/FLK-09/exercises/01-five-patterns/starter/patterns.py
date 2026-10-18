"""Starter — fill in each pattern's SQL. These stubs are wrong on purpose so the
tests fail until you implement them (G1). See README for the source tables and
the required output columns.
"""


def dedup_keep_first() -> str:
    # TODO: keep one row per order_id (the first), append-only.
    return "SELECT order_id, amount FROM ord"


def top_n(n: int) -> str:
    # TODO: top-n by amount per company (an updating query).
    return "SELECT company, amount FROM ord"


def split_merge() -> str:
    # TODO: label each row large/small and UNION ALL the two halves.
    return "SELECT order_id, 'small' AS bucket FROM ord"


def match_recognize() -> str:
    # TODO: consecutive increasing pairs per g via MATCH_RECOGNIZE.
    return "SELECT g, v AS v1, v AS v2 FROM ev"


def over_running_sum() -> str:
    # TODO: running SUM per g ordered by ts.
    return "SELECT g, v, v AS run_sum FROM ev"
