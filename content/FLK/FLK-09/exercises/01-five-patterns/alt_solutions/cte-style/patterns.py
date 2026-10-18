"""Alt A — same answers, written with CTEs, BETWEEN, a single CASE, different
MATCH_RECOGNIZE variable names, and the default OVER frame."""


def dedup_keep_first() -> str:
    return (
        "WITH r AS (SELECT order_id, amount, "
        "ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount ASC) AS k "
        "FROM ord) SELECT order_id, amount FROM r WHERE k = 1"
    )


def top_n(n: int) -> str:
    return (
        f"WITH r AS (SELECT company, amount, "
        f"ROW_NUMBER() OVER (PARTITION BY company ORDER BY amount DESC) AS k "
        f"FROM ord) SELECT company, amount FROM r WHERE k BETWEEN 1 AND {n}"
    )


def split_merge() -> str:
    # a single CASE emits exactly one labelled row per input — same multiset as
    # the two filtered halves unioned together.
    return (
        "SELECT order_id, CASE WHEN amount >= 50 THEN 'large' ELSE 'small' END "
        "AS bucket FROM ord"
    )


def match_recognize() -> str:
    return (
        "SELECT * FROM ev MATCH_RECOGNIZE ("
        "  PARTITION BY g ORDER BY ts"
        "  MEASURES FIRST(LOW.v) AS v1, LAST(HIGH.v) AS v2"
        "  ONE ROW PER MATCH"
        "  PATTERN (LOW HIGH)"
        "  DEFINE HIGH AS HIGH.v > LOW.v)"
    )


def over_running_sum() -> str:
    # default frame (RANGE UNBOUNDED PRECEDING .. CURRENT ROW) == the running sum
    return (
        "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY ts) AS run_sum FROM ev"
    )
