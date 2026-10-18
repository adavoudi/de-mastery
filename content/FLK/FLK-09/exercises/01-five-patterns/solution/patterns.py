"""Reference: five streaming SQL patterns as SQL-returning functions.

The test runs each returned query on an embedded Flink TableEnvironment over a
fixed source (see README) and grades the changelog + final state vs DuckDB.
"""


def dedup_keep_first() -> str:
    # keep-first dedup per order_id (append-only). Tie-break amount ascending.
    return (
        "SELECT order_id, amount FROM ("
        "  SELECT order_id, amount,"
        "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount) AS rn"
        "  FROM ord) WHERE rn = 1"
    )


def top_n(n: int) -> str:
    # top-n orders by amount per company (updating: retracts displaced rows).
    return (
        f"SELECT company, amount FROM ("
        f"  SELECT company, amount,"
        f"    ROW_NUMBER() OVER (PARTITION BY company ORDER BY amount DESC) AS rn"
        f"  FROM ord) WHERE rn <= {n}"
    )


def split_merge() -> str:
    # split by predicate, merge with UNION ALL (append-only, keeps duplicates).
    return (
        "SELECT order_id, 'large' AS bucket FROM ord WHERE amount >= 50 "
        "UNION ALL "
        "SELECT order_id, 'small' AS bucket FROM ord WHERE amount < 50"
    )


def match_recognize() -> str:
    # consecutive strictly-increasing pairs per g, in ts order.
    return (
        "SELECT * FROM ev MATCH_RECOGNIZE ("
        "  PARTITION BY g ORDER BY ts"
        "  MEASURES FIRST(A.v) AS v1, LAST(B.v) AS v2"
        "  ONE ROW PER MATCH"
        "  PATTERN (A B)"
        "  DEFINE B AS B.v > A.v)"
    )


def over_running_sum() -> str:
    # running SUM per g ordered by ts (append-only, one row out per row in).
    return (
        "SELECT g, v,"
        "  SUM(v) OVER (PARTITION BY g ORDER BY ts"
        "    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_sum"
        " FROM ev"
    )
