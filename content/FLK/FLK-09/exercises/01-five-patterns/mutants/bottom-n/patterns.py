"""MUTANT: top_n sorts ASCending, returning the BOTTOM-n (smallest) instead of
the top-n. Classic ORDER BY direction mistake."""


def dedup_keep_first() -> str:
    return (
        "SELECT order_id, amount FROM ("
        "  SELECT order_id, amount,"
        "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount) AS rn"
        "  FROM ord) WHERE rn = 1"
    )


def top_n(n: int) -> str:
    # BUG: ASC -> bottom-n, not top-n.
    return (
        f"SELECT company, amount FROM ("
        f"  SELECT company, amount,"
        f"    ROW_NUMBER() OVER (PARTITION BY company ORDER BY amount ASC) AS rn"
        f"  FROM ord) WHERE rn <= {n}"
    )


def split_merge() -> str:
    return (
        "SELECT order_id, 'large' AS bucket FROM ord WHERE amount >= 50 "
        "UNION ALL "
        "SELECT order_id, 'small' AS bucket FROM ord WHERE amount < 50"
    )


def match_recognize() -> str:
    return (
        "SELECT * FROM ev MATCH_RECOGNIZE ("
        "  PARTITION BY g ORDER BY ts"
        "  MEASURES FIRST(A.v) AS v1, LAST(B.v) AS v2"
        "  ONE ROW PER MATCH"
        "  PATTERN (A B)"
        "  DEFINE B AS B.v > A.v)"
    )


def over_running_sum() -> str:
    return (
        "SELECT g, v,"
        "  SUM(v) OVER (PARTITION BY g ORDER BY ts"
        "    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_sum"
        " FROM ev"
    )
