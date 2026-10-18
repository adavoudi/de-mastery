"""Alt B — same answers via QUALIFY, a '< n+1' bound, UNION ALL with a NOT
predicate, an explicit AFTER MATCH SKIP, and a named WINDOW clause."""


def dedup_keep_first() -> str:
    return (
        "SELECT order_id, amount FROM ord "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount) = 1"
    )


def top_n(n: int) -> str:
    return (
        f"SELECT company, amount FROM ("
        f"  SELECT company, amount,"
        f"    ROW_NUMBER() OVER (PARTITION BY company ORDER BY amount DESC) AS k"
        f"  FROM ord) WHERE k < {n + 1}"
    )


def split_merge() -> str:
    return (
        "SELECT order_id, CAST('large' AS STRING) AS bucket FROM ord "
        "WHERE amount >= 50 "
        "UNION ALL "
        "SELECT order_id, CAST('small' AS STRING) AS bucket FROM ord "
        "WHERE NOT amount >= 50"
    )


def match_recognize() -> str:
    return (
        "SELECT * FROM ev MATCH_RECOGNIZE ("
        "  PARTITION BY g ORDER BY ts"
        "  MEASURES A.v AS v1, B.v AS v2"
        "  ONE ROW PER MATCH"
        "  AFTER MATCH SKIP TO NEXT ROW"
        "  PATTERN (A B)"
        "  DEFINE B AS B.v > A.v)"
    )


def over_running_sum() -> str:
    return (
        "SELECT g, v, SUM(v) OVER w AS run_sum FROM ev "
        "WINDOW w AS (PARTITION BY g ORDER BY ts "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )
