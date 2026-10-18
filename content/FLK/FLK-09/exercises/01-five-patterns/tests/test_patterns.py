"""FLK-09 — five streaming SQL patterns, graded on changelog + final state.

Each pattern's SQL comes from the learner's `patterns.py` (via the `submission`
fixture). We run it on the embedded Flink `flink_sql` runner and compare against
an independent DuckDB answer (G6). We assert the *changelog shape* (append-only
vs updating) and the *final materialized state* / emitted multiset — never the
SQL text, so alternate phrasings pass (G3).
"""

import os
from pathlib import Path

import duckdb
import pytest

ORD_DDL = (
    "CREATE VIEW ord AS SELECT * FROM (VALUES "
    "('A','o1',10),('A','o1',10),('A','o2',50),('A','o3',30),"
    "('B','o4',20),('B','o4',20),('B','o5',90)) AS t(company,order_id,amount)"
)

# event-time rows (g, v, ts) — strictly increasing distinct timestamps so the
# bounded source flushes every row deterministically (no sleep, no wall-clock).
EV_ROWS = [
    ("x", 10, "2026-01-01 00:00:01"),
    ("y", 5, "2026-01-01 00:00:02"),
    ("x", 20, "2026-01-01 00:00:03"),
    ("x", 15, "2026-01-01 00:00:04"),
    ("y", 8, "2026-01-01 00:00:05"),
]


def _ev_ddl(tmp_path: Path) -> str:
    csv = tmp_path / "ev.csv"
    csv.write_text("".join(f"{g},{v},{ts}\n" for g, v, ts in EV_ROWS))
    return (
        f"CREATE TABLE ev (g STRING, v INT, ts TIMESTAMP(3), "
        f"WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) WITH "
        f"('connector'='filesystem','path'='{csv}','format'='csv')"
    )


def _final_state(rows):
    """Apply the changelog in order; return the surviving rows as a sorted
    multiset of tuples (counts preserved for append-only duplicates)."""
    counts = {}
    for r in rows:
        kind, fields = r[0], tuple(r[1:])
        if kind in ("+I", "+U"):
            counts[fields] = counts.get(fields, 0) + 1
        else:  # -U / -D
            counts[fields] = counts.get(fields, 0) - 1
    out = []
    for fields, c in counts.items():
        out.extend([fields] * max(c, 0))
    return sorted(out)


def _kinds(rows):
    return {r[0] for r in rows}


# --------------------------------------------------------------------------
# 1. dedup keep-first — append-only, one row per order_id
# --------------------------------------------------------------------------
def test_dedup_keep_first(flink_sql, submission):
    sql = submission.dedup_keep_first()
    rows = flink_sql.run(ddl=[ORD_DDL], query=sql, mode="streaming", limit=200)
    assert _kinds(rows) == {"+I"}, f"dedup keep-first must be append-only; got {_kinds(rows)}"
    # oracle: first row per order_id (tie-break amount asc). Each order_id's rows
    # are identical in amount here, so the kept amount is the per-id amount.
    con = duckdb.connect()
    con.execute("CREATE TABLE ord(company VARCHAR, order_id VARCHAR, amount INT)")
    con.executemany("INSERT INTO ord VALUES (?,?,?)",
                    [("A", "o1", 10), ("A", "o1", 10), ("A", "o2", 50),
                     ("A", "o3", 30), ("B", "o4", 20), ("B", "o4", 20),
                     ("B", "o5", 90)])
    exp = con.execute(
        "SELECT order_id, amount FROM (SELECT order_id, amount, "
        "ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount) rn FROM ord) "
        "WHERE rn=1").fetchall()
    want = sorted((oid, str(amt)) for oid, amt in exp)
    assert _final_state(rows) == want


# --------------------------------------------------------------------------
# 2. top-n — updating (must retract), top 2 by amount per company
# --------------------------------------------------------------------------
def test_top_n_is_updating(flink_sql, submission):
    rows = flink_sql.run(ddl=[ORD_DDL], query=submission.top_n(2),
                         mode="streaming", limit=200)
    assert any(r[0] in ("-D", "-U") for r in rows), \
        "Top-N must emit retractions (it is an updating query)"


def test_top_n_final_state(flink_sql, submission):
    rows = flink_sql.run(ddl=[ORD_DDL], query=submission.top_n(2),
                         mode="streaming", limit=200)
    con = duckdb.connect()
    con.execute("CREATE TABLE ord(company VARCHAR, order_id VARCHAR, amount INT)")
    con.executemany("INSERT INTO ord VALUES (?,?,?)",
                    [("A", "o1", 10), ("A", "o1", 10), ("A", "o2", 50),
                     ("A", "o3", 30), ("B", "o4", 20), ("B", "o4", 20),
                     ("B", "o5", 90)])
    exp = con.execute(
        "SELECT company, amount FROM (SELECT company, amount, "
        "ROW_NUMBER() OVER (PARTITION BY company ORDER BY amount DESC) rn "
        "FROM (SELECT DISTINCT company, order_id, amount FROM ord)) "
        "WHERE rn<=2").fetchall()
    want = sorted((c, str(a)) for c, a in exp)
    assert _final_state(rows) == want


# --------------------------------------------------------------------------
# 3. split/merge — append-only, keeps duplicates, correct bucket per row
# --------------------------------------------------------------------------
def test_split_merge(flink_sql, submission):
    rows = flink_sql.run(ddl=[ORD_DDL], query=submission.split_merge(),
                         mode="streaming", limit=200)
    assert _kinds(rows) == {"+I"}, "split/merge via UNION ALL must be append-only"
    # oracle: every input row labelled large/small, duplicates kept (multiset)
    con = duckdb.connect()
    con.execute("CREATE TABLE ord(company VARCHAR, order_id VARCHAR, amount INT)")
    con.executemany("INSERT INTO ord VALUES (?,?,?)",
                    [("A", "o1", 10), ("A", "o1", 10), ("A", "o2", 50),
                     ("A", "o3", 30), ("B", "o4", 20), ("B", "o4", 20),
                     ("B", "o5", 90)])
    exp = con.execute(
        "SELECT order_id, CASE WHEN amount>=50 THEN 'large' ELSE 'small' END "
        "FROM ord").fetchall()
    want = sorted((oid, b) for oid, b in exp)
    assert _final_state(rows) == want


# --------------------------------------------------------------------------
# 4. MATCH_RECOGNIZE — consecutive increasing pairs per g
# --------------------------------------------------------------------------
def test_match_recognize(flink_sql, submission, tmp_path):
    rows = flink_sql.run(ddl=[_ev_ddl(tmp_path)],
                         query=submission.match_recognize(),
                         mode="streaming", limit=200)
    # oracle: per g (ordered by ts), consecutive pairs where v increases
    con = duckdb.connect()
    con.execute("CREATE TABLE ev(g VARCHAR, v INT, ts TIMESTAMP)")
    con.executemany("INSERT INTO ev VALUES (?,?,?)", EV_ROWS)
    exp = con.execute(
        "WITH p AS (SELECT g, v, LEAD(v) OVER (PARTITION BY g ORDER BY ts) nxt "
        "FROM ev) SELECT g, v, nxt FROM p WHERE nxt IS NOT NULL AND nxt > v"
    ).fetchall()
    want = sorted((g, str(v), str(n)) for g, v, n in exp)
    assert _final_state(rows) == want


# --------------------------------------------------------------------------
# 5. OVER running sum — append-only, one row per input
# --------------------------------------------------------------------------
def test_over_running_sum(flink_sql, submission, tmp_path):
    rows = flink_sql.run(ddl=[_ev_ddl(tmp_path)],
                         query=submission.over_running_sum(),
                         mode="streaming", limit=200)
    assert _kinds(rows) == {"+I"}, "OVER aggregation must be append-only"
    assert len(rows) == len(EV_ROWS), "OVER emits exactly one row per input row"
    con = duckdb.connect()
    con.execute("CREATE TABLE ev(g VARCHAR, v INT, ts TIMESTAMP)")
    con.executemany("INSERT INTO ev VALUES (?,?,?)", EV_ROWS)
    exp = con.execute(
        "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY ts "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM ev").fetchall()
    want = sorted((g, str(v), str(s)) for g, v, s in exp)
    assert _final_state(rows) == want
