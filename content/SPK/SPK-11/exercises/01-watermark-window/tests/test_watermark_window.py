"""Tests for SPK-11 / 01-watermark-window.

Drives the learner's windowed_counts across runs sharing one checkpoint so the
watermark advances between runs (the memory sink can't recover from a checkpoint, so
we write to Iceberg). An early window's events arrive, then a far-future event
advances the watermark and finalizes that window, then LATE events for the finalized
window arrive and must be dropped.

G6 oracle: DuckDB counts the NON-LATE events per 5-minute window, keeping only windows
that have finalized (window end + 10-min watermark <= max event time). The streaming
table must equal that. Deterministic: availableNow + processAllAvailable, no sleep.
Requires the Iceberg JARs (spark_iceberg); SKIPS cleanly without them.
"""

import json
import uuid

from de_testkit import assert_frames_equal

# Non-late events (arrive before their window finalizes).
ON_TIME = [
    {"event_id": 1, "company_id": "C1", "amount": 1.0, "event_ts": "2026-01-01 00:00:30"},
    {"event_id": 2, "company_id": "C1", "amount": 2.0, "event_ts": "2026-01-01 00:01:30"},
    {"event_id": 3, "company_id": "C2", "amount": 3.0, "event_ts": "2026-01-01 00:03:00"},
    # In the NEXT 5-min window [00:05,00:10): distinguishes 5-min from 10-min windows.
    {"event_id": 7, "company_id": "C1", "amount": 7.0, "event_ts": "2026-01-01 00:06:30"},
]
# Advances the watermark past window [00:00,00:05): with a 10-min delay, need ts >= 00:15.
ADVANCE = [
    {"event_id": 4, "company_id": "C1", "amount": 4.0, "event_ts": "2026-01-01 00:20:00"},
]
# Late events for the (now finalized) first window -> must be dropped.
LATE = [
    {"event_id": 5, "company_id": "C1", "amount": 5.0, "event_ts": "2026-01-01 00:02:00"},
    {"event_id": 6, "company_id": "C2", "amount": 6.0, "event_ts": "2026-01-01 00:04:00"},
]


def _write(src, rows, idx):
    (src / f"b{idx:04d}.json").write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def _oracle(con, rows):
    # Count non-late events per 5-min window; keep only FINALIZED windows
    # (window end + 10 min <= max event time across all non-late events).
    con.execute("CREATE OR REPLACE TABLE ev AS SELECT * FROM (VALUES " +
                ",".join(f"('{r['company_id']}', TIMESTAMP '{r['event_ts']}')" for r in rows) +
                ") AS t(company_id, event_ts)")
    return con.execute(
        """
        WITH w AS (
          SELECT company_id,
                 time_bucket(INTERVAL '5 minutes', event_ts) AS w_start,
                 count(*) AS n
          FROM ev GROUP BY 1, 2
        ),
        mx AS (SELECT max(event_ts) AS m FROM ev)
        SELECT company_id, strftime(w_start, '%Y-%m-%d %H:%M:%S') AS w_start, n
        FROM w, mx
        WHERE w_start + INTERVAL '5 minutes' + INTERVAL '10 minutes' <= mx.m
        """
    ).arrow()


def _run(submission, spark, src, table, ckpt):
    submission.windowed_counts(spark, str(src), table, ckpt)


def _read(spark, table):
    spark.sql(f"REFRESH TABLE {table}")
    # Format the window start inside Spark (UTC session) to a stable string, so the
    # comparison does not depend on the driver's local timezone during toPandas().
    return spark.sql(
        f"SELECT company_id, date_format(w_start, 'yyyy-MM-dd HH:mm:ss') AS w_start, n FROM {table}"
    )


def test_late_events_dropped(submission, spark_iceberg, duckdb_con, tmp_path):
    s = spark_iceberg
    ns = f"wm_{uuid.uuid4().hex[:8]}"
    table = f"local.{ns}.win"
    s.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    s.sql(f"CREATE TABLE {table} (company_id STRING, w_start TIMESTAMP, n BIGINT) USING iceberg")
    src = tmp_path / "src"
    src.mkdir()
    ckpt = str(tmp_path / "ck")

    _write(src, ON_TIME, 1)
    _run(submission, s, src, table, ckpt)
    _write(src, ADVANCE, 2)
    _run(submission, s, src, table, ckpt)
    _write(src, LATE, 3)
    _run(submission, s, src, table, ckpt)

    out = _read(s, table)
    oracle = _oracle(duckdb_con, ON_TIME + ADVANCE)  # late events excluded by construction
    assert_frames_equal(out, oracle)


def test_finalized_window_counts(submission, spark_iceberg, tmp_path):
    # The two finalized 5-minute windows carry exactly the on-time counts:
    # [00:00,00:05) -> C1:2, C2:1 ; [00:05,00:10) -> C1:1. The open [00:20,00:25)
    # window is NOT emitted, and no late event is counted.
    s = spark_iceberg
    ns = f"wm_{uuid.uuid4().hex[:8]}"
    table = f"local.{ns}.win"
    s.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    s.sql(f"CREATE TABLE {table} (company_id STRING, w_start TIMESTAMP, n BIGINT) USING iceberg")
    src = tmp_path / "src"
    src.mkdir()
    ckpt = str(tmp_path / "ck")
    _write(src, ON_TIME, 1); _run(submission, s, src, table, ckpt)
    _write(src, ADVANCE, 2); _run(submission, s, src, table, ckpt)
    _write(src, LATE, 3); _run(submission, s, src, table, ckpt)

    # Three finalized window-rows total; the open [00:20,00:25) window is not emitted
    # and no late event is counted. Compare the (company, count) multiset — this is
    # timezone-display independent (w_start equality is covered by test_late_events_dropped).
    collected = _read(s, table).collect()
    assert len(collected) == 3, collected
    got = sorted((r["company_id"], r["n"]) for r in collected)
    assert got == [("C1", 1), ("C1", 2), ("C2", 1)], got
    # Every emitted window must be a non-late, finalized one: total counted events = 4.
    assert sum(r["n"] for r in collected) == 4
