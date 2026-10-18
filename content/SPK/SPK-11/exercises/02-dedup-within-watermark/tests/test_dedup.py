"""Tests for SPK-11 / 02-dedup-within-watermark.

Duplicate event_ids arrive within the 30-minute watermark horizon, both inside one
run and across runs sharing the checkpoint. The final Iceberg table must contain
exactly one row per distinct event_id (DuckDB oracle keeps one row per id). All event
times are within 30 minutes, so a correct dedup removes every duplicate.
Requires the Iceberg JARs (spark_iceberg); SKIPS cleanly without them.
"""

import json
import uuid

from de_testkit import assert_frames_equal

RUN1 = [
    {"event_id": 1, "company_id": "C1", "amount": 10.0, "event_ts": "2026-01-01 00:00:00"},
    {"event_id": 1, "company_id": "C1", "amount": 10.0, "event_ts": "2026-01-01 00:00:00"},  # dup in-run
    {"event_id": 2, "company_id": "C1", "amount": 20.0, "event_ts": "2026-01-01 00:01:00"},
    {"event_id": 3, "company_id": "C2", "amount": 30.0, "event_ts": "2026-01-01 00:02:00"},
]
RUN2 = [
    {"event_id": 2, "company_id": "C1", "amount": 20.0, "event_ts": "2026-01-01 00:01:00"},  # dup across runs
    {"event_id": 4, "company_id": "C2", "amount": 40.0, "event_ts": "2026-01-01 00:03:00"},
    {"event_id": 3, "company_id": "C2", "amount": 30.0, "event_ts": "2026-01-01 00:02:00"},  # dup across runs
]
DISTINCT_IDS = {1, 2, 3, 4}


def _write(src, rows, idx):
    (src / f"b{idx:04d}.json").write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def _oracle(con, rows):
    con.execute("CREATE OR REPLACE TABLE ev AS SELECT * FROM (VALUES " +
                ",".join(
                    f"({r['event_id']}, '{r['company_id']}', {r['amount']}, "
                    f"TIMESTAMP '{r['event_ts']}')" for r in rows) +
                ") AS t(event_id, company_id, amount, event_ts)")
    # One row per event_id (the inputs for a given id are identical here).
    return con.execute(
        """
        SELECT event_id, any_value(company_id) AS company_id,
               any_value(amount) AS amount,
               strftime(any_value(event_ts), '%Y-%m-%d %H:%M:%S') AS event_ts
        FROM ev GROUP BY event_id
        """
    ).arrow()


def _read(spark, table):
    spark.sql(f"REFRESH TABLE {table}")
    return spark.sql(
        f"SELECT event_id, company_id, amount, "
        f"date_format(event_ts, 'yyyy-MM-dd HH:mm:ss') AS event_ts FROM {table}"
    )


def _make_table(spark):
    ns = f"dd_{uuid.uuid4().hex[:8]}"
    table = f"local.{ns}.dedup"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    spark.sql(f"CREATE TABLE {table} "
              "(event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP) USING iceberg")
    return table


def test_one_row_per_id(submission, spark_iceberg, duckdb_con, tmp_path):
    s = spark_iceberg
    table = _make_table(s)
    src = tmp_path / "src"; src.mkdir()
    ckpt = str(tmp_path / "ck")

    _write(src, RUN1, 1)
    submission.dedup(s, str(src), table, ckpt)
    _write(src, RUN2, 2)
    submission.dedup(s, str(src), table, ckpt)

    out = _read(s, table)
    assert out.count() == len(DISTINCT_IDS)
    assert {r["event_id"] for r in out.collect()} == DISTINCT_IDS
    assert_frames_equal(out, _oracle(duckdb_con, RUN1 + RUN2), rel_tol=1e-6)


def test_no_duplicate_ids(submission, spark_iceberg, tmp_path):
    s = spark_iceberg
    table = _make_table(s)
    src = tmp_path / "src"; src.mkdir()
    ckpt = str(tmp_path / "ck")
    _write(src, RUN1, 1); submission.dedup(s, str(src), table, ckpt)
    _write(src, RUN2, 2); submission.dedup(s, str(src), table, ckpt)
    out = _read(s, table)
    assert out.select("event_id").distinct().count() == out.count()
