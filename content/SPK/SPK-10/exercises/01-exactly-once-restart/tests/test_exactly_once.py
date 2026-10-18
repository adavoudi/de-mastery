"""Tests for SPK-10 / 01-exactly-once-restart.

Drives the learner's run_batch across THREE invocations that share one checkpoint:
batches 1-2, then a new batch, then a no-op run. Proves the final Iceberg table has
no duplicates and no loss (exactly-once) and matches a DuckDB oracle over all the
JSON files. Deterministic: trigger(availableNow) + processAllAvailable, no sleep.

Requires the pinned Iceberg JARs (spark_iceberg fixture); SKIPS cleanly without them.
"""

import json
import uuid

from de_testkit import assert_frames_equal

EVENTS_1 = [
    {"event_id": 1, "company_id": "C001", "amount": 10.0, "event_ts": "2026-01-01T00:00:00"},
    {"event_id": 2, "company_id": "C001", "amount": 20.0, "event_ts": "2026-01-01T00:01:00"},
]
EVENTS_2 = [
    {"event_id": 3, "company_id": "C002", "amount": 30.0, "event_ts": "2026-01-01T00:02:00"},
]
EVENTS_3 = [
    {"event_id": 4, "company_id": "C002", "amount": 40.0, "event_ts": "2026-01-01T00:03:00"},
    {"event_id": 5, "company_id": "C001", "amount": 50.0, "event_ts": "2026-01-01T00:04:00"},
]
ALL_EVENTS = EVENTS_1 + EVENTS_2 + EVENTS_3


def _write_batch(src_dir, rows, idx):
    f = src_dir / f"batch-{idx:04d}.json"
    f.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def _oracle(con, src_glob):
    return con.execute(
        f"SELECT event_id, company_id, amount, event_ts FROM read_json_auto('{src_glob}')"
    ).arrow()


def _read_table(spark, table):
    spark.sql(f"REFRESH TABLE {table}")
    return spark.table(table).select("event_id", "company_id", "amount", "event_ts")


def test_exactly_once_across_restarts(submission, spark_iceberg, duckdb_con, tmp_path):
    s = spark_iceberg
    ns = f"stream_{uuid.uuid4().hex[:8]}"
    table = f"local.{ns}.events"
    s.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    s.sql(f"CREATE TABLE {table} "
          "(event_id INT, company_id STRING, amount DOUBLE, event_ts STRING) USING iceberg")

    src = tmp_path / "src"
    src.mkdir()
    ckpt = str(tmp_path / "ckpt")

    # Run 1: first two files present.
    _write_batch(src, EVENTS_1, 1)
    _write_batch(src, EVENTS_2, 2)
    submission.run_batch(s, str(src), table, ckpt)
    rows_after_1 = _read_table(s, table).count()

    # Run 2: a new file; resume from the SAME checkpoint.
    _write_batch(src, EVENTS_3, 3)
    submission.run_batch(s, str(src), table, ckpt)
    rows_after_2 = _read_table(s, table).count()

    # Run 3: no new files; must add nothing.
    submission.run_batch(s, str(src), table, ckpt)
    rows_after_3 = _read_table(s, table).count()

    assert rows_after_1 == len(EVENTS_1 + EVENTS_2), rows_after_1
    assert rows_after_2 == len(ALL_EVENTS), rows_after_2
    assert rows_after_3 == len(ALL_EVENTS), rows_after_3  # exactly-once: no re-ingest

    out = _read_table(s, table)
    # No duplicate event_ids (no double-write).
    assert out.select("event_id").distinct().count() == len(ALL_EVENTS)
    # Equals the oracle set over every event that was dropped (no loss).
    assert_frames_equal(out, _oracle(duckdb_con, str(src / "batch-*.json")), rel_tol=1e-9)


def test_no_loss_single_run(submission, spark_iceberg, duckdb_con, tmp_path):
    # A single run must ingest every available event exactly once.
    s = spark_iceberg
    ns = f"stream_{uuid.uuid4().hex[:8]}"
    table = f"local.{ns}.events"
    s.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    s.sql(f"CREATE TABLE {table} "
          "(event_id INT, company_id STRING, amount DOUBLE, event_ts STRING) USING iceberg")
    src = tmp_path / "src2"
    src.mkdir()
    _write_batch(src, ALL_EVENTS, 1)
    submission.run_batch(s, str(src), table, str(tmp_path / "ckpt2"))
    out = _read_table(s, table)
    assert out.count() == len(ALL_EVENTS)
    assert_frames_equal(out, _oracle(duckdb_con, str(src / "batch-*.json")), rel_tol=1e-9)
