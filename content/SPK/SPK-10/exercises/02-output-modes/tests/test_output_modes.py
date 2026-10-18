"""Tests for SPK-10 / 02-output-modes.

Uses the bare `spark` fixture and the in-memory sink (no Iceberg), so it runs with no
Docker and no JARs. Writes several JSON batches, runs the learner's query, and asserts
the final per-company counts equal a DuckDB oracle. A query using append on this
aggregation must fail (Spark rejects it).
"""

import json
import uuid

from de_testkit import assert_frames_equal

EVENTS = [
    {"event_id": 1, "company_id": "C001", "amount": 10.0, "event_ts": "2026-01-01T00:00:00"},
    {"event_id": 2, "company_id": "C001", "amount": 20.0, "event_ts": "2026-01-01T00:01:00"},
    {"event_id": 3, "company_id": "C002", "amount": 30.0, "event_ts": "2026-01-01T00:02:00"},
    {"event_id": 4, "company_id": "C002", "amount": 40.0, "event_ts": "2026-01-01T00:03:00"},
    {"event_id": 5, "company_id": "C002", "amount": 50.0, "event_ts": "2026-01-01T00:04:00"},
    {"event_id": 6, "company_id": "C003", "amount": 60.0, "event_ts": "2026-01-01T00:05:00"},
]


def _setup_src(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    # Two files so the query sees more than one chunk of input.
    (src / "b1.json").write_text("\n".join(json.dumps(r) for r in EVENTS[:3]) + "\n")
    (src / "b2.json").write_text("\n".join(json.dumps(r) for r in EVENTS[3:]) + "\n")
    return src


def _oracle(con, src_glob):
    return con.execute(
        f"SELECT company_id, count(*) AS n FROM read_json_auto('{src_glob}') GROUP BY company_id"
    ).arrow()


def test_counts_match_oracle(submission, spark, duckdb_con, tmp_path):
    src = _setup_src(tmp_path)
    qname = f"counts_{uuid.uuid4().hex[:8]}"
    submission.run_counts(spark, str(src), qname)
    out = spark.sql(f"SELECT company_id, n FROM {qname}")
    assert_frames_equal(out, _oracle(duckdb_con, str(src / "*.json")))


def test_one_row_per_company(submission, spark, tmp_path):
    src = _setup_src(tmp_path)
    qname = f"counts_{uuid.uuid4().hex[:8]}"
    submission.run_counts(spark, str(src), qname)
    out = spark.sql(f"SELECT company_id, n FROM {qname}")
    assert out.count() == 3
    assert out.select("company_id").distinct().count() == 3


def test_expected_columns(submission, spark, tmp_path):
    src = _setup_src(tmp_path)
    qname = f"counts_{uuid.uuid4().hex[:8]}"
    submission.run_counts(spark, str(src), qname)
    out = spark.sql(f"SELECT * FROM {qname}")
    assert set(out.columns) == {"company_id", "n"}
