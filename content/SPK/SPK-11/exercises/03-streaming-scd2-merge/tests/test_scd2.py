"""Tests for SPK-11 / 03-streaming-scd2-merge.

Drives the learner's streaming SCD2 upsert across runs sharing one checkpoint:
run 1 inserts two new customers, run 2 changes one, run 3 is a no-op repeat for the
other. Asserts the SCD2 invariants and that the CURRENT attribute per key matches a
DuckDB oracle (latest change wins). Requires the Iceberg JARs (spark_iceberg); SKIPS
cleanly without them.
"""

import json
import uuid

RUN1 = [
    {"cust_key": 1, "attr": "gold", "change_ts": "2026-01-01 00:00:00"},
    {"cust_key": 2, "attr": "silver", "change_ts": "2026-01-01 00:00:00"},
]
RUN2 = [
    {"cust_key": 1, "attr": "platinum", "change_ts": "2026-02-01 00:00:00"},  # real change
]
RUN3 = [
    {"cust_key": 2, "attr": "silver", "change_ts": "2026-03-01 00:00:00"},  # no-op repeat
]
# Final current state per key (latest change / unchanged value).
EXPECTED_CURRENT = {1: "platinum", 2: "silver"}


def _write(src, rows, idx):
    (src / f"b{idx:04d}.json").write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def _make_table(spark):
    ns = f"scd_{uuid.uuid4().hex[:8]}"
    table = f"local.{ns}.dim"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{ns}")
    spark.sql(f"CREATE TABLE {table} (cust_key INT, attr STRING, valid_from TIMESTAMP, "
              "valid_to TIMESTAMP, is_current BOOLEAN) USING iceberg")
    return table


def _drive(submission, spark, table, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    ckpt = str(tmp_path / "ck")
    _write(src, RUN1, 1); submission.upsert_scd2(spark, str(src), table, ckpt)
    _write(src, RUN2, 2); submission.upsert_scd2(spark, str(src), table, ckpt)
    _write(src, RUN3, 3); submission.upsert_scd2(spark, str(src), table, ckpt)
    spark.sql(f"REFRESH TABLE {table}")


def test_exactly_one_open_per_key(submission, spark_iceberg, tmp_path):
    s = spark_iceberg
    table = _make_table(s)
    _drive(submission, s, table, tmp_path)
    # Exactly one open row per key, by BOTH markers (is_current and valid_to IS NULL).
    by_current = {r["cust_key"]: r["c"] for r in s.sql(
        f"SELECT cust_key, count(*) c FROM {table} WHERE is_current = true "
        f"GROUP BY cust_key").collect()}
    by_valid_to = {r["cust_key"]: r["c"] for r in s.sql(
        f"SELECT cust_key, count(*) c FROM {table} WHERE valid_to IS NULL "
        f"GROUP BY cust_key").collect()}
    assert by_current == {1: 1, 2: 1}, by_current
    assert by_valid_to == {1: 1, 2: 1}, by_valid_to


def test_current_attr_matches_oracle(submission, spark_iceberg, duckdb_con, tmp_path):
    s = spark_iceberg
    table = _make_table(s)
    _drive(submission, s, table, tmp_path)
    current = {r["cust_key"]: r["attr"] for r in s.sql(
        f"SELECT cust_key, attr FROM {table} WHERE is_current = true").collect()}
    # Oracle: latest change per key wins.
    rows = RUN1 + RUN2 + RUN3
    duckdb_con.execute("CREATE OR REPLACE TABLE ch AS SELECT * FROM (VALUES " +
                       ",".join(f"({r['cust_key']}, '{r['attr']}', TIMESTAMP '{r['change_ts']}')"
                                for r in rows) + ") AS t(cust_key, attr, change_ts)")
    oracle = {r[0]: r[1] for r in duckdb_con.execute(
        "SELECT cust_key, attr FROM (SELECT cust_key, attr, "
        "row_number() OVER (PARTITION BY cust_key ORDER BY change_ts DESC) rn FROM ch) "
        "WHERE rn = 1").fetchall()}
    assert current == oracle == EXPECTED_CURRENT, (current, oracle)


def test_history_preserved(submission, spark_iceberg, tmp_path):
    # The changed key (1) keeps its old 'gold' version as a CLOSED row with a real
    # valid_to; versioning, not overwriting.
    s = spark_iceberg
    table = _make_table(s)
    _drive(submission, s, table, tmp_path)
    hist = s.sql(f"SELECT attr, valid_to, is_current FROM {table} "
                 f"WHERE cust_key = 1 AND is_current = false").collect()
    assert len(hist) == 1, hist
    old = hist[0]
    assert old["attr"] == "gold", old
    assert old["valid_to"] is not None, "superseded row must have a closed valid_to"


def test_closed_valid_to_equals_next_valid_from(submission, spark_iceberg, tmp_path):
    # No gaps/overlaps: the old version's valid_to equals the new version's valid_from.
    s = spark_iceberg
    table = _make_table(s)
    _drive(submission, s, table, tmp_path)
    rows = s.sql(f"SELECT attr, valid_from, valid_to, is_current FROM {table} "
                 f"WHERE cust_key = 1 ORDER BY valid_from").collect()
    assert len(rows) == 2, rows
    old, new = rows[0], rows[1]
    assert old["valid_to"] == new["valid_from"], (old, new)
    assert new["valid_to"] is None and new["is_current"] is True
