"""Tests for LKH-04 / 01-scd2-merge.

Seed a day-1 dimension, apply a day-2 batch via the learner, and compare the whole
table to an independent DuckDB SCD2 oracle (G6). Also assert SCD2 invariants:
exactly one current row per key, and old.valid_to == new.valid_from (contiguous).
Shared Iceberg Spark session -> unique table name per test. SKIPS without JARs.
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest
from de_testkit import assert_frames_equal

DAY1 = [(1, "x"), (2, "y")]           # loaded with valid_from = 2026-01-01
BATCH2 = [(1, "z"), (2, "y"), (3, "w")]  # 1 changed, 2 unchanged, 3 new
D1, D2 = "2026-01-01", "2026-01-02"


def _duck_oracle(duckdb_con):
    con = duckdb_con
    con.execute(
        "CREATE TABLE dim(nk INT, attr VARCHAR, valid_from DATE, valid_to DATE, is_current BOOLEAN)"
    )
    for nk, attr in DAY1:
        con.execute(
            f"INSERT INTO dim VALUES ({nk}, '{attr}', DATE '{D1}', NULL, true)"
        )
    con.execute("CREATE TABLE src(nk INT, attr VARCHAR)")
    con.executemany("INSERT INTO src VALUES (?, ?)", BATCH2)
    # close changed current rows
    con.execute(
        f"UPDATE dim SET valid_to = DATE '{D2}', is_current = false "
        f"WHERE is_current AND nk IN (SELECT s.nk FROM src s WHERE s.attr <> dim.attr)"
    )
    # insert new versions for keys no longer present as current
    con.execute(
        f"INSERT INTO dim SELECT s.nk, s.attr, DATE '{D2}', NULL, true FROM src s "
        f"WHERE s.nk NOT IN (SELECT nk FROM dim WHERE is_current)"
    )
    return con.execute(
        "SELECT nk, attr, CAST(valid_from AS VARCHAR) vf, CAST(valid_to AS VARCHAR) vt, is_current FROM dim"
    ).arrow()


@pytest.fixture
def dim(spark_iceberg):
    s = spark_iceberg
    s.sql("CREATE NAMESPACE IF NOT EXISTS local.scd")
    name = f"local.scd.dim_{uuid.uuid4().hex[:8]}"
    s.sql(
        f"CREATE TABLE {name} (nk INT, attr STRING, valid_from DATE, valid_to DATE, "
        f"is_current BOOLEAN) USING iceberg"
    )
    for nk, attr in DAY1:
        s.sql(f"INSERT INTO {name} VALUES ({nk}, '{attr}', DATE'{D1}', NULL, true)")
    yield s, name
    s.sql(f"DROP TABLE IF EXISTS {name} PURGE")


def _actual(s, name):
    return s.sql(
        f"SELECT nk, attr, CAST(valid_from AS STRING) vf, "
        f"CAST(valid_to AS STRING) vt, is_current FROM {name}"
    )


def test_matches_duckdb_oracle(submission, dim, duckdb_con):
    s, name = dim
    submission.apply_scd2(s, name, "SELECT * FROM VALUES (1,'z'),(2,'y'),(3,'w') AS t(nk, attr)", D2)
    assert_frames_equal(_actual(s, name), _duck_oracle(duckdb_con))


def test_one_current_row_per_key(submission, dim):
    s, name = dim
    submission.apply_scd2(s, name, "SELECT * FROM VALUES (1,'z'),(2,'y'),(3,'w') AS t(nk, attr)", D2)
    rows = s.sql(
        f"SELECT nk, count(*) c FROM {name} WHERE is_current = true GROUP BY nk"
    ).collect()
    assert all(r["c"] == 1 for r in rows), "a key has more than one current row"
    # current rows must have NULL valid_to; closed rows must have a non-NULL valid_to
    assert s.sql(f"SELECT count(*) c FROM {name} WHERE is_current = true AND valid_to IS NOT NULL").collect()[0]["c"] == 0
    assert s.sql(f"SELECT count(*) c FROM {name} WHERE is_current = false AND valid_to IS NULL").collect()[0]["c"] == 0


def test_history_preserved_for_changed_key(submission, dim):
    """nk=1 changed x->z: both the closed 'x' and the open 'z' version must exist."""
    s, name = dim
    submission.apply_scd2(s, name, "SELECT * FROM VALUES (1,'z') AS t(nk, attr)", D2)
    vals = sorted(r["attr"] for r in s.sql(f"SELECT attr FROM {name} WHERE nk = 1").collect())
    assert vals == ["x", "z"], f"history lost (SCD1?): {vals}"


def test_contiguous_dates_no_gap_or_overlap(submission, dim):
    """old.valid_to must equal new.valid_from for a changed key (contiguous)."""
    s, name = dim
    submission.apply_scd2(s, name, "SELECT * FROM VALUES (1,'z') AS t(nk, attr)", D2)
    closed_to = s.sql(
        f"SELECT CAST(valid_to AS STRING) v FROM {name} WHERE nk = 1 AND is_current = false"
    ).collect()[0]["v"]
    open_from = s.sql(
        f"SELECT CAST(valid_from AS STRING) v FROM {name} WHERE nk = 1 AND is_current = true"
    ).collect()[0]["v"]
    assert closed_to == open_from == D2, f"off-by-one date: closed_to={closed_to} open_from={open_from}"


def test_unchanged_key_untouched(submission, dim):
    s, name = dim
    submission.apply_scd2(s, name, "SELECT * FROM VALUES (2,'y') AS t(nk, attr)", D2)
    rows = s.sql(f"SELECT * FROM {name} WHERE nk = 2").collect()
    assert len(rows) == 1 and rows[0]["is_current"] and rows[0]["valid_to"] is None
