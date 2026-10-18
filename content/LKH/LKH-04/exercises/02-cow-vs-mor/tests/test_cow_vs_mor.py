"""Tests for LKH-04 / 02-cow-vs-mor.

Create a CoW and a MoR table, delete the same row from each, and assert the
measurable metadata differences (never timing):
  - MoR: a position-delete file appears (content=1); data-file set unchanged.
  - CoW: no delete file; the affected data file is REWRITTEN (data-file set changes).
Shared Iceberg Spark session -> unique names per test. SKIPS without JARs.
"""

from __future__ import annotations

import uuid

import pytest


def _mk(spark, submission, mode):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.rl")
    name = f"local.rl.t_{mode.replace('-', '')}_{uuid.uuid4().hex[:8]}"
    submission.create_table(spark, name, mode)
    # Write all rows into exactly ONE data file (coalesce(1)) so the deleted row
    # (id=2) shares its file with survivors: MoR then writes a position-delete file
    # instead of dropping the whole file, and CoW rewrites that single file.
    df = spark.createDataFrame([(1, "US"), (2, "DE"), (3, "FR")], ["id", "cc"]).coalesce(1)
    df.writeTo(name).append()
    return name


@pytest.fixture
def tables(spark_iceberg, submission):
    s = spark_iceberg
    made = []

    def make(mode):
        n = _mk(s, submission, mode)
        made.append(n)
        return n

    yield s, make
    for n in made:
        s.sql(f"DROP TABLE IF EXISTS {n} PURGE")


def test_mor_adds_delete_file_keeps_data_files(submission, tables):
    s, make = tables
    name = make("merge-on-read")
    before = submission.data_file_paths(s, name)
    assert len(before) >= 1
    s.sql(f"DELETE FROM {name} WHERE id = 2")
    bc = submission.files_by_content(s, name)
    # a position-delete file (content=1) must be present
    assert bc.get(1, 0) >= 1, f"MoR must add a delete file; got {bc}"
    # the data files are NOT rewritten
    after = submission.data_file_paths(s, name)
    assert after == before, "MoR must not rewrite data files"


def test_cow_rewrites_data_file_no_delete_file(submission, tables):
    s, make = tables
    name = make("copy-on-write")
    before = submission.data_file_paths(s, name)
    assert len(before) >= 1
    s.sql(f"DELETE FROM {name} WHERE id = 2")
    bc = submission.files_by_content(s, name)
    # NO delete file for CoW
    assert bc.get(1, 0) == 0, f"CoW must not add a delete file; got {bc}"
    # the data-file set changed: the file holding id=2 was rewritten/removed
    after = submission.data_file_paths(s, name)
    assert after != before, "CoW must rewrite (change) the affected data file"
    # and the deleted row is gone
    assert s.sql(f"SELECT count(*) c FROM {name} WHERE id = 2").collect()[0]["c"] == 0


def test_both_modes_return_correct_rows_after_delete(submission, tables):
    """Whatever the mode, the logical result of the delete is identical."""
    s, make = tables
    for mode in ("merge-on-read", "copy-on-write"):
        name = make(mode)
        s.sql(f"DELETE FROM {name} WHERE id = 2")
        ids = sorted(r["id"] for r in s.sql(f"SELECT id FROM {name}").collect())
        assert ids == [1, 3], f"{mode}: wrong rows after delete: {ids}"


def test_files_by_content_before_delete_is_data_only(submission, tables):
    s, make = tables
    name = make("merge-on-read")
    bc = submission.files_by_content(s, name)
    # a fresh table has data files only, and no delete files.
    assert bc.get(0, 0) >= 1 and bc.get(1, 0) == 0, f"fresh table should be data-only: {bc}"
