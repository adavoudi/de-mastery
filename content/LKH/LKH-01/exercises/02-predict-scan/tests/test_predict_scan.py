"""Tests for LKH-01 / 02-predict-scan.

Data is written one file per year with a single country per file, so the exact set
of files each predicate reads is known by construction. The oracle is an
independent, correct pyiceberg scan plan computed in the test. No JVM / Docker.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo, GreaterThanOrEqual

# (year, country) -> one data file each. 4 files total.
LAYOUT = [(2021, "US"), (2022, "DE"), (2023, "US"), (2024, "FR")]


@pytest.fixture
def table(pyiceberg_catalog):
    cat = pyiceberg_catalog
    cat.create_namespace("db")
    schema = pa.schema([("id", pa.int64()), ("yr", pa.int64()), ("cc", pa.string())])
    t = cat.create_table("db.orders", schema=schema)
    for i, (yr, cc) in enumerate(LAYOUT):
        t.append(pa.table({"id": [i * 10 + 1, i * 10 + 2],
                           "yr": [yr, yr], "cc": [cc, cc]}))
    return cat.load_table("db.orders")


def _oracle(table, expr):
    """Independent correct scan plan -> sorted file paths."""
    return sorted(task.file.file_path for task in table.scan(row_filter=expr).plan_files())


def test_all_files_is_four(submission, table):
    allf = submission.all_files(table)
    assert len(allf) == len(LAYOUT) == 4
    assert allf == sorted(allf)


def test_equals_country_matches_oracle(submission, table):
    out = submission.files_for_equals(table, "cc", "US")
    oracle = _oracle(table, EqualTo("cc", "US"))
    assert out["paths"] == oracle
    assert out["num_files"] == len(oracle) == 2  # 2021 + 2023 files


def test_equals_prunes_strictly(submission, table):
    # A single-year predicate reads exactly one file; proves pruning really happens.
    out = submission.files_for_equals(table, "yr", 2022)
    assert out["num_files"] == 1
    assert out["num_files"] < len(submission.all_files(table))


def test_at_least_matches_oracle(submission, table):
    out = submission.files_for_at_least(table, "yr", 2023)
    oracle = _oracle(table, GreaterThanOrEqual("yr", 2023))
    assert out["paths"] == oracle
    assert out["num_files"] == len(oracle) == 2  # 2023 + 2024 files


def test_at_least_includes_boundary(submission, table):
    """yr >= 2023 must include the 2023 file (kills the strict '>' mutant)."""
    out = submission.files_for_at_least(table, "yr", 2023)
    full = submission.all_files(table)
    # the 2023 file is index 2 in LAYOUT; identify it by scanning each file's year.
    # Correct answer keeps exactly 2 files; strict '>' would keep only 1 (2024).
    assert out["num_files"] == 2


def test_filtered_is_subset_of_all(submission, table):
    allf = set(submission.all_files(table))
    for out in (
        submission.files_for_equals(table, "cc", "US"),
        submission.files_for_at_least(table, "yr", 2023),
    ):
        assert set(out["paths"]) <= allf, "filtered plan must be a subset of all files"


def test_paths_are_real_data_files(submission, table):
    out = submission.files_for_equals(table, "cc", "US")
    for p in out["paths"]:
        assert p.endswith(".parquet"), p
