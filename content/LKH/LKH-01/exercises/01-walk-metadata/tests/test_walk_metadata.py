"""Tests for LKH-01 / 01-walk-metadata.

We build a table with a KNOWN number of appends (-> snapshots/manifests) and a
known number of rows, so expected counts are derived from construction. An
independent manifest walk cross-checks the data-file set (oracle = pyiceberg's own
manifest entries, read a second, independent way).

No JVM, no Docker: uses the pure-Python `pyiceberg_catalog` fixture.
"""

from __future__ import annotations

import pyarrow as pa
import pytest


# Three appends with known row counts -> 3 data files, 3 manifests, 3 snapshots, 6 rows.
APPENDS = [
    [(1, "US"), (2, "US")],
    [(3, "DE")],
    [(4, "US"), (5, "FR"), (6, "DE")],
]
TOTAL_ROWS = sum(len(a) for a in APPENDS)
NUM_APPENDS = len(APPENDS)


@pytest.fixture
def table(pyiceberg_catalog):
    cat = pyiceberg_catalog
    cat.create_namespace("db")
    schema = pa.schema([("id", pa.int64()), ("cc", pa.string())])
    t = cat.create_table("db.events", schema=schema)
    for batch in APPENDS:
        ids = [r[0] for r in batch]
        ccs = [r[1] for r in batch]
        t.append(pa.table({"id": ids, "cc": ccs}))
    return cat.load_table("db.events")


def _independent_file_set(t):
    """Oracle: collect current-snapshot data-file paths a second, independent way
    (via the inspect.files Arrow table, content==0)."""
    files = t.inspect.files()
    paths = files.column("file_path").to_pylist()
    contents = files.column("content").to_pylist()
    return {p for p, k in zip(paths, contents) if k == 0}


def test_counts(submission, table):
    w = submission.walk(table)
    assert w["num_snapshots"] == NUM_APPENDS
    assert w["num_data_files"] == NUM_APPENDS  # one data file per append here
    assert w["total_records"] == TOTAL_ROWS
    assert w["num_manifests"] >= 1


def test_metadata_location_is_current_json(submission, table):
    w = submission.walk(table)
    assert w["metadata_location"] == table.metadata_location
    assert w["metadata_location"].endswith(".metadata.json")


def test_file_paths_match_independent_oracle(submission, table):
    w = submission.walk(table)
    assert set(w["data_file_paths"]) == _independent_file_set(table)
    # no duplicates (would signal walking all snapshots)
    assert len(w["data_file_paths"]) == len(set(w["data_file_paths"]))


def test_records_per_file_sums_to_total(submission, table):
    rpf = submission.records_per_file(table)
    w = submission.walk(table)
    assert set(rpf.keys()) == set(w["data_file_paths"])
    assert sum(rpf.values()) == w["total_records"] == TOTAL_ROWS


def test_records_per_file_matches_manifest_stats(submission, table):
    """Cross-check each file's record_count against the manifest entries directly."""
    expected = {}
    snap = table.current_snapshot()
    for manifest in snap.manifests(table.io):
        for entry in manifest.fetch_manifest_entry(table.io):
            expected[entry.data_file.file_path] = entry.data_file.record_count
    assert submission.records_per_file(table) == expected


def test_manifests_not_equal_to_files_in_general(submission, table):
    """num_data_files must come from entries, not from len(manifests): kill the
    'manifests_as_files' mutant even if they coincide at 1-file-per-append by also
    checking the explicit total via records_per_file."""
    w = submission.walk(table)
    assert w["num_data_files"] == len(submission.records_per_file(table))
