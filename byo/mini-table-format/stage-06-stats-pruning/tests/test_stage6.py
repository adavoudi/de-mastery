"""Stage 6 — manifest column stats + file pruning (planning speed).

We count the data files physically opened (``table.files_opened``) to prove that
pruning really skips non-overlapping files, and we check soundness: pruning must
never drop a row that matches the filter (inclusive bounds).
"""

import pyarrow as pa

from objectstore import ObjectStore

SCHEMA = pa.schema([("id", pa.int64()), ("amt", pa.float64())])


def _store(tmp_path):
    return ObjectStore(str(tmp_path / "wh"))


def _rows(ids):
    return pa.table({"id": ids, "amt": [float(i) for i in ids]}, schema=SCHEMA)


def _ids(tbl):
    if tbl.num_rows == 0 or "id" not in tbl.schema.names:
        return []
    return sorted(tbl.column("id").to_pylist())


def _fresh(submission, tmp_path):
    """One file per id-range: [0..9], [100..109], [200..209]."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows(list(range(0, 10))))
    t.append(_rows(list(range(100, 110))))
    t.append(_rows(list(range(200, 210))))
    return t


def test_prune_opens_only_overlapping_files(submission, tmp_path):
    t = _fresh(submission, tmp_path)
    t.files_opened.clear()
    out = t.scan(filter=("id", 100, 109))
    assert _ids(out) == list(range(100, 110))
    assert len(t.files_opened) == 1, (
        f"opened {len(t.files_opened)} files, pruning should open exactly 1")


def test_prune_narrow_range_single_file(submission, tmp_path):
    t = _fresh(submission, tmp_path)
    t.files_opened.clear()
    out = t.scan(filter=("id", 205, 207))
    assert _ids(out) == [205, 206, 207]
    assert len(t.files_opened) == 1


def test_no_filter_reads_all_files(submission, tmp_path):
    t = _fresh(submission, tmp_path)
    t.files_opened.clear()
    out = t.scan()
    assert _ids(out) == list(range(0, 10)) + list(range(100, 110)) + list(range(200, 210))
    assert len(t.files_opened) == 3


def test_inclusive_boundary_file_not_pruned(submission, tmp_path):
    """Filter touches a file only at its boundary value (max == lo). Inclusive
    bounds mean that file MUST be read; a strict < would wrongly prune it."""
    t = _fresh(submission, tmp_path)
    t.files_opened.clear()
    # file one is [0..9]; lo == 9 == that file's max.
    out = t.scan(filter=("id", 9, 105))
    assert _ids(out) == [9] + list(range(100, 106))
    # must have opened the [0..9] and [100..109] files (2), pruned [200..209].
    assert len(t.files_opened) == 2


def test_pruning_is_sound_matches_oracle(submission, tmp_path, duckdb_con):
    t = _fresh(submission, tmp_path)
    all_ids = list(range(0, 10)) + list(range(100, 110)) + list(range(200, 210))
    duckdb_con.execute("CREATE TABLE o(id BIGINT)")
    duckdb_con.executemany("INSERT INTO o VALUES (?)", [(i,) for i in all_ids])
    for lo, hi in [(5, 205), (0, 300), (50, 60), (105, 105)]:
        oracle = [r[0] for r in duckdb_con.execute(
            "SELECT id FROM o WHERE id BETWEEN ? AND ? ORDER BY id", [lo, hi]).fetchall()]
        assert _ids(t.scan(filter=("id", lo, hi))) == oracle, f"filter {(lo, hi)}"
