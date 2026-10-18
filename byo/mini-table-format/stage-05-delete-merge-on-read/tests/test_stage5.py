"""Stage 5 — delete files / merge-on-read (row-level changes)."""

import pyarrow as pa
import pyarrow.compute as pc

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


def test_delete_hides_rows_matches_oracle(submission, tmp_path, duckdb_con):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2, 3, 4, 5]))
    t.append(_rows([6, 7, 8]))

    t.delete(lambda tbl: pa.array([v % 2 == 0 for v in tbl.column("id").to_pylist()]))  # evens

    duckdb_con.execute("CREATE TABLE o(id BIGINT)")
    duckdb_con.execute("INSERT INTO o SELECT * FROM range(1, 9)")
    oracle = [r[0] for r in duckdb_con.execute(
        "SELECT id FROM o WHERE id % 2 != 0 ORDER BY id").fetchall()]
    assert _ids(t.scan()) == oracle == [1, 3, 5, 7]


def test_delete_does_not_rewrite_data_files(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2, 3]))
    data_keys = set(store.list("t/data/"))
    before = {k: store.get(k) for k in data_keys}

    t.delete(lambda tbl: pc.greater(tbl.column("id"), 1))   # delete 2,3

    # data files untouched (merge-on-read, not copy-on-write)
    assert set(store.list("t/data/")) == data_keys
    for k, v in before.items():
        assert store.get(k) == v
    assert _ids(t.scan()) == [1]


def test_delete_is_snapshotted_time_travel(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    s_before = t.append(_rows([1, 2, 3]))
    s_after = t.delete(lambda tbl: pc.equal(tbl.column("id"), 2))

    assert _ids(t.scan(snapshot_id=s_before)) == [1, 2, 3]   # pre-delete snapshot
    assert _ids(t.scan(snapshot_id=s_after)) == [1, 3]
    assert _ids(t.scan()) == [1, 3]


def test_delete_targets_correct_positions_across_files(submission, tmp_path):
    """Two files with overlapping id ranges: deletes must apply per data file by
    position, not globally — otherwise the wrong rows vanish."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([10, 20, 30]))     # file A positions 0,1,2
    t.append(_rows([20, 40]))         # file B positions 0,1 (20 duplicated)

    t.delete(lambda tbl: pc.equal(tbl.column("id"), 20))  # both 20s go

    assert _ids(t.scan()) == [10, 30, 40]
