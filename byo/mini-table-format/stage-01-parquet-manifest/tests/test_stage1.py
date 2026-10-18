"""Stage 1 — Parquet files + JSON manifest (metadata vs. data).

The tests act as a *reader engine*: they open the learner's JSON manifests and
Parquet data files with real readers (json, pyarrow) and compare the table's own
``scan()`` against a DuckDB oracle over the same rows (G6).
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from objectstore import ObjectStore


SCHEMA = pa.schema([("id", pa.int64()), ("amt", pa.float64())])


def _store(tmp_path):
    return ObjectStore(str(tmp_path / "wh"))


def _rows(ids, amts):
    return pa.table({"id": ids, "amt": amts}, schema=SCHEMA)


def _sorted(tbl):
    return sorted(zip(tbl.column("id").to_pylist(), tbl.column("amt").to_pylist()))


def test_create_then_scan_empty(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    out = t.scan()
    assert out.num_rows == 0


def test_create_twice_fails(submission, tmp_path):
    store = _store(tmp_path)
    submission.Table.create(store, "t", SCHEMA)
    with pytest.raises(Exception):
        submission.Table.create(store, "t", SCHEMA)


def test_load_missing_fails(submission, tmp_path):
    store = _store(tmp_path)
    with pytest.raises(Exception):
        submission.Table.load(store, "ghost")


def test_append_and_scan_matches_oracle(submission, tmp_path, duckdb_con):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2, 3], [10.0, 20.0, 30.0]))
    t.append(_rows([4, 5], [40.0, 50.0]))

    got = _sorted(t.scan())

    # DuckDB oracle over the SAME input rows.
    duckdb_con.execute(
        "CREATE TABLE o(id BIGINT, amt DOUBLE); "
        "INSERT INTO o VALUES (1,10.0),(2,20.0),(3,30.0),(4,40.0),(5,50.0)")
    oracle = duckdb_con.execute("SELECT id, amt FROM o ORDER BY id").fetchall()
    assert got == [tuple(r) for r in oracle]


def test_metadata_is_json_and_lists_data_files(submission, tmp_path):
    """Reader-engine view: the manifest is JSON and names Parquet data files
    that a real Parquet reader can open, and metadata != data."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2, 3], [1.0, 2.0, 3.0]))

    meta_keys = [k for k in store.list("t/metadata/") if k.endswith(".json")]
    assert meta_keys, "no JSON metadata written"
    # The latest metadata parses as JSON and carries a manifest of data files.
    latest = sorted(meta_keys)[-1]
    meta = json.loads(store.get(latest).decode())
    manifest = meta.get("manifest")
    assert isinstance(manifest, list) and len(manifest) == 1
    entry = manifest[0]
    assert "path" in entry and entry.get("num_rows") == 3

    # Data files are Parquet, separate objects, openable by a real reader.
    data_key = entry["path"]
    assert data_key != latest
    assert data_key.endswith(".parquet")
    tbl = pq.read_table(store.path(data_key))
    assert tbl.num_rows == 3
    assert set(tbl.schema.names) == {"id", "amt"}


def test_append_is_immutable_adds_new_files(submission, tmp_path):
    """A second append must ADD a data file, never rewrite the first one."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1], [1.0]))
    data_after_1 = set(store.list("t/data/"))
    assert len(data_after_1) == 1
    first = next(iter(data_after_1))
    first_bytes = store.get(first)

    t.append(_rows([2], [2.0]))
    data_after_2 = set(store.list("t/data/"))
    assert len(data_after_2) == 2
    assert data_after_1 < data_after_2          # strictly added
    assert store.get(first) == first_bytes       # original file untouched
    assert t.scan().num_rows == 2
