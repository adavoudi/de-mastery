"""Stage 7 — compaction + snapshot expiration (maintenance).

Readers pinned to old snapshots stay consistent until those snapshots are expired;
expiration must never delete a file still referenced by a retained snapshot.
"""

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


def _build(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2, 3]))
    t.append(_rows([4, 5, 6]))
    s_del = t.delete(lambda tbl: pc.equal(tbl.column("id"), 5))   # live: 1,2,3,4,6
    return store, t, s_del


def test_compact_collapses_manifest_keeps_rows(submission, tmp_path):
    store, t, _ = _build(submission, tmp_path)
    assert _ids(t.scan()) == [1, 2, 3, 4, 6]
    s_c = t.compact()
    snap = t.current_snapshot()
    assert len(snap.manifest) == 1, "compaction should leave one data file"
    assert len(snap.deletes) == 0, "compaction should clear delete files"
    assert _ids(t.scan()) == [1, 2, 3, 4, 6]           # unchanged content
    assert _ids(t.scan(snapshot_id=s_c)) == [1, 2, 3, 4, 6]


def test_old_snapshot_consistent_after_compaction(submission, tmp_path):
    store, t, s_del = _build(submission, tmp_path)
    t.compact()
    # the pre-compaction snapshot still reads its exact old live rows: compaction
    # only ADDS a file, so the old data/delete files are intact.
    assert _ids(t.scan(snapshot_id=s_del)) == [1, 2, 3, 4, 6]


def test_expire_gcs_only_unreferenced_files(submission, tmp_path):
    store, t, s_del = _build(submission, tmp_path)
    t.compact()                                   # old files now unreferenced by latest
    import time
    future = int(time.time() * 1000) + 10_000_000
    data_before = set(store.list("t/data/"))
    removed = t.expire(future)                    # expire everything but the latest
    assert removed > 0
    # latest snapshot's single file must survive and still read correctly
    assert _ids(t.scan()) == [1, 2, 3, 4, 6]
    live_file = t.current_snapshot().manifest[0]["path"]
    assert store.exists(live_file)
    # strictly fewer data files than before (old ones GC'd)
    assert len(set(store.list("t/data/"))) < len(data_before)


def test_expire_keeps_files_of_every_retained_snapshot(submission, tmp_path):
    """Retain a NON-latest snapshot that references files the latest no longer
    does (post-compaction). expire must keep ALL files any retained snapshot
    references, not just the current one. Kills expire-deletes-referenced."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    s_pre = t.append(_rows([1, 2, 3]))            # references data file A
    pre_files = {e["path"] for e in t.current_snapshot().manifest}
    s_comp = t.compact()                           # new file B; A now only in s_pre
    comp_files = {e["path"] for e in t.current_snapshot().manifest}
    assert pre_files and comp_files and pre_files != comp_files

    # Expire with a cutoff of 0 => nothing is "too old" (all timestamps >= 0), so
    # BOTH s_pre and s_comp are retained. A correct expire removes nothing that is
    # still referenced by s_pre or s_comp.
    t.expire(0)
    for key in pre_files | comp_files:
        assert store.exists(key), f"expire deleted a file referenced by a retained snapshot: {key}"
    # the retained pre-compaction snapshot is still fully readable
    assert _ids(t.scan(snapshot_id=s_pre)) == [1, 2, 3]
    assert _ids(t.scan(snapshot_id=s_comp)) == [1, 2, 3]


def test_expire_never_deletes_referenced_files(submission, tmp_path):
    """Keep a recent snapshot: all files it references must remain on disk."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2]))
    import time
    cutoff = int(time.time() * 1000) + 1          # everything so far is "old"
    # a NEW append after the cutoff -> this snapshot is retained.
    time_marker = cutoff
    s_keep = t.append(_rows([3, 4]))

    # expire snapshots strictly older than the marker; the retained snapshot(s)
    # still reference their data files, which must NOT be collected.
    referenced = {e["path"] for e in t.current_snapshot().manifest}
    t.expire(time_marker)
    for key in referenced:
        assert store.exists(key), f"expire deleted a referenced file: {key}"
    assert _ids(t.scan()) == [1, 2, 3, 4]
