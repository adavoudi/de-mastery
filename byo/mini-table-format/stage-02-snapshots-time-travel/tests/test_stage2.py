"""Stage 2 — snapshots & time travel (immutability)."""

import pyarrow as pa

from objectstore import ObjectStore

SCHEMA = pa.schema([("id", pa.int64()), ("amt", pa.float64())])


def _store(tmp_path):
    return ObjectStore(str(tmp_path / "wh"))


def _rows(ids, amts):
    return pa.table({"id": ids, "amt": amts}, schema=SCHEMA)


def _ids(tbl):
    return sorted(tbl.column("id").to_pylist())


def test_create_snapshot_is_listed(submission, tmp_path):
    """Right after create() there is exactly one snapshot (the empty base), and it
    stays in the history after appends — the create commit is part of the chain."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    snaps0 = t.snapshots()
    assert len(snaps0) == 1
    assert snaps0[0].manifest == []
    create_id = snaps0[0].snapshot_id
    t.append(_rows([1], [1.0]))
    ids = [s.snapshot_id for s in t.snapshots()]
    assert create_id in ids and ids[0] == create_id    # still the first snapshot


def test_snapshots_grow_monotonically(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    base = len(t.snapshots())
    s1 = t.append(_rows([1], [1.0]))
    s2 = t.append(_rows([2], [2.0]))
    snaps = t.snapshots()
    assert len(snaps) == base + 2
    ids = [s.snapshot_id for s in snaps]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)   # strictly increasing
    assert s1 != s2
    assert t.current_snapshot().snapshot_id == s2


def test_timestamps_present_and_nondecreasing(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1], [1.0]))
    t.append(_rows([2], [2.0]))
    ts = [s.timestamp_ms for s in t.snapshots()]
    assert all(isinstance(x, int) for x in ts)
    assert ts == sorted(ts)


def test_time_travel_reads_old_state(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    s1 = t.append(_rows([1, 2, 3], [1.0, 2.0, 3.0]))
    s2 = t.append(_rows([4, 5], [4.0, 5.0]))

    assert _ids(t.scan(snapshot_id=s1)) == [1, 2, 3]
    assert _ids(t.scan(snapshot_id=s2)) == [1, 2, 3, 4, 5]
    assert _ids(t.scan()) == [1, 2, 3, 4, 5]      # no arg => latest


def test_old_snapshot_is_stable_after_more_writes(submission, tmp_path):
    """A reader that pinned s1 keeps seeing s1 no matter how many appends follow."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    s1 = t.append(_rows([1, 2], [1.0, 2.0]))
    pinned = _ids(t.scan(snapshot_id=s1))
    for i in range(5):
        t.append(_rows([100 + i], [0.0]))
    assert _ids(t.scan(snapshot_id=s1)) == pinned == [1, 2]
    assert _ids(t.scan()) == [1, 2, 100, 101, 102, 103, 104]
