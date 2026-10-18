"""Stage 4 — concurrent writers: conflict detection + retry (isolation).

Deterministic: writers are released together by a threading.Barrier (no sleep).
The result must equal a serial oracle (every append applied — no lost updates),
and a concurrent reader must only ever observe complete, consistent snapshots.
"""

import threading

import pyarrow as pa

from objectstore import ObjectStore

SCHEMA = pa.schema([("id", pa.int64()), ("amt", pa.float64())])


def _store(tmp_path):
    return ObjectStore(str(tmp_path / "wh"))


def _one(i):
    return pa.table({"id": [i], "amt": [float(i)]}, schema=SCHEMA)


def _ids(tbl):
    return sorted(tbl.column("id").to_pylist())


def test_concurrent_appends_no_lost_update(submission, tmp_path):
    store = _store(tmp_path)
    submission.Table.create(store, "t", SCHEMA)

    n_writers = 6
    per_writer = 8
    barrier = threading.Barrier(n_writers)
    errors = []

    def writer(wid):
        try:
            t = submission.Table.load(store, "t")
            barrier.wait()                       # maximize contention, no sleep
            for j in range(per_writer):
                t.append(_one(wid * 1000 + j))
        except Exception as e:                   # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(n_writers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors, f"writer errors: {errors}"

    # Serial oracle: the full set of ids that every append should have added.
    expected = sorted(w * 1000 + j for w in range(n_writers) for j in range(per_writer))
    got = _ids(submission.Table.load(store, "t").scan())
    assert got == expected, "lost update: final rows != serial oracle"


def test_snapshot_chain_well_formed_after_contention(submission, tmp_path):
    store = _store(tmp_path)
    submission.Table.create(store, "t", SCHEMA)
    n_writers = 5
    barrier = threading.Barrier(n_writers)

    def writer(wid):
        t = submission.Table.load(store, "t")
        barrier.wait()
        t.append(_one(wid))

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(n_writers)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    snaps = submission.Table.load(store, "t").snapshots()
    ids = [s.snapshot_id for s in snaps]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)                 # no duplicate snapshot ids
    # create snapshot + one per successful append
    assert len(snaps) == 1 + n_writers


def test_reader_always_sees_consistent_snapshot(submission, tmp_path):
    """A reader engine running while writers commit must only ever see a complete
    snapshot: its row count is always one of the legal prefixes, never partial."""
    store = _store(tmp_path)
    submission.Table.create(store, "t", SCHEMA)
    n_writers = 6
    barrier = threading.Barrier(n_writers + 1)
    stop = threading.Event()
    bad = []

    def writer(wid):
        t = submission.Table.load(store, "t")
        barrier.wait()
        t.append(_one(wid))

    def reader():
        rt = submission.Table.load(store, "t")
        barrier.wait()
        while not stop.is_set():
            out = rt.scan()
            cnt = out.num_rows
            # every legal snapshot has between 0 and n_writers rows, each a
            # distinct writer id — never a torn/partial read.
            if not (0 <= cnt <= n_writers):
                bad.append(("count", cnt))
            if cnt > 0 and len(set(out.column("id").to_pylist())) != cnt:
                bad.append(("dupes", out.column("id").to_pylist()))

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(n_writers)]
    rthread = threading.Thread(target=reader)
    rthread.start()
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    stop.set()
    rthread.join()

    assert not bad, f"reader saw inconsistent state: {bad[:5]}"
    assert _ids(submission.Table.load(store, "t").scan()) == list(range(n_writers))
