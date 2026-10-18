"""Stage 3 — atomic commit via put-if-absent (optimistic concurrency).

We don't assert *which* method name you call; we assert the *behavior* that only
``put_if_absent`` gives: an already-present version object is never clobbered, and
a committed metadata object parses as complete JSON (all-or-nothing).
"""

import json

import pyarrow as pa

from objectstore import ObjectStore

SCHEMA = pa.schema([("id", pa.int64()), ("amt", pa.float64())])


def _store(tmp_path):
    return ObjectStore(str(tmp_path / "wh"))


def _rows(ids):
    return pa.table({"id": ids, "amt": [float(i) for i in ids]}, schema=SCHEMA)


def _ids(tbl):
    return sorted(tbl.column("id").to_pylist())


def _latest_version(store, name):
    best = 0
    for k in store.list(f"{name}/metadata/"):
        b = k.rsplit("/", 1)[-1]
        if b.startswith("v") and b.endswith(".json"):
            best = max(best, int(b[1:-5]))
    return best


class _MetaGuardStore(ObjectStore):
    """An ObjectStore that forbids UNCONDITIONAL writes to metadata objects.

    A correct commit publishes a version object only through ``put_if_absent``
    (the conditional create). Any plain ``put`` to a ``metadata/v*.json`` key is a
    lost-update bug, so we trip it immediately.
    """

    def put(self, key, data):
        if "/metadata/v" in key and key.endswith(".json"):
            raise AssertionError(
                f"commit used unconditional put() on {key!r}; "
                "version objects must go through put_if_absent")
        return super().put(key, data)


class _RaceOnceStore(ObjectStore):
    """Deterministically injects ONE commit conflict.

    The first time the writer tries ``put_if_absent`` on a metadata version key,
    we first slip a *rival* valid version into that exact key, so the writer's
    attempt loses and it must take the conflict-retry path — single-threaded, no
    timing. The rival carries a sentinel data file so we can detect clobbering.
    """

    def __init__(self, root):
        super().__init__(root)
        self._raced = False
        self._rival_key = None
        self._rival_bytes = None

    def put_if_absent(self, key, data):
        if (not self._raced and "/metadata/v" in key and key.endswith(".json")
                and not key.endswith("/v1.json")):
            self._raced = True
            # A rival occupies this exact version key first with VALID metadata
            # (we reuse the caller's own bytes, which already point at real data).
            super().put_if_absent(key, data)
            self._rival_key = key
            self._rival_bytes = data
        return super().put_if_absent(key, data)


def test_conflict_forces_retry_without_clobber(submission, tmp_path):
    """A single injected conflict: the commit must detect the lost race, NOT
    overwrite the rival's version object, and land at a higher version with the
    row still present. Kills: plain-overwrite, overwrite-on-conflict, fixed-version."""
    store = _RaceOnceStore(str(tmp_path / "wh"))
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1]))        # this append hits the injected conflict once
    assert store._raced, "test harness did not inject a conflict"
    # the rival's version object must be intact (not clobbered)
    assert store.get(store._rival_key) == store._rival_bytes
    # our row survived (we retried at a higher version, didn't lose the update)
    assert 1 in t.scan().column("id").to_pylist()
    assert _latest_version(store, "t") > int(
        store._rival_key.rsplit("/v", 1)[1][:-5])


def test_commit_publishes_version_only_via_put_if_absent(submission, tmp_path):
    """The version object must be created with the conditional primitive, never an
    unconditional overwrite (which would clobber a concurrent writer)."""
    store = _MetaGuardStore(str(tmp_path / "wh"))
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1]))
    t.append(_rows([2]))
    assert _ids(t.scan()) == [1, 2]
    assert _latest_version(store, "t") >= 3        # v1 create + 2 appends


def test_every_committed_metadata_is_complete_json(submission, tmp_path):
    """All-or-nothing: every version object parses fully (no truncated writes)."""
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    for i in range(5):
        t.append(_rows([i]))
    for k in store.list("t/metadata/"):
        if k.endswith(".json"):
            meta = json.loads(store.get(k).decode())   # raises if truncated/partial
            assert "manifest" in meta and "snapshot_id" in meta


def test_append_still_consistent(submission, tmp_path):
    store = _store(tmp_path)
    t = submission.Table.create(store, "t", SCHEMA)
    t.append(_rows([1, 2]))
    t.append(_rows([3]))
    assert _ids(t.scan()) == [1, 2, 3]
