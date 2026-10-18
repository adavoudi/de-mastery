"""Mini table format — cumulative learner codebase (STARTER).

You implement each method as you reach its stage. The provided ``objectstore.py``
gives you ONE conditional primitive (``put_if_absent``); build everything else on
it. Do NOT edit ``objectstore.py``.

Public API (see each stage README):
    Table.create(store, name, schema) -> Table
    Table.load(store, name) -> Table
    table.append(arrow_table) -> snapshot_id
    table.scan(filter=None, snapshot_id=None) -> pyarrow.Table
    table.snapshots() -> list[Snapshot]
    table.delete(predicate) -> snapshot_id        # stage 5
    table.compact() -> snapshot_id                # stage 7
    table.expire(older_than_ms) -> int            # stage 7

``filter`` is an inclusive range ``(col, lo, hi)``. Keep ``self.files_opened`` a
list of the data-file keys you physically open (stage 6 counts it).
"""

from __future__ import annotations

import pyarrow as pa  # noqa: F401  (you will build pyarrow Tables)

from objectstore import ObjectStore  # noqa: F401  (provided; do not edit)


class Snapshot:
    """A point-in-time view: snapshot_id, version, timestamp_ms, manifest, deletes."""

    def __init__(self, d: dict):
        raise NotImplementedError("stage 2")


class Table:
    def __init__(self, store, name, schema=None):
        self.store = store
        self.name = name
        self.schema = schema
        self.files_opened: list[str] = []

    @classmethod
    def create(cls, store, name, schema):
        raise NotImplementedError("stage 1")

    @classmethod
    def load(cls, store, name):
        raise NotImplementedError("stage 1")

    def append(self, arrow_table):
        raise NotImplementedError("stage 1")

    def scan(self, filter=None, snapshot_id=None):
        raise NotImplementedError("stage 1")

    def snapshots(self):
        raise NotImplementedError("stage 2")

    def current_snapshot(self):
        raise NotImplementedError("stage 2")

    def delete(self, predicate):
        raise NotImplementedError("stage 5")

    def compact(self):
        raise NotImplementedError("stage 7")

    def expire(self, older_than_ms):
        raise NotImplementedError("stage 7")
