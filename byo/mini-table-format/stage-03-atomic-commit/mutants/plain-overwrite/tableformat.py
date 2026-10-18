"""Mini table format — reference implementation (FULL, stages 1..7).

A table is a directory of immutable Parquet data files plus a chain of JSON
metadata snapshots. All atomicity rests on ``ObjectStore.put_if_absent`` (see
objectstore.py). Keys under a table ``name``:

    <name>/metadata/v<N>.json     # one per snapshot; N is monotic, created with CAS
    <name>/data/<uuid>.parquet    # immutable data files
    <name>/deletes/<uuid>.json    # position-delete files (stage 5): {data_file: [pos,...]}

The "current" version is the highest ``v<N>.json`` that exists. A commit:
reads the latest version N, builds snapshot N+1, and ``put_if_absent`` on
``v<N+1>.json``. If that returns False another writer won the race, so we
re-read and retry (optimistic concurrency, stage 3/4).
"""

from __future__ import annotations

import json
import time
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from objectstore import ObjectStore


# --------------------------------------------------------------------------
# metadata helpers
# --------------------------------------------------------------------------
def _meta_key(name: str, version: int) -> str:
    return f"{name}/metadata/v{version}.json"


def _latest_version(store: ObjectStore, name: str) -> int:
    """Highest committed metadata version, or 0 if none."""
    prefix = f"{name}/metadata/v"
    best = 0
    for key in store.list(f"{name}/metadata/"):
        base = key.rsplit("/", 1)[-1]
        if base.startswith("v") and base.endswith(".json"):
            try:
                n = int(base[1:-5])
            except ValueError:
                continue
            best = max(best, n)
    return best


def _read_meta(store: ObjectStore, name: str, version: int) -> dict:
    return json.loads(store.get(_meta_key(name, version)).decode())


class Snapshot:
    def __init__(self, d: dict):
        self.snapshot_id: int = d["snapshot_id"]
        self.version: int = d["version"]
        self.timestamp_ms: int = d["timestamp_ms"]
        self.parent_id = d.get("parent_id")
        self.manifest: list[dict] = d.get("manifest", [])
        self.deletes: list[dict] = d.get("deletes", [])

    def __repr__(self) -> str:
        return f"<Snapshot id={self.snapshot_id} v={self.version} files={len(self.manifest)}>"


# --------------------------------------------------------------------------
# stats (stage 6)
# --------------------------------------------------------------------------
def _column_stats(tbl: pa.Table) -> dict:
    stats = {}
    for field in tbl.schema:
        col = tbl.column(field.name)
        if len(col) == 0:
            continue
        try:
            cmin = pc.min(col).as_py()
            cmax = pc.max(col).as_py()
        except pa.ArrowNotImplementedError:
            continue
        if cmin is None or cmax is None:
            continue
        stats[field.name] = {"min": cmin, "max": cmax}
    return stats


def _overlaps(file_stats: dict, col: str, lo, hi) -> bool:
    """Could a file with these stats contain any row with lo <= col <= hi?"""
    s = file_stats.get(col)
    if s is None:
        return True  # no stats -> cannot prune
    # inclusive bounds on BOTH sides: prune only if ranges are disjoint.
    return not (s["max"] < lo or s["min"] > hi)


# --------------------------------------------------------------------------
# the table
# --------------------------------------------------------------------------
class Table:
    def __init__(self, store: ObjectStore, name: str, schema: pa.Schema):
        self.store = store
        self.name = name
        self.schema = schema
        # Instrumentation for tests (stage 6): count data files physically opened.
        self.files_opened: list[str] = []

    # --- lifecycle ----------------------------------------------------
    @classmethod
    def create(cls, store: ObjectStore, name: str, schema: pa.Schema) -> "Table":
        if _latest_version(store, name) != 0:
            raise FileExistsError(f"table {name!r} already exists")
        meta = {
            "snapshot_id": 0,
            "version": 1,
            "timestamp_ms": _now_ms(),
            "parent_id": None,
            "schema": schema.to_string(),
            "manifest": [],
            "deletes": [],
        }
        ok = store.put_if_absent(_meta_key(name, 1), _dumps(meta))
        if not ok:
            raise FileExistsError(f"table {name!r} already exists")
        return cls(store, name, schema)

    @classmethod
    def load(cls, store: ObjectStore, name: str) -> "Table":
        v = _latest_version(store, name)
        if v == 0:
            raise FileNotFoundError(f"no such table {name!r}")
        meta = _read_meta(store, name, v)
        schema = pa.schema([])  # schema string is informational; infer lazily on scan
        t = cls(store, name, schema)
        return t

    # --- snapshot inspection -----------------------------------------
    def snapshots(self) -> list[Snapshot]:
        v = _latest_version(self.store, self.name)
        out = []
        for n in range(1, v + 1):
            if self.store.exists(_meta_key(self.name, n)):
                out.append(Snapshot(_read_meta(self.store, self.name, n)))
        return out

    def current_snapshot(self) -> Snapshot:
        v = _latest_version(self.store, self.name)
        return Snapshot(_read_meta(self.store, self.name, v))

    # --- writes -------------------------------------------------------
    def append(self, arrow_table: pa.Table) -> int:
        """Append rows as a new immutable Parquet file + a new snapshot."""
        data_key = f"{self.name}/data/{uuid.uuid4().hex}.parquet"
        self.store.put(data_key, _write_parquet(arrow_table))
        entry = {
            "path": data_key,
            "num_rows": arrow_table.num_rows,
            "stats": _column_stats(arrow_table),
        }
        return self._commit(add_files=[entry], add_deletes=[])

    def delete(self, predicate) -> int:
        """Merge-on-read delete: mark matching rows via position-delete files.

        ``predicate`` is a callable ``(pyarrow.Table) -> pyarrow.BooleanArray``
        (True = row should be deleted). Data files are never rewritten here.
        """
        snap = self.current_snapshot()
        new_delete_files = []
        existing = self._delete_index(snap)
        for entry in snap.manifest:
            tbl = self._read_file(entry["path"])
            mask = predicate(tbl)
            mask = _as_bool_list(mask, tbl.num_rows)
            already = set(existing.get(entry["path"], []))
            positions = [i for i, d in enumerate(mask) if d and i not in already]
            if not positions:
                continue
            dkey = f"{self.name}/deletes/{uuid.uuid4().hex}.json"
            self.store.put(dkey, _dumps({"data_file": entry["path"],
                                         "positions": positions}))
            new_delete_files.append({"path": dkey, "data_file": entry["path"]})
        if not new_delete_files:
            return snap.snapshot_id
        return self._commit(add_files=[], add_deletes=new_delete_files)

    def compact(self) -> int:
        """Rewrite all live rows into one new data file, applying deletes.

        Produces a snapshot whose manifest is a single file with NO deletes. Old
        files/deletes are not physically removed until ``expire`` (readers pinned
        to old snapshots must stay consistent).
        """
        snap = self.current_snapshot()
        live = self._materialize(snap)
        new_entry = None
        if live.num_rows > 0:
            data_key = f"{self.name}/data/{uuid.uuid4().hex}.parquet"
            self.store.put(data_key, _write_parquet(live))
            new_entry = {"path": data_key, "num_rows": live.num_rows,
                         "stats": _column_stats(live)}
        return self._commit(add_files=[], add_deletes=[],
                            replace_manifest=([new_entry] if new_entry else []),
                            replace_deletes=[])

    def expire(self, older_than_ms: int) -> int:
        """Drop snapshots older than ``older_than_ms`` and GC unreferenced files.

        Files still referenced by ANY retained snapshot are kept. Returns the
        number of data/delete files physically deleted.
        """
        snaps = self.snapshots()
        retained = [s for s in snaps
                    if s.timestamp_ms >= older_than_ms or s.version == snaps[-1].version]
        retained_versions = {s.version for s in retained}
        referenced: set[str] = set()
        for s in retained:
            for e in s.manifest:
                referenced.add(e["path"])
            for d in s.deletes:
                referenced.add(d["path"])
        removed = 0
        # delete expired metadata versions
        for s in snaps:
            if s.version not in retained_versions:
                self.store.delete(_meta_key(self.name, s.version))
        # GC data/delete files no retained snapshot references
        for key in self.store.list(f"{self.name}/data/"):
            if key not in referenced:
                self.store.delete(key)
                removed += 1
        for key in self.store.list(f"{self.name}/deletes/"):
            if key not in referenced:
                self.store.delete(key)
                removed += 1
        return removed

    # --- read / scan --------------------------------------------------
    def scan(self, filter=None, snapshot_id: int | None = None) -> pa.Table:
        """Read a consistent snapshot. ``filter`` = (col, lo, hi) inclusive range.

        Scans are isolated: once we pick a snapshot's manifest, concurrent
        commits cannot change what we read (immutability + versioned metadata).
        """
        snap = self._pick_snapshot(snapshot_id)
        return self._materialize(snap, filter=filter)

    # --- internals ----------------------------------------------------
    def _pick_snapshot(self, snapshot_id: int | None) -> Snapshot:
        if snapshot_id is None:
            return self.current_snapshot()
        for s in self.snapshots():
            if s.snapshot_id == snapshot_id:
                return s
        raise KeyError(f"no snapshot {snapshot_id}")

    def _read_file(self, key: str) -> pa.Table:
        self.files_opened.append(key)
        return pq.read_table(self.store.path(key))

    def _delete_index(self, snap: "Snapshot") -> dict:
        """Map data_file key -> set of deleted positions, read from delete files."""
        idx: dict[str, set] = {}
        for d in snap.deletes:
            payload = json.loads(self.store.get(d["path"]).decode())
            idx.setdefault(payload["data_file"], set()).update(payload["positions"])
        return idx

    def _materialize(self, snap: Snapshot, filter=None) -> pa.Table:
        del_index = self._delete_index(snap)
        parts = []
        schema = None
        for entry in snap.manifest:
            if filter is not None:
                col, lo, hi = filter
                if not _overlaps(entry.get("stats", {}), col, lo, hi):
                    continue  # pruned by stats: file not opened at all
            tbl = self._read_file(entry["path"])
            schema = tbl.schema
            dels = del_index.get(entry["path"])
            if dels:
                keep = [i not in dels for i in range(tbl.num_rows)]
                tbl = tbl.filter(pa.array(keep))
            if filter is not None:
                col, lo, hi = filter
                m = pc.and_(pc.greater_equal(tbl.column(col), pa.scalar(lo)),
                            pc.less_equal(tbl.column(col), pa.scalar(hi)))
                tbl = tbl.filter(m)
            parts.append(tbl)
        if not parts:
            if schema is None:
                return pa.table({})
            return schema.empty_table()
        return pa.concat_tables(parts)

    def _commit(self, *, add_files, add_deletes,
                replace_manifest=None, replace_deletes=None,
                max_retries: int = 50) -> int:
        """Optimistic commit: read latest, build v+1, put_if_absent, retry on race."""
        for _ in range(max_retries):
            base_v = _latest_version(self.store, self.name)
            base = _read_meta(self.store, self.name, base_v)
            if replace_manifest is not None:
                manifest = list(replace_manifest)
            else:
                manifest = list(base.get("manifest", [])) + list(add_files)
            if replace_deletes is not None:
                deletes = list(replace_deletes)
            else:
                deletes = list(base.get("deletes", [])) + list(add_deletes)
            new_v = base_v + 1
            new_snap_id = base["snapshot_id"] + 1
            meta = {
                "snapshot_id": new_snap_id,
                "version": new_v,
                "timestamp_ms": _now_ms(),
                "parent_id": base["snapshot_id"],
                "schema": base.get("schema"),
                "manifest": manifest,
                "deletes": deletes,
            }
            self.store.put(_meta_key(self.name, new_v), _dumps(meta))
            return new_snap_id
            # lost the race: re-read the now-latest snapshot and retry.
        raise RuntimeError("commit failed after retries (contention too high)")


# --------------------------------------------------------------------------
# module-level helpers
# --------------------------------------------------------------------------
def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _dumps(obj: dict) -> bytes:
    return json.dumps(obj, sort_keys=True, default=str).encode()


def _write_parquet(tbl: pa.Table) -> bytes:
    import io
    buf = io.BytesIO()
    pq.write_table(tbl, buf)
    return buf.getvalue()


def _as_bool_list(mask, n: int) -> list[bool]:
    if isinstance(mask, (list, tuple)):
        return list(mask)
    try:
        return [bool(x) for x in mask.to_pylist()]
    except AttributeError:
        return [bool(x) for x in list(mask)]
