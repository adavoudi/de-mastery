"""Mini table format — ALTERNATIVE reference (full, stages 1..7).

Genuinely different from the primary solution:
- metadata modeled with a dataclass + explicit (de)serialization, not raw dicts;
- deletes merged into a single position set per data file at read time;
- scan builds the result from a Python-built keep-mask per row, not pyarrow.compute
  predicate chaining.
Same provided ObjectStore and same public API, so it passes every stage.
"""

from __future__ import annotations

import io
import json
import time
import uuid
from dataclasses import dataclass, field, asdict

import pyarrow as pa
import pyarrow.parquet as pq

from objectstore import ObjectStore


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class Meta:
    snapshot_id: int
    version: int
    timestamp_ms: int
    parent_id: object = None
    manifest: list = field(default_factory=list)
    deletes: list = field(default_factory=list)

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True, default=str).encode()

    @classmethod
    def from_bytes(cls, b: bytes) -> "Meta":
        d = json.loads(b.decode())
        return cls(snapshot_id=d["snapshot_id"], version=d["version"],
                   timestamp_ms=d["timestamp_ms"], parent_id=d.get("parent_id"),
                   manifest=d.get("manifest", []), deletes=d.get("deletes", []))


class Snapshot:
    def __init__(self, m: Meta):
        self.snapshot_id = m.snapshot_id
        self.version = m.version
        self.timestamp_ms = m.timestamp_ms
        self.parent_id = m.parent_id
        self.manifest = m.manifest
        self.deletes = m.deletes


def _mkey(name: str, v: int) -> str:
    return f"{name}/metadata/v{v}.json"


class Table:
    def __init__(self, store: ObjectStore, name: str):
        self.store = store
        self.name = name
        self.files_opened: list[str] = []

    def _latest(self) -> int:
        versions = []
        for key in self.store.list(f"{self.name}/metadata/v"):
            b = key.rsplit("/", 1)[-1]
            if b.startswith("v") and b.endswith(".json"):
                try:
                    versions.append(int(b[1:-5]))
                except ValueError:
                    pass
        return max(versions) if versions else 0

    def _meta(self, v: int) -> Meta:
        return Meta.from_bytes(self.store.get(_mkey(self.name, v)))

    @classmethod
    def create(cls, store, name, schema):
        t = cls(store, name)
        if t._latest() != 0:
            raise FileExistsError(name)
        m = Meta(snapshot_id=0, version=1, timestamp_ms=_now_ms())
        if not store.put_if_absent(_mkey(name, 1), m.to_bytes()):
            raise FileExistsError(name)
        return t

    @classmethod
    def load(cls, store, name):
        t = cls(store, name)
        if t._latest() == 0:
            raise FileNotFoundError(name)
        return t

    def snapshots(self):
        return [Snapshot(self._meta(v)) for v in range(1, self._latest() + 1)
                if self.store.exists(_mkey(self.name, v))]

    def current_snapshot(self):
        return Snapshot(self._meta(self._latest()))

    @staticmethod
    def _stats(tbl: pa.Table) -> dict:
        out = {}
        for name in tbl.schema.names:
            col = tbl.column(name)
            if len(col) == 0:
                continue
            vals = [v for v in col.to_pylist() if v is not None]
            if not vals:
                continue
            try:
                out[name] = {"min": min(vals), "max": max(vals)}
            except TypeError:
                pass
        return out

    def append(self, arrow_table: pa.Table) -> int:
        key = f"{self.name}/data/{uuid.uuid4().hex}.parquet"
        buf = io.BytesIO(); pq.write_table(arrow_table, buf)
        self.store.put(key, buf.getvalue())
        entry = {"path": key, "num_rows": arrow_table.num_rows,
                 "stats": self._stats(arrow_table)}
        return self._commit(add=[entry], add_del=[])

    def _delmap(self, snap) -> dict:
        m = {}
        for d in snap.deletes:
            p = json.loads(self.store.get(d["path"]).decode())
            m.setdefault(p["data_file"], set()).update(p["positions"])
        return m

    def delete(self, predicate) -> int:
        snap = self.current_snapshot()
        existing = self._delmap(snap)
        newdel = []
        for e in snap.manifest:
            tbl = self._open(e["path"])
            mask = predicate(tbl)
            mask = mask.to_pylist() if hasattr(mask, "to_pylist") else list(mask)
            prev = existing.get(e["path"], set())
            pos = [i for i, m in enumerate(mask) if m and i not in prev]
            if not pos:
                continue
            dk = f"{self.name}/deletes/{uuid.uuid4().hex}.json"
            self.store.put(dk, json.dumps({"data_file": e["path"],
                                           "positions": pos}).encode())
            newdel.append({"path": dk, "data_file": e["path"]})
        if not newdel:
            return snap.snapshot_id
        return self._commit(add=[], add_del=newdel)

    def compact(self) -> int:
        snap = self.current_snapshot()
        live = self._read(snap)
        new = []
        if live.num_rows:
            key = f"{self.name}/data/{uuid.uuid4().hex}.parquet"
            buf = io.BytesIO(); pq.write_table(live, buf)
            self.store.put(key, buf.getvalue())
            new = [{"path": key, "num_rows": live.num_rows, "stats": self._stats(live)}]
        return self._commit(add=[], add_del=[], replace=new, replace_del=[])

    def expire(self, older_than_ms: int) -> int:
        snaps = self.snapshots()
        keep = [s for s in snaps
                if s.timestamp_ms >= older_than_ms or s.version == snaps[-1].version]
        keepv = {s.version for s in keep}
        ref = set()
        for s in keep:
            ref.update(e["path"] for e in s.manifest)
            ref.update(d["path"] for d in s.deletes)
        for s in snaps:
            if s.version not in keepv:
                self.store.delete(_mkey(self.name, s.version))
        removed = 0
        for key in self.store.list(f"{self.name}/data/") + self.store.list(f"{self.name}/deletes/"):
            if key not in ref:
                self.store.delete(key); removed += 1
        return removed

    def _open(self, key: str) -> pa.Table:
        self.files_opened.append(key)
        return pq.read_table(self.store.path(key))

    def scan(self, filter=None, snapshot_id=None) -> pa.Table:
        if snapshot_id is None:
            snap = self.current_snapshot()
        else:
            snap = next((s for s in self.snapshots() if s.snapshot_id == snapshot_id), None)
            if snap is None:
                raise KeyError(snapshot_id)
        return self._read(snap, filter)

    def _read(self, snap, filter=None) -> pa.Table:
        dm = self._delmap(snap)
        batches = []
        schema = None
        for e in snap.manifest:
            if filter is not None:
                col, lo, hi = filter
                st = e.get("stats", {}).get(col)
                if st is not None and (st["max"] < lo or st["min"] > hi):
                    continue
            tbl = self._open(e["path"])
            schema = tbl.schema
            dels = dm.get(e["path"], set())
            keep_mask = []
            for i in range(tbl.num_rows):
                if i in dels:
                    keep_mask.append(False); continue
                if filter is not None:
                    col, lo, hi = filter
                    v = tbl.column(col)[i].as_py()
                    keep_mask.append(v is not None and lo <= v <= hi)
                else:
                    keep_mask.append(True)
            batches.append(tbl.filter(pa.array(keep_mask)))
        if not batches:
            return schema.empty_table() if schema is not None else pa.table({})
        return pa.concat_tables(batches)

    def _commit(self, *, add, add_del, replace=None, replace_del=None,
                max_retries=50) -> int:
        for _ in range(max_retries):
            bv = self._latest()
            base = self._meta(bv)
            manifest = list(replace) if replace is not None else base.manifest + add
            deletes = list(replace_del) if replace_del is not None else base.deletes + add_del
            m = Meta(snapshot_id=base.snapshot_id + 1, version=bv + 1,
                     timestamp_ms=_now_ms(), parent_id=base.snapshot_id,
                     manifest=manifest, deletes=deletes)
            if self.store.put_if_absent(_mkey(self.name, bv + 1), m.to_bytes()):
                return m.snapshot_id
        raise RuntimeError("commit failed after retries")
