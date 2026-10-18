#!/usr/bin/env python
"""Generate stage-specific mutants for the mini-table-format BYO challenge.

Each mutant is the FULL cumulative reference (passes stages < N) with ONE targeted
defect that breaks stage N. Written with objectstore.py + WHY.md. Idempotent.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "_src" / "tableformat_full.py"
OBJ = ROOT / "_src" / "objectstore.py"
FULL = SRC.read_text()


def patch(old: str, new: str) -> str:
    if old not in FULL:
        raise SystemExit(f"PATCH TARGET NOT FOUND:\n{old!r}")
    return FULL.replace(old, new, 1)


# mutant spec: stage_dir -> {name: (why, patched_source)}
MUT: dict[str, dict[str, tuple[str, str]]] = {}


# ---- Stage 1 ----------------------------------------------------------------
MUT["stage-01-parquet-manifest"] = {
    "wrong-rowcount": (
        "Manifest records num_rows=0, so the manifest no longer describes the data (metadata/data split broken).",
        patch('"num_rows": arrow_table.num_rows,', '"num_rows": 0,'),
    ),
    "data-not-registered": (
        "Writes the Parquet file but commits an EMPTY manifest, so scan() loses the appended rows.",
        patch("return self._commit(add_files=[entry], add_deletes=[])",
              "return self._commit(add_files=[], add_deletes=[])"),
    ),
    "overwrite-on-append": (
        "append() replaces the manifest instead of extending it, dropping all earlier data files.",
        patch("return self._commit(add_files=[entry], add_deletes=[])",
              "return self._commit(add_files=[], add_deletes=[], replace_manifest=[entry], replace_deletes=[])"),
    ),
}

# ---- Stage 2 ----------------------------------------------------------------
MUT["stage-02-snapshots-time-travel"] = {
    "ignore-pin": (
        "scan(snapshot_id=...) ignores the pin and always reads the current snapshot: no time travel.",
        patch("    def _pick_snapshot(self, snapshot_id: int | None) -> Snapshot:\n"
              "        if snapshot_id is None:\n"
              "            return self.current_snapshot()",
              "    def _pick_snapshot(self, snapshot_id: int | None) -> Snapshot:\n"
              "        if snapshot_id is None or True:\n"
              "            return self.current_snapshot()"),
    ),
    "constant-timestamp": (
        "All snapshots share timestamp_ms=0, so history is not ordered/auditable in time.",
        patch('"timestamp_ms": _now_ms(),\n'
              '                "parent_id": base["snapshot_id"],',
              '"timestamp_ms": 0,\n'
              '                "parent_id": base["snapshot_id"],'),
    ),
    "drop-create-snapshot": (
        "snapshots() hides v1 (the create snapshot), so the history is incomplete.",
        patch("        for n in range(1, v + 1):", "        for n in range(2, v + 1):"),
    ),
}

# ---- Stage 3 ----------------------------------------------------------------
MUT["stage-03-atomic-commit"] = {
    "plain-overwrite": (
        "Commits with unconditional put() instead of put_if_absent(): a concurrent version is silently clobbered (lost update).",
        patch("            if self.store.put_if_absent(_meta_key(self.name, new_v), _dumps(meta)):\n"
              "                return new_snap_id",
              "            self.store.put(_meta_key(self.name, new_v), _dumps(meta))\n"
              "            return new_snap_id"),
    ),
    "overwrite-on-conflict": (
        "On a lost put_if_absent it falls back to an unconditional put(), overwriting the concurrent writer's version (lost update).",
        patch("            if self.store.put_if_absent(_meta_key(self.name, new_v), _dumps(meta)):\n"
              "                return new_snap_id\n"
              "            # lost the race: re-read the now-latest snapshot and retry.",
              "            if self.store.put_if_absent(_meta_key(self.name, new_v), _dumps(meta)):\n"
              "                return new_snap_id\n"
              "            self.store.put(_meta_key(self.name, new_v), _dumps(meta))  # BUG: clobber\n"
              "            return new_snap_id"),
    ),
    "fixed-version-1": (
        "Always writes v2 (base_v hardcoded to 1), so the second append collides with and discards history.",
        patch("            base_v = _latest_version(self.store, self.name)",
              "            base_v = 1"),
    ),
}

# ---- Stage 4 ----------------------------------------------------------------
MUT["stage-04-concurrent-writers"] = {
    "stale-base-retry": (
        "On conflict it retries with the STALE base metadata (re-reads version number but not the manifest), losing the winner's rows.",
        patch("        for _ in range(max_retries):\n"
              "            base_v = _latest_version(self.store, self.name)\n"
              "            base = _read_meta(self.store, self.name, base_v)",
              "        base_v0 = _latest_version(self.store, self.name)\n"
              "        base0 = _read_meta(self.store, self.name, base_v0)\n"
              "        for _ in range(max_retries):\n"
              "            base_v = _latest_version(self.store, self.name)\n"
              "            base = base0  # BUG: stale manifest, never re-read"),
    ),
    "no-retry": (
        "No retry at all: on the first conflict the append is dropped, so concurrent writers lose updates.",
        patch("            if self.store.put_if_absent(_meta_key(self.name, new_v), _dumps(meta)):\n"
              "                return new_snap_id\n"
              "            # lost the race: re-read the now-latest snapshot and retry.\n"
              "        raise RuntimeError(\"commit failed after retries (contention too high)\")",
              "            if self.store.put_if_absent(_meta_key(self.name, new_v), _dumps(meta)):\n"
              "                return new_snap_id\n"
              "            return new_snap_id  # BUG: give up on conflict, pretend success"),
    ),
    "reuse-version-number": (
        "Commits at the current latest version (not latest+1), so every retry collides and the append is lost.",
        patch("            new_v = base_v + 1\n"
              "            new_snap_id = base[\"snapshot_id\"] + 1",
              "            new_v = base_v  # BUG: should be base_v + 1\n"
              "            new_snap_id = base[\"snapshot_id\"] + 1"),
    ),
}

# ---- Stage 5 ----------------------------------------------------------------
MUT["stage-05-delete-merge-on-read"] = {
    "scan-ignores-deletes": (
        "scan() never subtracts deleted positions, so deleted rows still appear.",
        patch("            dels = del_index.get(entry[\"path\"])\n"
              "            if dels:\n"
              "                keep = [i not in dels for i in range(tbl.num_rows)]\n"
              "                tbl = tbl.filter(pa.array(keep))",
              "            dels = None  # BUG: ignore delete files on read"),
    ),
    "copy-on-write-delete": (
        "delete() rewrites the data file in place (copy-on-write) instead of writing a merge-on-read delete file, violating immutability.",
        patch("            dkey = f\"{self.name}/deletes/{uuid.uuid4().hex}.json\"\n"
              "            self.store.put(dkey, _dumps({\"data_file\": entry[\"path\"],\n"
              "                                         \"positions\": positions}))\n"
              "            new_delete_files.append({\"path\": dkey, \"data_file\": entry[\"path\"]})",
              "            keep = [i for i in range(tbl.num_rows) if i not in set(positions)]\n"
              "            self.store.put(entry[\"path\"], _write_parquet(tbl.take(keep)))  # BUG: rewrites data file"),
    ),
    "delete-all-positions": (
        "Delete records ALL positions of a matched file, not just matching rows, deleting too much.",
        patch("            positions = [i for i, d in enumerate(mask) if d and i not in already]\n"
              "            if not positions:\n"
              "                continue",
              "            positions = [i for i in range(tbl.num_rows) if i not in already] if any(mask) else []\n"
              "            if not positions:\n"
              "                continue"),
    ),
}

# ---- Stage 6 ----------------------------------------------------------------
MUT["stage-06-stats-pruning"] = {
    "exclusive-bound": (
        "Pruning uses strict </> instead of <=/>=, so a file whose max==lo (boundary match) is wrongly pruned and rows are lost.",
        patch("    return not (s[\"max\"] < lo or s[\"min\"] > hi)",
              "    return not (s[\"max\"] <= lo or s[\"min\"] >= hi)  # BUG: exclusive bounds"),
    ),
    "never-prune": (
        "Opens every file regardless of stats, so files_opened never shrinks (no pruning benefit).",
        patch("    return not (s[\"max\"] < lo or s[\"min\"] > hi)",
              "    return True  # BUG: never prune"),
    ),
    "always-prune-nonmatch": (
        "Pruning predicate is inverted (overlap test negated), so overlapping files are skipped and matching rows vanish.",
        patch("    return not (s[\"max\"] < lo or s[\"min\"] > hi)",
              "    return (s[\"max\"] < lo or s[\"min\"] > hi)  # BUG: inverted overlap"),
    ),
}

# ---- Stage 7 ----------------------------------------------------------------
MUT["stage-07-compaction-expiration"] = {
    "expire-deletes-referenced": (
        "expire() GCs files referenced only by non-expired snapshots too (uses current snapshot only for 'referenced'), corrupting retained snapshots.",
        patch("        referenced: set[str] = set()\n"
              "        for s in retained:\n"
              "            for e in s.manifest:\n"
              "                referenced.add(e[\"path\"])\n"
              "            for d in s.deletes:\n"
              "                referenced.add(d[\"path\"])",
              "        referenced: set[str] = set()\n"
              "        cur = retained[-1]\n"
              "        for e in cur.manifest:  # BUG: only the latest snapshot's files\n"
              "            referenced.add(e[\"path\"])\n"
              "        for d in cur.deletes:\n"
              "            referenced.add(d[\"path\"])"),
    ),
    "compact-keeps-deletes": (
        "compact() carries the old delete files forward instead of clearing them, so deletes get double-applied by position against the rewritten file.",
        patch("        return self._commit(add_files=[], add_deletes=[],\n"
              "                            replace_manifest=([new_entry] if new_entry else []),\n"
              "                            replace_deletes=[])",
              "        return self._commit(add_files=[], add_deletes=[],\n"
              "                            replace_manifest=([new_entry] if new_entry else []))  # BUG: deletes kept"),
    ),
    "expire-drops-latest": (
        "expire() can drop the current snapshot (no 'always keep latest' guard), destroying the live table.",
        patch("        retained = [s for s in snaps\n"
              "                    if s.timestamp_ms >= older_than_ms or s.version == snaps[-1].version]",
              "        retained = [s for s in snaps\n"
              "                    if s.timestamp_ms >= older_than_ms]  # BUG: may drop latest"),
    ),
}


def main() -> int:
    obj = OBJ.read_text()
    for stage_dir, mutants in MUT.items():
        base = ROOT / stage_dir / "mutants"
        for name, (why, source) in mutants.items():
            d = base / name
            d.mkdir(parents=True, exist_ok=True)
            (d / "tableformat.py").write_text(source)
            (d / "objectstore.py").write_text(obj)
            (d / "WHY.md").write_text(why.rstrip() + "\n")
            print(f"wrote {stage_dir}/mutants/{name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
