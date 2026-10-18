"""mini-query-engine — reference engine (cumulative through stage 8).

PUBLIC API (stable across all stages; see stage-01 README)
----------------------------------------------------------
Plan nodes are frozen dataclasses. You build a tree of them and call:

    execute(plan) -> pyarrow.Table      # run the plan, return the result
    explain(plan) -> str                # a Spark-like indented plan string

Expressions used by Filter are small dataclasses:

    Col(name)                           # a column reference
    Lit(value)                          # a literal
    Compare(left, op, right)            # op in {"=","!=","<","<=",">",">="}
    And(*preds) / Or(*preds)            # boolean combinators

Plan nodes:

    Scan(path, columns=None)            # read a Parquet file (row-group batches)
    Project(child, columns)             # keep/reorder columns
    Filter(child, predicate)            # row filter (Volcano iterator model)
    Aggregate(child, keys, aggs)        # hash aggregate; aggs=[(func,col,out)]
    HashJoin(left, right, left_keys, right_keys, how="inner")
    BroadcastJoin(...)                  # same semantics, build side broadcast
    Exchange(child, keys, num_partitions)   # hash-partition exchange (stage 6)
    Optimized(child)                    # marker produced by optimize()

Everything is pyarrow-native. Scanning reads row groups as RecordBatches and
tracks statistics so stage 3 can skip row groups (PushedFilters).
"""

from __future__ import annotations

import hashlib
import heapq
import os
import pickle
import tempfile
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

_OPS: dict[str, Callable[[Any, Any], Any]] = {
    "=": pc.equal,
    "==": pc.equal,
    "!=": pc.not_equal,
    "<": pc.less,
    "<=": pc.less_equal,
    ">": pc.greater,
    ">=": pc.greater_equal,
}


@dataclass(frozen=True)
class Col:
    name: str


@dataclass(frozen=True)
class Lit:
    value: Any


@dataclass(frozen=True)
class Compare:
    left: Any          # Col or Lit
    op: str
    right: Any         # Col or Lit


@dataclass(frozen=True)
class And:
    preds: tuple = ()

    def __init__(self, *preds):
        object.__setattr__(self, "preds", tuple(preds))


@dataclass(frozen=True)
class Or:
    preds: tuple = ()

    def __init__(self, *preds):
        object.__setattr__(self, "preds", tuple(preds))


def columns_in(expr) -> set[str]:
    """Return the set of column names referenced by a predicate expression."""
    if isinstance(expr, Col):
        return {expr.name}
    if isinstance(expr, Lit):
        return set()
    if isinstance(expr, Compare):
        return columns_in(expr.left) | columns_in(expr.right)
    if isinstance(expr, (And, Or)):
        out: set[str] = set()
        for p in expr.preds:
            out |= columns_in(p)
        return out
    return set()


def _eval_operand(operand, batch: pa.RecordBatch):
    if isinstance(operand, Col):
        return batch.column(batch.schema.get_field_index(operand.name))
    if isinstance(operand, Lit):
        return operand.value
    raise TypeError(f"operand must be Col or Lit, got {operand!r}")


def eval_predicate(expr, batch: pa.RecordBatch) -> pa.Array:
    """Evaluate a boolean predicate against a RecordBatch -> boolean mask."""
    if isinstance(expr, Compare):
        left = _eval_operand(expr.left, batch)
        right = _eval_operand(expr.right, batch)
        return _OPS[expr.op](left, right)
    if isinstance(expr, And):
        mask = None
        for p in expr.preds:
            m = eval_predicate(p, batch)
            mask = m if mask is None else pc.and_kleene(mask, m)
        return mask if mask is not None else pc.scalar(True)
    if isinstance(expr, Or):
        mask = None
        for p in expr.preds:
            m = eval_predicate(p, batch)
            mask = m if mask is None else pc.or_kleene(mask, m)
        return mask if mask is not None else pc.scalar(False)
    raise TypeError(f"not a predicate: {expr!r}")


# ---------------------------------------------------------------------------
# Plan nodes
# ---------------------------------------------------------------------------


class ScanStats:
    """Row-group skip counters, exposed through explain() (like PushedFilters)."""

    def __init__(self):
        self.row_groups_total = 0
        self.row_groups_read = 0
        self.row_groups_skipped = 0


@dataclass(frozen=True)
class Scan:
    path: str
    columns: tuple[str, ...] | None = None
    pushdown: Any = None          # a predicate pushed into the scan (stage 3)
    # mutable counters live on a side object so the node stays frozen/hashable
    stats: ScanStats = field(default=None, compare=False, hash=False)


@dataclass(frozen=True)
class Project:
    child: Any
    columns: tuple[str, ...]


@dataclass(frozen=True)
class Filter:
    child: Any
    predicate: Any


@dataclass(frozen=True)
class Aggregate:
    child: Any
    keys: tuple[str, ...]
    # aggs: list of (func, input_col, output_name); func in {sum,count,min,max,avg}
    aggs: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True)
class HashJoin:
    left: Any
    right: Any
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    how: str = "inner"


@dataclass(frozen=True)
class BroadcastJoin:
    left: Any
    right: Any
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    how: str = "inner"


@dataclass(frozen=True)
class Exchange:
    child: Any
    keys: tuple[str, ...]
    num_partitions: int = 4


@dataclass(frozen=True)
class Optimized:
    child: Any


# ---------------------------------------------------------------------------
# Stage 1: Scan Parquet in row-group batches
# ---------------------------------------------------------------------------


def _scan_batches(node: Scan) -> Iterator[pa.RecordBatch]:
    """Yield RecordBatches one Parquet row group at a time, applying predicate
    pushdown via row-group statistics (stage 3) and column projection."""
    pf = pq.ParquetFile(node.path)
    stats = node.stats
    if stats is not None:
        stats.row_groups_total = pf.num_row_groups
    cols = list(node.columns) if node.columns else None
    for rg in range(pf.num_row_groups):
        if node.pushdown is not None and _row_group_can_skip(pf, rg, node.pushdown):
            if stats is not None:
                stats.row_groups_skipped += 1
            continue
        if stats is not None:
            stats.row_groups_read += 1
        table = pf.read_row_group(rg, columns=cols)
        for batch in table.to_batches():
            yield batch


def _row_group_can_skip(pf: pq.ParquetFile, rg: int, predicate) -> bool:
    """Return True iff NO row in row group ``rg`` can satisfy ``predicate``,
    using column min/max statistics. Conservative: unknown -> do not skip."""
    md = pf.metadata.row_group(rg)
    name_to_idx = {pf.schema_arrow.field(i).name: i
                   for i in range(len(pf.schema_arrow))}
    return _pred_excludes_rowgroup(predicate, md, name_to_idx)


def _col_min_max(md, idx):
    col = md.column(idx)
    st = col.statistics
    if st is None or not st.has_min_max:
        return None
    return st.min, st.max


def _pred_excludes_rowgroup(expr, md, name_to_idx) -> bool:
    """True if the predicate proves the row group has no matching row."""
    if isinstance(expr, Compare):
        if not isinstance(expr.left, Col) or not isinstance(expr.right, Lit):
            return False
        name = expr.left.name
        if name not in name_to_idx:
            return False
        mm = _col_min_max(md, name_to_idx[name])
        if mm is None:
            return False
        cmin, cmax = mm
        v = expr.right.value
        op = expr.op
        # If every value in [cmin, cmax] fails the comparison -> skip.
        if op in ("=", "=="):
            return v < cmin or v > cmax
        if op == ">":
            return cmax <= v
        if op == ">=":
            return cmax < v
        if op == "<":
            return cmin >= v
        if op == "<=":
            return cmin > v
        return False
    if isinstance(expr, And):
        # Skip if ANY conjunct proves exclusion.
        return any(_pred_excludes_rowgroup(p, md, name_to_idx) for p in expr.preds)
    if isinstance(expr, Or):
        # Skip only if EVERY disjunct proves exclusion.
        return bool(expr.preds) and all(
            _pred_excludes_rowgroup(p, md, name_to_idx) for p in expr.preds)
    return False


# ---------------------------------------------------------------------------
# Volcano iterator model: each operator yields RecordBatches
# ---------------------------------------------------------------------------


def _batches(node) -> Iterator[pa.RecordBatch]:
    if isinstance(node, Scan):
        yield from _scan_batches(node)
    elif isinstance(node, Project):
        for b in _batches(node.child):
            idxs = [b.schema.get_field_index(c) for c in node.columns]
            yield b.select(idxs)
    elif isinstance(node, Filter):
        for b in _batches(node.child):
            mask = eval_predicate(node.predicate, b)
            yield b.filter(mask)
    elif isinstance(node, Optimized):
        yield from _batches(node.child)
    elif isinstance(node, Exchange):
        # Exchange is a logical repartition; for local (non-shuffled) execution it
        # is a pass-through. Stage 6 runs it across processes via execute_parallel.
        yield from _batches(node.child)
    else:
        # Aggregate / joins are materializing operators handled in execute().
        tbl = _materialize(node)
        for b in tbl.to_batches():
            yield b


def _collect(node) -> pa.Table:
    batches = [b for b in _batches(node) if b.num_rows >= 0]
    batches = [b for b in batches]
    if not batches:
        # Build an empty table with the right schema if we can infer it.
        schema = _infer_schema(node)
        return pa.table({f.name: pa.array([], type=f.type) for f in schema})
    return pa.Table.from_batches(batches)


def _infer_schema(node) -> pa.Schema:
    if isinstance(node, Scan):
        sch = pq.ParquetFile(node.path).schema_arrow
        if node.columns:
            sch = pa.schema([sch.field(c) for c in node.columns])
        return sch
    if isinstance(node, (Filter, Optimized, Exchange)):
        return _infer_schema(node.child)
    if isinstance(node, Project):
        sch = _infer_schema(node.child)
        return pa.schema([sch.field(c) for c in node.columns])
    return pa.schema([])


# ---------------------------------------------------------------------------
# Stage 4: hash aggregate (partial + final)
# ---------------------------------------------------------------------------

# Partial aggregation state per group: dict[key_tuple] -> dict[out] -> acc
# Each agg func is described by (init, combine_value, merge_partials, finalize).


def _agg_spec(func: str):
    if func == "sum":
        return (0, lambda a, v: a + v, lambda a, b: a + b, lambda a, n: a)
    if func == "count":
        return (0, lambda a, v: a + 1, lambda a, b: a + b, lambda a, n: a)
    if func == "min":
        return (None, lambda a, v: v if a is None else min(a, v),
                lambda a, b: b if a is None else (a if b is None else min(a, b)),
                lambda a, n: a)
    if func == "max":
        return (None, lambda a, v: v if a is None else max(a, v),
                lambda a, b: b if a is None else (a if b is None else max(a, b)),
                lambda a, n: a)
    if func == "avg":
        # BUG: carries a running mean and merges by AVERAGING the two means,
        # which is wrong when chunk sizes differ (average of averages).
        return ((0.0, 0),
                lambda a, v: ((a[0] * a[1] + v) / (a[1] + 1), a[1] + 1),
                lambda a, b: ((a[0] + b[0]) / 2 if b[1] else a[0],
                              a[1] + b[1]),
                lambda a, n: a[0] if a[1] else None)
    raise ValueError(f"unknown agg func {func!r}")


def partial_aggregate(table: pa.Table, keys, aggs) -> dict:
    """Compute PARTIAL aggregation state over one chunk of data."""
    specs = {out: _agg_spec(func) for func, _col, out in aggs}
    state: dict[tuple, dict[str, Any]] = {}
    key_cols = [table.column(k).to_pylist() for k in keys]
    val_cols = {col: table.column(col).to_pylist()
                for _f, col, _o in aggs if col is not None}
    n = table.num_rows
    for i in range(n):
        kt = tuple(key_cols[j][i] for j in range(len(keys)))
        grp = state.get(kt)
        if grp is None:
            grp = {out: specs[out][0] for _f, _c, out in aggs}
            state[kt] = grp
        for func, col, out in aggs:
            init, combine, _merge, _fin = specs[out]
            if func == "count" and col is None:
                grp[out] = combine(grp[out], None)
            else:
                v = val_cols[col][i]
                if v is None and func != "count":
                    continue
                grp[out] = combine(grp[out], v)
    return state


def merge_partials(states: Iterable[dict], aggs) -> dict:
    """Merge PARTIAL states into a final state (final aggregation)."""
    specs = {out: _agg_spec(func) for func, _c, out in aggs}
    merged: dict[tuple, dict[str, Any]] = {}
    for st in states:
        for kt, grp in st.items():
            tgt = merged.get(kt)
            if tgt is None:
                tgt = {out: specs[out][0] for _f, _c, out in aggs}
                merged[kt] = tgt
            for _f, _c, out in aggs:
                tgt[out] = specs[out][2](tgt[out], grp[out])
    return merged


def _finalize_agg(state: dict, keys, aggs) -> pa.Table:
    specs = {out: _agg_spec(func) for func, _c, out in aggs}
    out_cols: dict[str, list] = {k: [] for k in keys}
    for _f, _c, out in aggs:
        out_cols[out] = []
    for kt, grp in state.items():
        for j, k in enumerate(keys):
            out_cols[k].append(kt[j])
        for _f, _c, out in aggs:
            out_cols[out].append(specs[out][3](grp[out], None))
    return pa.table(out_cols)


def _execute_aggregate(node: Aggregate) -> pa.Table:
    child = _collect(node.child)
    partial = partial_aggregate(child, node.keys, node.aggs)
    final = merge_partials([partial], node.aggs)
    return _finalize_agg(final, node.keys, node.aggs)


# ---------------------------------------------------------------------------
# Stage 5: hash join + broadcast join
# ---------------------------------------------------------------------------


def _build_hash_table(table: pa.Table, keys) -> dict[tuple, list[dict]]:
    """Build side: multimap key_tuple -> list of rows (keeps duplicate keys)."""
    ht: dict[tuple, list[dict]] = {}
    rows = table.to_pylist()
    kidx = list(keys)
    for row in rows:
        kt = tuple(row[k] for k in kidx)
        if None in kt:
            continue  # SQL join semantics: NULL keys never match
        ht.setdefault(kt, []).append(row)
    return ht


def _hash_join(left: pa.Table, right: pa.Table, left_keys, right_keys,
               how: str) -> pa.Table:
    """Inner/left hash join. Build on the RIGHT; probe with the LEFT."""
    ht = _build_hash_table(right, right_keys)
    rcols = [c for c in right.column_names if c not in set(right_keys)]
    out_rows: list[dict] = []
    for lrow in left.to_pylist():
        kt = tuple(lrow[k] for k in left_keys)
        matches = ht.get(kt, []) if None not in kt else []
        if matches:
            for rrow in matches:
                merged = dict(lrow)
                for c in rcols:
                    merged[c] = rrow[c]
                out_rows.append(merged)
        elif how == "left":
            merged = dict(lrow)
            for c in rcols:
                merged[c] = None
            out_rows.append(merged)
    schema = _join_schema(left, right, right_keys)
    if not out_rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in schema})
    return pa.Table.from_pylist(out_rows, schema=schema)


def _join_schema(left: pa.Table, right: pa.Table, right_keys) -> pa.Schema:
    fields = list(left.schema)
    rk = set(right_keys)
    for f in right.schema:
        if f.name not in rk:
            fields.append(f)
    return pa.schema(fields)


def _execute_join(node) -> pa.Table:
    left = _collect(node.left)
    right = _collect(node.right)
    # Broadcast join == hash join where the (small) build side is broadcast.
    # Result semantics are identical; only distribution differs.
    return _hash_join(left, right, list(node.left_keys), list(node.right_keys),
                      node.how)


# ---------------------------------------------------------------------------
# Stage 6: partitioned execution with a hash shuffle (multiprocessing)
# ---------------------------------------------------------------------------


def stable_hash(value) -> int:
    """Deterministic hash across processes (Python's hash() is salted per run)."""
    h = hashlib.blake2b(repr(value).encode("utf-8"), digest_size=8)
    return int.from_bytes(h.digest(), "big")


def hash_partition(table: pa.Table, keys, num_partitions: int) -> list[pa.Table]:
    """Split a table into ``num_partitions`` by a stable hash of the key tuple."""
    buckets: list[list[dict]] = [[] for _ in range(num_partitions)]
    for row in table.to_pylist():
        kt = tuple(row[k] for k in keys)
        buckets[stable_hash(kt) % num_partitions].append(row)
    out = []
    for b in buckets:
        if b:
            out.append(pa.Table.from_pylist(b, schema=table.schema))
        else:
            out.append(table.schema.empty_table())
    return out


def _write_ipc(table: pa.Table, path: str) -> None:
    with pa.OSFile(path, "wb") as sink:
        with pa.ipc.new_stream(sink, table.schema) as w:
            w.write_table(table)


def _read_ipc(path: str) -> pa.Table:
    with pa.memory_map(path, "r") as src:
        return pa.ipc.open_stream(src).read_all()


def _shuffle_reduce_worker(part_path: str, out_path: str, keys, aggs) -> None:
    """Reduce task (runs in a child process): read one shuffle partition file,
    compute its PARTIAL aggregate, and write the state back as a file."""
    table = _read_ipc(part_path)
    state = partial_aggregate(table, keys, aggs)
    with open(out_path, "wb") as fh:
        pickle.dump(state, fh)


def execute_parallel(plan: Aggregate, num_partitions: int = 4,
                     processes: int = 2) -> pa.Table:
    """Run a hash-aggregate as a shuffled, partitioned job across processes.

    This mirrors a Spark shuffle: the "map" side hash-partitions rows by the
    group keys and writes one shuffle file per partition; the "reduce" side runs
    in separate OS processes, each reading its partition file and computing a
    PARTIAL aggregate written back to disk. The parent merges the partials.
    Because each group's key hashes to exactly one partition, every group is
    confined to a single reduce task, so merging the partials is the final
    answer. Uses the ``fork`` start method so children share the already-loaded
    engine module (robust and fast under pytest; no re-import, no pickling of
    dynamically-imported callables)."""
    import multiprocessing as mp

    child = _collect(plan.child)
    parts = hash_partition(child, list(plan.keys), num_partitions)
    keys = list(plan.keys)
    aggs = plan.aggs

    tmpdir = tempfile.mkdtemp(prefix="mqe_shuffle_")
    try:
        # Map side: write non-empty shuffle partition files.
        jobs = []
        for i, part in enumerate(parts):
            if part.num_rows == 0:
                continue
            pin = os.path.join(tmpdir, f"part_{i}.arrow")
            pout = os.path.join(tmpdir, f"state_{i}.pkl")
            _write_ipc(part, pin)
            jobs.append((pin, pout))
        if not jobs:
            return _finalize_agg({}, plan.keys, plan.aggs)

        # Reduce side: run workers in child processes, `processes` at a time.
        ctx = mp.get_context("fork")
        states = []
        for start in range(0, len(jobs), max(1, processes)):
            batch = jobs[start:start + max(1, processes)]
            procs = []
            for pin, pout in batch:
                p = ctx.Process(target=_shuffle_reduce_worker,
                                args=(pin, pout, keys, aggs))
                p.start()
                procs.append(p)
            for p in procs:
                p.join(timeout=45)
                if p.exitcode != 0:
                    raise RuntimeError(f"shuffle reduce worker failed: {p.exitcode}")
            for _pin, pout in batch:
                with open(pout, "rb") as fh:
                    states.append(pickle.load(fh))

        final = merge_partials(states, plan.aggs)
        return _finalize_agg(final, plan.keys, plan.aggs)
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Stage 7: rule-based optimizer (filter pushdown, projection pruning)
# ---------------------------------------------------------------------------


def optimize(plan) -> Any:
    """Apply rule-based rewrites and return the optimized plan tree.

    Rules:
      * push Filter below Project (and into Scan.pushdown when possible),
      * prune Scan columns to only those used upstream (projection pruning).
    """
    plan = _push_filters(plan)
    plan = _prune_columns(plan, needed=None)
    return plan


def _push_filters(node):
    if isinstance(node, Filter):
        child = _push_filters(node.child)
        # Filter over Project(child): push the filter beneath the projection
        # as long as the predicate's columns survive the projection.
        if isinstance(child, Project):
            if columns_in(node.predicate) <= set(child.columns):
                pushed = Filter(child.child, node.predicate)
                return Project(_push_filters(pushed), child.columns)
        # Filter over Scan: record it as a pushed-down predicate (keep the
        # Filter too — pushdown only prunes row groups, it does not filter rows).
        if isinstance(child, Scan):
            new_scan = replace(child, pushdown=node.predicate)
            return Filter(new_scan, node.predicate)
        return Filter(child, node.predicate)
    return _map_children(node, _push_filters)


def _prune_columns(node, needed):
    """Push the set of needed columns down so a Scan reads only what's used."""
    if isinstance(node, Scan):
        if needed is None:
            return node
        keep = tuple(c for c in _scan_all_columns(node) if c in needed)
        return replace(node, columns=keep or None)
    if isinstance(node, Project):
        child = _prune_columns(node.child, set(node.columns))
        return Project(child, node.columns)
    if isinstance(node, Filter):
        req = (needed or set()) | columns_in(node.predicate)
        return Filter(_prune_columns(node.child, req if needed is not None else None),
                      node.predicate)
    if isinstance(node, Aggregate):
        req = set(node.keys) | {c for _f, c, _o in node.aggs if c is not None}
        return Aggregate(_prune_columns(node.child, req), node.keys, node.aggs)
    if isinstance(node, (HashJoin, BroadcastJoin)):
        return _map_children(node, lambda c: _prune_columns(c, None))
    if isinstance(node, (Exchange, Optimized)):
        return _map_children(node, lambda c: _prune_columns(c, needed))
    return node


def _scan_all_columns(node: Scan) -> list[str]:
    return [f.name for f in pq.ParquetFile(node.path).schema_arrow]


def _map_children(node, fn):
    """Return a copy of ``node`` with ``fn`` applied to each child plan."""
    if isinstance(node, Scan):
        return node
    if isinstance(node, (Project, Filter, Exchange, Optimized)):
        return replace(node, child=fn(node.child))
    if isinstance(node, Aggregate):
        return replace(node, child=fn(node.child))
    if isinstance(node, (HashJoin, BroadcastJoin)):
        return replace(node, left=fn(node.left), right=fn(node.right))
    return node


# ---------------------------------------------------------------------------
# Stage 8: spill-to-disk hash aggregate under a memory budget
# ---------------------------------------------------------------------------


def aggregate_with_spill(table: pa.Table, keys, aggs, *,
                         max_groups_in_memory: int,
                         spill_dir: str | None = None) -> pa.Table:
    """Hash aggregate that spills partial state to disk when the in-memory group
    count exceeds ``max_groups_in_memory``. The final answer is identical to the
    in-memory aggregate (spill is a memory-management detail, not a semantics
    change)."""
    tmpdir = spill_dir or tempfile.mkdtemp(prefix="mqe_spill_")
    os.makedirs(tmpdir, exist_ok=True)
    spill_files: list[str] = []
    state = partial_aggregate_streaming(table, keys, aggs,
                                        max_groups=max_groups_in_memory,
                                        on_spill=lambda s: _spill(s, tmpdir, spill_files))
    all_states = [state]
    for f in spill_files:
        with open(f, "rb") as fh:
            all_states.append(pickle.load(fh))
    final = merge_partials(all_states, aggs)
    for f in spill_files:
        try:
            os.remove(f)
        except OSError:
            pass
    if spill_dir is None:
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass
    return _finalize_agg(final, keys, aggs)


def _spill(state: dict, tmpdir: str, spill_files: list[str]):
    path = os.path.join(tmpdir, f"spill_{len(spill_files)}.pkl")
    with open(path, "wb") as fh:
        pickle.dump(state, fh)
    spill_files.append(path)


def partial_aggregate_streaming(table, keys, aggs, *, max_groups, on_spill) -> dict:
    """Like partial_aggregate but flushes (spills) the hash table whenever it
    grows past ``max_groups`` groups, so memory stays bounded."""
    specs = {out: _agg_spec(func) for func, _c, out in aggs}
    state: dict[tuple, dict[str, Any]] = {}
    key_cols = [table.column(k).to_pylist() for k in keys]
    val_cols = {col: table.column(col).to_pylist()
                for _f, col, _o in aggs if col is not None}
    n = table.num_rows
    for i in range(n):
        kt = tuple(key_cols[j][i] for j in range(len(keys)))
        grp = state.get(kt)
        if grp is None:
            if len(state) >= max_groups:
                on_spill(state)
                state = {}
            grp = {out: specs[out][0] for _f, _c, out in aggs}
            state[kt] = grp
        for func, col, out in aggs:
            _init, combine, _m, _f = specs[out]
            if func == "count" and col is None:
                grp[out] = combine(grp[out], None)
            else:
                v = val_cols[col][i]
                if v is None and func != "count":
                    continue
                grp[out] = combine(grp[out], v)
    return state


# ---------------------------------------------------------------------------
# execute() / explain() — the stable public entry points
# ---------------------------------------------------------------------------


def _materialize(node) -> pa.Table:
    if isinstance(node, Aggregate):
        return _execute_aggregate(node)
    if isinstance(node, (HashJoin, BroadcastJoin)):
        return _execute_join(node)
    if isinstance(node, Optimized):
        return _materialize(node.child)
    return _collect(node)


def execute(plan) -> pa.Table:
    """Execute a plan tree and return the result as a pyarrow.Table."""
    return _materialize(plan)


def _explain_node(node, depth: int) -> list[str]:
    pad = "  " * depth
    if isinstance(node, Scan):
        parts = [f"Scan {os.path.basename(node.path)}"]
        if node.columns:
            parts.append(f"columns={list(node.columns)}")
        if node.pushdown is not None:
            parts.append(f"PushedFilters={_fmt_pred(node.pushdown)}")
        if node.stats is not None and node.stats.row_groups_total:
            parts.append(
                f"rowGroups={node.stats.row_groups_read}/"
                f"{node.stats.row_groups_total} "
                f"(skipped {node.stats.row_groups_skipped})")
        return [pad + " ".join(parts)]
    label = type(node).__name__
    extra = ""
    if isinstance(node, Project):
        extra = f" {list(node.columns)}"
    elif isinstance(node, Filter):
        extra = f" {_fmt_pred(node.predicate)}"
    elif isinstance(node, Aggregate):
        extra = f" keys={list(node.keys)} aggs={[a for a in node.aggs]}"
    elif isinstance(node, Exchange):
        extra = f" hashpartitioning(keys={list(node.keys)}, n={node.num_partitions})"
    lines = [pad + label + extra]
    for child in _children(node):
        lines += _explain_node(child, depth + 1)
    return lines


def _children(node):
    if isinstance(node, (Project, Filter, Exchange, Optimized, Aggregate)):
        return [node.child]
    if isinstance(node, (HashJoin, BroadcastJoin)):
        return [node.left, node.right]
    return []


def _fmt_pred(expr) -> str:
    if isinstance(expr, Compare):
        return f"({_fmt_pred(expr.left)}{expr.op}{_fmt_pred(expr.right)})"
    if isinstance(expr, Col):
        return expr.name
    if isinstance(expr, Lit):
        return repr(expr.value)
    if isinstance(expr, And):
        return "(" + " AND ".join(_fmt_pred(p) for p in expr.preds) + ")"
    if isinstance(expr, Or):
        return "(" + " OR ".join(_fmt_pred(p) for p in expr.preds) + ")"
    return str(expr)


def explain(plan) -> str:
    """Return a Spark-like indented physical-plan string."""
    return "\n".join(_explain_node(plan, 0))


def new_scan(path, columns=None, pushdown=None) -> Scan:
    """Factory that attaches a fresh ScanStats counter to a Scan node."""
    return Scan(path=path,
                columns=tuple(columns) if columns else None,
                pushdown=pushdown,
                stats=ScanStats())
