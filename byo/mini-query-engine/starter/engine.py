"""mini-query-engine — your cumulative codebase (starter).

You will grow THIS single file across all 8 stages. The plan-node dataclasses and
the function signatures below are the stable public API the tests call; do not
rename them. Replace each ``raise NotImplementedError`` as you reach its stage.

Public API (see stage-01/README.md for the full contract):

    execute(plan) -> pyarrow.Table
    explain(plan) -> str
    new_scan(path, columns=None, pushdown=None) -> Scan

Plan nodes: Scan, Project, Filter, Aggregate, HashJoin, BroadcastJoin, Exchange,
Optimized.  Expressions: Col, Lit, Compare, And, Or.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa  # noqa: F401  (tests build Arrow tables)


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Col:
    name: str


@dataclass(frozen=True)
class Lit:
    value: Any


@dataclass(frozen=True)
class Compare:
    left: Any
    op: str
    right: Any


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
    raise NotImplementedError("stage 7: implement columns_in")


# ---------------------------------------------------------------------------
# Plan nodes (frozen dataclasses)
# ---------------------------------------------------------------------------
class ScanStats:
    """Row-group skip counters exposed through explain() (stage 3)."""

    def __init__(self):
        self.row_groups_total = 0
        self.row_groups_read = 0
        self.row_groups_skipped = 0


@dataclass(frozen=True)
class Scan:
    path: str
    columns: tuple[str, ...] | None = None
    pushdown: Any = None
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


def new_scan(path, columns=None, pushdown=None) -> Scan:
    return Scan(path=path,
                columns=tuple(columns) if columns else None,
                pushdown=pushdown,
                stats=ScanStats())


# ---------------------------------------------------------------------------
# Stages — implement these as you go.
# ---------------------------------------------------------------------------
def execute(plan):
    """Stage 1+: execute a plan tree and return a pyarrow.Table."""
    raise NotImplementedError("stage 1: implement execute()")


def explain(plan) -> str:
    """Stage 1+: return a Spark-like indented plan string."""
    raise NotImplementedError("stage 1: implement explain()")


def partial_aggregate(table, keys, aggs) -> dict:
    """Stage 4: compute PARTIAL aggregation state over a chunk."""
    raise NotImplementedError("stage 4")


def merge_partials(states, aggs) -> dict:
    """Stage 4: merge PARTIAL states into a final state."""
    raise NotImplementedError("stage 4")


def stable_hash(value) -> int:
    """Stage 6: deterministic hash across processes."""
    raise NotImplementedError("stage 6")


def hash_partition(table, keys, num_partitions):
    """Stage 6: split a table into partitions by a stable hash of the keys."""
    raise NotImplementedError("stage 6")


def execute_parallel(plan, num_partitions=4, processes=2):
    """Stage 6: run a hash-aggregate as shuffled work across processes."""
    raise NotImplementedError("stage 6")


def optimize(plan):
    """Stage 7: rule-based optimizer -> optimized plan tree."""
    raise NotImplementedError("stage 7")


def aggregate_with_spill(table, keys, aggs, *, max_groups_in_memory,
                         spill_dir=None):
    """Stage 8 (optional): spill-to-disk hash aggregate under a memory budget."""
    raise NotImplementedError("stage 8")
