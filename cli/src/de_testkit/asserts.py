"""Assertion helpers shared by exercise tests.

* ``assert_frames_equal`` normalizes Spark / pandas / Arrow / DuckDB results to a
  sorted Arrow table and compares them, with a readable diff on failure.
* ``plan_of`` / ``assert_plan_contains`` / ``count_exchanges`` inspect a Spark
  DataFrame's physical plan string.
"""

from __future__ import annotations

import math
from typing import Any

import pyarrow as pa


# ---------------------------------------------------------------------------
# Frame equality
# ---------------------------------------------------------------------------


def _to_arrow(obj: Any) -> pa.Table:
    """Convert a supported result object to a pyarrow Table."""
    # Already Arrow
    if isinstance(obj, pa.Table):
        return obj

    # DuckDB relation / connection result
    cls = type(obj).__module__ + "." + type(obj).__name__
    if hasattr(obj, "arrow") and "duckdb" in type(obj).__module__:
        return obj.arrow()
    if hasattr(obj, "fetch_arrow_table"):
        return obj.fetch_arrow_table()

    # Spark DataFrame
    if "pyspark" in type(obj).__module__ and hasattr(obj, "toPandas"):
        return pa.Table.from_pandas(obj.toPandas(), preserve_index=False)

    # pandas DataFrame
    if "pandas" in type(obj).__module__ and hasattr(obj, "to_dict"):
        return pa.Table.from_pandas(obj, preserve_index=False)

    # list[dict]
    if isinstance(obj, list):
        return pa.Table.from_pylist(obj)

    raise TypeError(
        f"assert_frames_equal: unsupported result type {cls!r}. "
        "Supported: pyarrow.Table, Spark DataFrame, pandas DataFrame, "
        "DuckDB relation, or list[dict]."
    )


def _normalize(table: pa.Table, ignore_order: bool) -> list[tuple]:
    """Return rows as a list of tuples, with columns sorted by name and (optionally)
    rows sorted, so order differences don't fail the comparison."""
    names = sorted(table.column_names)
    table = table.select(names)
    rows = [tuple(d[c] for c in names) for d in table.to_pylist()]
    if ignore_order:
        rows = sorted(rows, key=lambda t: tuple((v is None, str(v)) for v in t))
    return rows


def _cells_equal(a: Any, b: Any, rel_tol: float) -> bool:
    if a is None or b is None:
        return a is b or (a is None and b is None)
    if isinstance(a, float) or isinstance(b, float):
        try:
            return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=1e-12)
        except (TypeError, ValueError):
            return a == b
    return a == b


def assert_frames_equal(actual: Any, expected: Any, *,
                        ignore_order: bool = True,
                        rel_tol: float = 1e-9) -> None:
    """Assert two tabular results are equal as sorted Arrow tables.

    Compares column *names* (as a set), row count, and cell values (floats with
    ``rel_tol``). Column and row order are ignored by default.
    """
    at = _to_arrow(actual)
    et = _to_arrow(expected)

    acols, ecols = set(at.column_names), set(et.column_names)
    if acols != ecols:
        only_a = sorted(acols - ecols)
        only_e = sorted(ecols - acols)
        raise AssertionError(
            "Column mismatch.\n"
            f"  only in actual:   {only_a}\n"
            f"  only in expected: {only_e}\n"
            f"  actual columns:   {sorted(acols)}\n"
            f"  expected columns: {sorted(ecols)}"
        )

    arows = _normalize(at, ignore_order)
    erows = _normalize(et, ignore_order)

    if len(arows) != len(erows):
        raise AssertionError(
            f"Row count mismatch: actual={len(arows)} expected={len(erows)}.\n"
            f"  first actual rows:   {arows[:3]}\n"
            f"  first expected rows: {erows[:3]}"
        )

    for i, (ar, er) in enumerate(zip(arows, erows)):
        for j, (av, ev) in enumerate(zip(ar, er)):
            if not _cells_equal(av, ev, rel_tol):
                col = sorted(acols)[j]
                raise AssertionError(
                    f"Cell mismatch at sorted-row {i}, column {col!r}:\n"
                    f"  actual   = {av!r}\n"
                    f"  expected = {ev!r}\n"
                    f"  actual row:   {ar}\n"
                    f"  expected row: {er}"
                )


# ---------------------------------------------------------------------------
# Physical-plan helpers (Spark)
# ---------------------------------------------------------------------------


def plan_of(df: Any, mode: str = "formatted") -> str:
    """Return the physical plan string for a Spark DataFrame."""
    if not hasattr(df, "_jdf"):
        raise TypeError("plan_of expects a Spark DataFrame.")
    try:
        return df._jdf.queryExecution().explainString(
            df.sparkSession._jvm.org.apache.spark.sql.execution.ExplainMode
            .fromString(mode)
        )
    except Exception:
        # Fallback: capture df.explain() output.
        import contextlib
        import io
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            df.explain(mode)
        return buf.getvalue()


def assert_plan_contains(df: Any, needle: str, mode: str = "formatted") -> None:
    """Assert the physical plan contains ``needle`` (e.g. ``'BroadcastHashJoin'``)."""
    plan = plan_of(df, mode)
    if needle not in plan:
        raise AssertionError(
            f"Expected {needle!r} in the physical plan, but it was not found.\n"
            f"--- plan ---\n{plan}"
        )


def count_exchanges(df: Any, mode: str = "formatted") -> int:
    """Count ``Exchange`` nodes (shuffles) in the physical plan."""
    plan = plan_of(df, mode)
    return plan.count("Exchange")
