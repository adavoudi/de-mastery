# Stage 1 — Scan Parquet in row-group batches

You are building a small **columnar query engine** in Python, with Apache Arrow
record batches as the in-memory format and DuckDB as the correctness oracle. You
grow a single file, `engine.py`, across all 8 stages. This stage lays the
foundation: reading a Parquet file one **row group** at a time.

## The public API (stable for every stage)

All stages extend this same surface. Do not rename these names — the tests call
them directly.

```python
# Entry points
execute(plan) -> pyarrow.Table      # run a plan tree, return the result
explain(plan) -> str                # a Spark-like, indented plan string
new_scan(path, columns=None, pushdown=None) -> Scan   # Scan factory (+ ScanStats)

# Plan nodes (frozen dataclasses) — a plan is a tree of these
Scan(path, columns=None, pushdown=None, stats=ScanStats)
Project(child, columns)             # keep/reorder columns          (stage 2)
Filter(child, predicate)            # row filter, Volcano model      (stage 2)
Aggregate(child, keys, aggs)        # hash aggregate                 (stage 4)
HashJoin(left, right, left_keys, right_keys, how="inner")           # stage 5
BroadcastJoin(...)                  # same fields as HashJoin         (stage 5)
Exchange(child, keys, num_partitions)   # hash-partition exchange     (stage 6)
Optimized(child)                    # optional optimizer marker       (stage 7)

# Expressions (used by Filter.predicate)
Col(name) / Lit(value)
Compare(left, op, right)            # op in {"=","!=","<","<=",">",">="}
And(*preds) / Or(*preds)
```

The dataclasses are already defined in the starter. Your job is to implement the
behavior behind `execute()` and `explain()`.

## This stage's task

Implement scanning so that `execute(new_scan(path))` returns the whole Parquet
file as a `pyarrow.Table`:

- Read the file **row group by row group** (`pyarrow.parquet.ParquetFile` +
  `read_row_group`), yielding `RecordBatch`es — the beginning of a Volcano
  (pull-based) operator pipeline.
- Honour **column projection**: `new_scan(path, columns=[...])` reads only those
  columns.
- `explain(plan)` returns a string whose Scan line contains `Scan`.
- An empty Parquet file returns a 0-row table with the correct schema.

## Constraints

- Arrow-native: results are `pyarrow.Table`s. No pandas required.
- Keep it lazy/iterator-based — later stages add operators that pull batches from
  this scan, so structure `execute()` around a per-node batch generator.
- The suite runs in well under 60 s. Oracle: DuckDB `read_parquet(...)`.

## Run it

```bash
de start SPK-BYO/01       # copies the cumulative starter into your workspace
de test                   # runs this stage (and, later, earlier stages too)
de hint                   # progressive hints
```
