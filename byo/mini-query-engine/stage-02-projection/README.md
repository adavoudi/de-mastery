# Stage 2 — Projection and filter operators (Volcano model)

Add two physical operators on top of your scan, in the classic **Volcano /
iterator** style: each operator pulls `RecordBatch`es from its child and pushes
transformed batches to its parent.

## Task

Make `execute()` handle these plan nodes:

- `Project(child, columns)` — select/reorder columns.
- `Filter(child, predicate)` — keep rows where `predicate` is true.

Predicates are expression trees over the stage-1 API:

```python
Filter(scan, Compare(Col("amt"), ">", Lit(25.0)))
Filter(scan, And(Compare(Col("amt"), ">=", Lit(10.0)),
                 Or(Compare(Col("cat"), "=", Lit("a")),
                    Compare(Col("cat"), "=", Lit("b")))))
```

## Requirements

- Evaluate predicates with `pyarrow.compute`: map each `Compare.op` to the right
  kernel (`equal`, `less`, `greater`, …) and combine `And`/`Or` with
  `and_kleene` / `or_kleene` so NULLs follow SQL three-valued logic.
- `Project` and `Filter` must compose with each other and with `Scan`.
- Results match DuckDB for the same `WHERE` / column list (G6). A Hypothesis
  property checks **filter soundness**: every surviving row satisfies the
  predicate and the row count matches the oracle.

## Constraints

- Stay in the iterator model — operators pull batches, they don't re-read files.
- Earlier stage (scan) must keep passing: `de test` runs stages 1–2 together.
