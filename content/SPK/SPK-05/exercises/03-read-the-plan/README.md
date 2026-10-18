# SPK-05 / 03 · Read the plan

Reading a physical plan is the core SPK-05 skill — the same muscle as reading a Redshift `EXPLAIN`.
Here you prove it by extracting three facts straight from the plan of a DataFrame you are handed,
with no knowledge of how it was built.

## Task

Implement `plan_facts(df)` that returns a dict:

```python
{
  "pushed_down": bool,        # True iff the scan has a non-empty PushedFilters list
  "pruned_columns": set[str], # the set of column names in the scan's ReadSchema
  "num_shuffles": int,        # number of Exchange nodes in the physical plan
}
```

## How to read the plan

- Get the physical plan string with `df._jdf.queryExecution()` ... or simply call
  `df.explain("formatted")`'s captured output. The helper `de_testkit.plan_of(df, "formatted")` is
  available if you import it, but you may also use `df.explain`.
- `PushedFilters: [ ... ]` — non-empty ⇒ predicate pushdown happened.
- `ReadSchema: struct<name:type, ...>` — the columns the scan reads (pruning).
- `Exchange` — each node is a shuffle boundary.

## How it is graded

The test builds several DataFrames with known plans (pushed/not, pruned/not, 0/1/2 shuffles) and
checks your returned facts against the plan computed independently by the testkit helpers
(`plan_of`, `count_exchanges`). Nothing is hard-coded; your parsing must track the real plan.
