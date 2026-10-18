# SPK-05 / 03 · Read the plan

Reading a physical plan is the core skill of SPK-05. It is the same skill as reading a Redshift
`EXPLAIN`. Here you prove that you have it. You extract three facts directly from the plan of a
DataFrame that you receive. You do not know how someone built it.

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

- Get the physical plan string with `df._jdf.queryExecution()` ... or call
  `df.explain("formatted")` and capture its output. The helper `de_testkit.plan_of(df, "formatted")`
  is available if you import it. You can also use `df.explain`.
- `PushedFilters: [ ... ]` — non-empty ⇒ predicate pushdown happened.
- `ReadSchema: struct<name:type, ...>` — the columns that the scan reads (pruning).
- `Exchange` — each node is a shuffle boundary.

## How it is graded

The test builds several DataFrames with known plans (pushed/not, pruned/not, 0/1/2 shuffles). It
checks the facts that you return against the plan that the testkit helpers compute independently
(`plan_of`, `count_exchanges`). Nothing is hard-coded. Your parsing must follow the real plan.
