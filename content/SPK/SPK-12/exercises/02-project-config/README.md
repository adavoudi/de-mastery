# Config precedence and a testable transform

Production Spark code is configured, not hard-coded. Build the configuration layer and the pure
transform. Together, they make a job portable and testable.

Implement two things in `solution.py`:

### 1. `build_config(env, overrides=None) -> dict[str, str]`

Merge three layers. Each layer has a higher precedence than the one before:

1. **BASE** defaults (always present): at least
   - `spark.sql.shuffle.partitions = "200"`
   - `spark.sql.adaptive.enabled = "false"`
   - `spark.sql.session.timeZone = "UTC"`
2. **per-env profile** for `env in {"dev", "prod"}`:
   - `dev`: keep BASE, but `spark.sql.shuffle.partitions = "8"`.
   - `prod`: `spark.sql.adaptive.enabled = "true"` and `spark.sql.shuffle.partitions = "400"`.
3. **overrides**: an optional dict that wins over everything.

Rules:
- The result always contains every BASE key. Profiles and overrides change only the keys that they set.
- `overrides` has the highest precedence. The `env` profile beats BASE.
- **Do not put secrets or absolute paths in BASE or the profiles.** Anything that is specific to an
  environment must arrive through `overrides`. The test checks that BASE/dev/prod contain no key that
  looks like a secret.

### 2. `revenue_report(orders_df) -> DataFrame`

A **pure** transform. You get `orders_df(company_id, order_item_id, line_amount)`. Return one row per
`company_id` with `revenue = sum(line_amount)`. Do no I/O and use no hard-coded paths. It is only a
`DataFrame -> DataFrame` function, so you can unit-test it.

The test checks the config precedence rules (dev vs prod vs overrides, BASE always present, no
secrets). It also compares `revenue_report` against a DuckDB oracle.

> See `emr_serverless/README.md` in this exercise for the **optional, ungraded 💲** 50 GB EMR
> Serverless run (cost estimate, required `project=de-mastery` tag, and a teardown script). It
> does **not** call AWS for you.
