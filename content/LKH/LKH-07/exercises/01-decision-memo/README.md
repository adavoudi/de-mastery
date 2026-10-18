# Format decision memo (machine-graded rubric)

Three teams ask you to pick a table format. Record your decision in `decision.yaml`. The grader
does **not** read prose. For each scenario, it checks two things that a machine can check. The
first is the **format** that you chose (one defensible answer). The second is that your **factors**
list includes the required justification keys.

Edit `decision.yaml`. For each of `scenario_1`, `scenario_2` and `scenario_3`, set these keys:

- `format`: one of `iceberg`, `delta`, `hudi`.
- `factors`: a list of factor keys from the controlled vocabulary below. You must include **all
  required** factors for that scenario. You may add extra factors.

## The scenarios

**Scenario 1 — Databricks platform.** A team runs entirely on Databricks with Unity Catalog for
governance. The team wants data skipping that the platform maintains incrementally via
`CLUSTER BY` (liquid clustering). The team also wants Lakeflow pipelines. It uses one vendor and is
Spark-centric.

**Scenario 2 — Vendor-neutral lake.** Trino, Athena, Spark and DuckDB must query a central lake
(no Databricks). The platform team expects to change partitioning later (for example, add
`days(event_ts)`) **without rewriting history**. The team wants analysts to filter raw columns
without a physical partition column in the queries.

**Scenario 3 — CDC ingestion.** A near-real-time pipeline applies frequent **primary-key upserts**
from a CDC stream. It must serve downstream consumers that say "**give me only the records that
changed since time T**". It must do this efficiently, and it must find the affected files without a
scan of the whole table.

## Controlled vocabulary for `factors`

`databricks_native`, `unity_catalog`, `liquid_clustering`, `engine_neutral`, `partition_evolution`,
`hidden_partitioning`, `record_level_index`, `upsert_heavy`, `incremental_pulls`, `append_only`,
`single_vendor`, `multi_engine`.

## What the grader requires (facts, cited to docs)

| Scenario | `format` | Required `factors` (all must be present) |
|---|---|---|
| scenario_1 | `delta` | `databricks_native`, and at least one of {`unity_catalog`, `liquid_clustering`} |
| scenario_2 | `iceberg` | `engine_neutral`, `partition_evolution` |
| scenario_3 | `hudi` | `record_level_index`, `incremental_pulls` |

These are the defensible single answers (see `lesson.md` → *How to choose* and the cited docs). The
grader fails you if the format is wrong or a required factor is missing.
