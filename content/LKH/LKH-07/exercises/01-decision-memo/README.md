# Format decision memo (machine-graded rubric)

Three teams ask you to pick a table format. Record your decision in `decision.yaml`. The grader does
**not** read prose — it checks two machine-checkable things per scenario: the **format** you chose
(one defensible answer) and that your **factors** list includes the required justification keys.

Edit `decision.yaml`. For each of `scenario_1`, `scenario_2`, `scenario_3` set:

- `format`: one of `iceberg`, `delta`, `hudi`.
- `factors`: a list of factor keys from the controlled vocabulary below. You must include **all
  required** factors for that scenario; extra ones are allowed.

## The scenarios

**Scenario 1 — Databricks platform.** A team runs entirely on Databricks with Unity Catalog for
governance and wants incrementally maintained data skipping via `CLUSTER BY` (liquid clustering),
plus Lakeflow pipelines. One vendor, Spark-centric.

**Scenario 2 — Vendor-neutral lake.** A central lake must be queried by Trino, Athena, Spark and
DuckDB (no Databricks). The platform team expects to change partitioning later (e.g. add
`days(event_ts)`) **without rewriting history**, and wants analysts to filter raw columns without a
physical partition column leaking into queries.

**Scenario 3 — CDC ingestion.** A near-real-time pipeline applies frequent **primary-key upserts**
from a CDC stream and must serve downstream consumers "**give me only the records that changed since
time T**" efficiently, locating affected files without scanning the whole table.

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
