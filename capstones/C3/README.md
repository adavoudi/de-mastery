# C3 — Streamhouse real-time commerce mart (Flink + Paimon/Fluss + Iceberg)

> **Status: scaffolding.** The starter jobs/DDLs under `starter/` are deliberately
> incomplete; the acceptance tests under `tests/` therefore **fail or skip clearly**
> until you implement each milestone. A tiny reference implementation under
> `reference/` proves the test harness itself is valid (it passes). Do not copy the
> reference into `starter/` — build your own.

This is the third and largest capstone (~50 h). It builds on **FLK + STH** and reuses
C1 (Spark batch backfills on the same tables) and C2 (the CDC topics as the source).
The pitch: *one storage layer serving both the streaming mart and the batch rebuild,
with a real-time Kimball star schema.*

```
C2 CDC topics  ──►  Fluss (streaming storage)  ──tier──►  Paimon / Iceberg (lake)
 (orders,              log + PK tables                       SCD2 dim_customer
  payments,                                                  partial-update golden record
  customers,                                                 fct_orders (FX temporal join)
  fx_rates)                                                        │
                                                                   ▼
                                                     Serving (pypaimon / pyiceberg → DuckDB;
                                                     StarRocks/Trino optional Docker layer)
```

Everything graded runs **with NO Docker** on the native testkit fixtures
(`paimon_sql`, `iceberg_sql`, `fluss_cluster`, `serving_query`, `sth_oracle`). The
`lab_streamhouse` Docker serving engine is an optional extra layer.

## Layout

```
capstones/C3/
  README.md                 # this file
  capstone.yaml             # id, milestones, module map, acceptance-test list
  data/
    generate.py             # deterministic data hook (reuses de_data.commerce)
  sql/                      # Flink SQL job stubs (starter) — fill these in
    01_dim_customer_scd2.sql
    02_golden_record_partial_update.sql
    03_fct_orders_fx_temporal_join.sql
  ddl/                      # table DDLs referenced by the jobs and tests
    dim_customer_scd2.sql
    golden_record_partial_update.sql
    fct_orders.sql
  starter/                  # the learner's incomplete building blocks (FAIL G1)
    jobs.py
  reference/                # a tiny proven-correct impl (PASSES) — harness proof
    jobs.py
  tests/                    # acceptance-test scaffolding (pytest + testkit fixtures)
    conftest.py
    test_streaming_equals_batch_rebuild.py
    test_savepoint_upgrade.py          # stub (skips: needs native cluster milestone)
    test_latency_tier_cost.py
  cost/
    latency_tiers.yaml      # seconds/minutes/hours cost template (edit the numbers)
  design/
    DESIGN_DOC.md           # 1 PB/year + DR, ARC-09 format (requirements→…→failure modes)
```

The learner implements `starter/jobs.py` (and fills the `sql/*.sql` stubs). Tests set
`DE_C3_IMPL` to pick which module to grade (`starter` by default, `reference` for the
harness-validity proof); see `tests/conftest.py`.

## Milestones (each maps to the modules that teach it)

| # | Milestone | Teaches / builds on | Proven by (acceptance test) |
|---|---|---|---|
| M0 | **Data + ingest hook** — generate the commerce dataset deterministically; land `customers/orders/fx_rates` as a debezium-json changelog the Flink jobs read (stands in for C2's topics). | `de_data`, KFK-07 (CDC/outbox), STH-04 | `data/generate.py` runs; `test_streaming_equals_batch_rebuild` M0 setup |
| M1 | **SCD2 `dim_customer`** — build a versioned dimension from the CDC changelog (valid_from/valid_to/is_current). | STH-05 (streaming SCD2), LKH-05 (MERGE/SCD2), FLK-04 (changelog) | `test_streaming_equals_batch_rebuild::test_scd2_current_rows` |
| M2 | **Golden record** — Paimon `partial-update` merge engine unifies a customer across companies into one row (MDM). | STH-02 (Paimon merge engines), ARC-08 (MDM) | `test_streaming_equals_batch_rebuild::test_golden_record_partial_update` |
| M3 | **`fct_orders` + FX temporal join** — enrich orders at the FX rate **as of order time** (temporal/versioned join), not the latest rate. | STH-05 (temporal joins), FLK-05 (joins), LKH (facts) | `test_streaming_equals_batch_rebuild::test_fct_orders_fx_as_of` |
| M4 | **Streaming == batch rebuild** — the G6 oracle at capstone scale: streaming final state equals a DuckDB batch rebuild over the same events. | STH-05/06, TESTING.md G6 | `test_streaming_equals_batch_rebuild::test_oracle_full_mart` |
| M5 | **Stateful upgrade** — take a savepoint, change the job, restore with no data loss. | FLK-06/11 (state/savepoints) | `test_savepoint_upgrade` (stub; needs native `flink_cluster`) |
| M6 | **Fluss → lake tiering** — write upsert to Fluss, tier to Paimon, read `$lake` in batch == streaming read. | STH-03 (Fluss tiering) | `test_streaming_equals_batch_rebuild::test_fluss_lake_roundtrip` (opt-in) |
| M7 | **Latency-tier cost** — compute compute+storage+ops cost for seconds/minutes/hours freshness; within ±20% of the model. | STH-08, ARC-11 (FinOps) | `test_latency_tier_cost` |
| M8 | **Design doc** — run it at 1 PB/year incl. DR, ARC-09 format. | ARC-02 (PB scale), ARC-09 (interview framework) | manual rubric (`design/DESIGN_DOC.md` section checklist) |

Serving (StarRocks/Trino sub-second) and the AWS Managed-Flink / Confluent-Cloud
comparison are **optional `💲`** extras layered on top of M4; they are not graded
by the no-Docker harness.

## How to work on it

```bash
# 1. Generate the deterministic dataset + CDC changelog into a scratch dir
uv run python capstones/C3/data/generate.py --out /tmp/c3 --seed 42 --scale 0.001

# 2. Implement starter/jobs.py (and sql/*.sql). Grade a single milestone:
DE_SUBMISSION_DIR=capstones/C3/starter \
  uv run pytest capstones/C3/tests/test_streaming_equals_batch_rebuild.py -q

# 3. Prove the harness is valid against the tiny reference (should pass):
DE_C3_IMPL=reference \
  uv run pytest capstones/C3/tests -q
```

The fixtures SKIP cleanly (never fail) when the streamhouse artifacts are missing;
run `bash labs/fetch_streamhouse.sh` and `uv run de doctor` first. Keep every run
bounded (batch / `deadline_s`); never iterate an unbounded stream.

## Further reading (official)

- Paimon merge engines — https://paimon.apache.org/docs/master/primary-key-table/merge-engine/
- Fluss lake tiering — https://fluss.apache.org/docs/
- Iceberg Flink — https://iceberg.apache.org/docs/latest/flink/
- Flink SQL temporal joins — https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/joins/
