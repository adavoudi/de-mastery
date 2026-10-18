# C3 Design Doc — Streamhouse commerce mart at 1 PB/year (with DR)

> **ARC-09 format.** Fill every section. The graded rubric checks that each of the
> six headings below is present and answered with *numbers*, not adjectives. This
> is the "payments ledger / CDC-to-lake" style prompt from ARC-09 applied to C3.

## 1. Requirements

- **Functional:** real-time Kimball star schema (SCD2 `dim_customer`, MDM golden
  record, `fct_orders` with FX as-of join) served to BI; batch backfills on the
  same tables (reuse C1).
- **Non-functional:** freshness SLA ____ (pick a tier from `cost/latency_tiers.yaml`);
  query p95 < ____ s; correctness: streaming == batch rebuild (G6); SOX audit
  (ARC-07) — immutable changelog, reconciliation `source == stream == lake`.
- **Scope / out of scope:** ____.
- **Multi-tenant:** 3 companies, one acquired mid-period (ARC-08 onboarding).

## 2. Estimates (capacity math)

- Volume: 1 PB/year ⇒ ____ GB/day ⇒ ____ events/s peak (state the fan-out).
- State size: SCD2 + golden record keyed by customer ⇒ ____ GB RocksDB state;
  TM count = ____.
- Storage: Fluss hot retention ____ days + Paimon/Iceberg lake ____; compaction
  budget ____.
- Compute: Flink slots = ____; checkpoint interval ____; small-file rate ____.
- Cost: cross-reference `cost/latency_tiers.yaml` (±20%).

## 3. Design (high level)

- Dataflow: C2 CDC topics → Fluss (log + PK tables) → tier to Paimon/Iceberg →
  serve (Trino/StarRocks). Draw the diagram (`design/architecture.svg`).
- Table choices: Paimon `partial-update` for the golden record; Paimon/Iceberg v2
  PK tables for SCD2/facts; partitioning + bucketing scheme = ____.
- Engine: Flink SQL + DataStream; batch rebuild = Spark on the same tables (C1).

## 4. Deep dive (pick two)

- **SCD2 from CDC:** close-and-open logic, late/out-of-order events, watermarks.
- **FX temporal join:** versioned table, `FOR SYSTEM_TIME AS OF`, rate gaps.
- **Golden record / MDM:** match key, merge order, conflict resolution.
- **Small-file vs. latency:** commit interval tuning (STH-04), compaction.

## 5. Failure modes & DR

- Job crash / TM loss: checkpoint + exactly-once sink recovery; RPO = ____, RTO = ____.
- Savepoint upgrade (M5): schema change without data loss (state migration plan).
- Region loss: lake on cross-region object storage; Fluss/Kafka MirrorMaker;
  warm-standby Flink in region B; failover runbook; **RPO/RTO targets + how met**.
- Poison data / schema break: quarantine + DLQ (STH-06); replay from the lake.
- Reconciliation: hourly `source == stream == lake`, `sum(debit)==sum(credit)`.

## 6. Trade-offs & alternatives

- Streamhouse vs. Lambda vs. Kappa vs. plain lakehouse — why this choice here
  (ARC-10 decision record). When NOT to use it (hourly SLA → batch is cheaper).
- Build vs. buy: self-managed Flink vs. AWS Managed Service for Apache Flink /
  Confluent Cloud (the optional 💲 comparison).
- Residual risks and what you'd do with more time.
