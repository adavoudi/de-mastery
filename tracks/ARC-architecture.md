# ARC: Architecture & system design (~45 h)

**Why.** Interviews at senior DE level are decided in the system-design round. They test
"which tool and why", capacity math, failure modes and trade-offs, not API trivia. These modules
are interleaved with the tool tracks (see [ROADMAP.md](../ROADMAP.md#arc-checkpoints)).
They are graded with rubrics plus numeric checks (capacity/cost tests with tolerances), and you
can optionally request an AI "interviewer" review.

| ID | Module | h | Key ideas | Deliverable (graded) | From your world |
|---|---|---|---|---|---|
| ARC-01 | Data systems thinking | 3 | Reliability, scalability, maintainability; OLTP vs. OLAP vs. streaming; the "do you need it?" test | One-page trade-off memo | – |
| ARC-02 | Batch at PB scale | 5 | Layout (partitioning/sort/compaction), compute sizing, shuffle budgets, cost per TB, backfills | 5 PB/year batch design with numbers | Commerce mart |
| ARC-03 | Orchestration & backfills | 4 | Airflow/Dagster, idempotent tasks, data-aware scheduling, backfill strategy, dbt in the DAG | Dagster or Airflow DAG with an idempotent backfill; tested by rerunning it | dbt + scheduler |
| ARC-04 | Data contracts & quality | 4 | Producer-owned contracts, schema evolution policy, DQ at each layer, ownership | Contract spec + CI check for the C2 topics | dbt tests/contracts |
| ARC-05 | Platform landscape | 4 | EMR, Glue, Databricks, Snowflake, BigQuery, Redshift, Fabric; managed Kafka/Flink options; build vs. buy. Done after CDW: reuses the CDW-02 cost model and CDW-17 port instead of re-teaching the platforms | Map one design onto 3 different stacks, with CDW-02 cost numbers | Your AWS stack |
| ARC-06 | Event-driven architecture | 4 | Outbox, CDC, event sourcing, CQRS, sagas; ordering and idempotency across services | Design + sequence diagram for an order lifecycle | – |
| ARC-07 | Financial correctness & audit | 4 | Reconciliation, immutable audit logs, lineage (OpenLineage), replay, change control | SOX control matrix for C2/C3 with automated evidence | **SOX experience** |
| ARC-08 | Multi-tenant data & MDM at scale | 4 | Tenant isolation, golden records in streams, entity resolution, onboarding acquired companies | Design: onboard a newly acquired company in a week | **M&A + MDM experience** |
| ARC-09 | System design interview practice | 6 | Framework: requirements → estimates → design → deep dive → failure modes | 8 mock prompts (clickstream, ads attribution, payments ledger, CDC to lake, ...) | – |
| ARC-10 | Choosing batch vs. stream | 3 | Lambda/Kappa/streamhouse decision tree; cost of freshness; the hybrid reality | Decision records for 4 scenarios | Daily SLAs |
| ARC-11 | Observability, SLOs, FinOps | 4 | Freshness/completeness SLOs, lag dashboards, incident response, cost attribution | SLO + alert set for C3, with Grafana dashboards | – |

Use your SOX, M&A and MDM experience in interviews: ARC-07 and ARC-08 turn it into stories in the
"I designed X under constraint Y" format, which most candidates can't tell.
