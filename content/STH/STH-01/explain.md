# STH-01 · Explain it back (Feynman)

**Prompt A.** "Walk a colleague from Lambda to Kappa to lakehouse to streamhouse. For each step,
name the one problem that the *previous* architecture had. This architecture was invented to fix
that problem. Then name the new cost that this architecture introduced."

**Prompt B.** "Someone gives you an unfamiliar data platform. Give the ordered set of yes/no
questions you ask about its *storage and pipeline shape*. These questions classify it as Lambda,
Kappa, lakehouse or streamhouse. Explain why you look at storage shape and not at the vendor's
marketing name."

## Rubric — a strong explanation hits these (5–7):

- [ ] Lambda = two parallel pipelines (batch + speed) merged at serving. The pain is **duplicated
      logic that drifts** and must be reconciled.
- [ ] Kappa = one streaming pipeline with the **log as system of record** and replay for
      reprocessing. The pain is **expensive and awkward long-term history and OLAP** in Kafka.
- [ ] Lakehouse = one **transactional lake table format** (Iceberg/Delta/Hudi) that serves SQL/BI.
      The limit is **batch/micro-batch latency** (and small files on frequent commits).
- [ ] Streamhouse = a **stream-native storage layer** (Paimon/Fluss) that serves **both** a
      streaming/changelog read and a batch/OLAP read from **one copy**. The cost is a **younger,
      more complex stack**.
- [ ] Classification uses **storage + pipeline shape**, not the vendor name.
- [ ] You can state when a streamhouse is **not** worth it (hourly freshness, log-only consumers,
      or a genuine two-computation Lambda).
- [ ] You connect it to your own batch lakehouse and say what a streamhouse would add underneath it.
