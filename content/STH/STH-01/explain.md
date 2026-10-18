# STH-01 · Explain it back (Feynman)

**Prompt A.** "Walk a colleague from Lambda to Kappa to lakehouse to streamhouse. For each step,
name the one problem the *previous* architecture had that this one was invented to fix, and the new
cost it introduced."

**Prompt B.** "You are handed an unfamiliar data platform. Give the ordered set of yes/no questions
you ask about its *storage and pipeline shape* to classify it as Lambda, Kappa, lakehouse or
streamhouse — and explain why you look at storage shape, not the vendor's marketing name."

## Rubric — a strong explanation hits these (5–7):

- [ ] Lambda = two parallel pipelines (batch + speed) merged at serving; the pain is **duplicated
      logic that drifts** and must be reconciled.
- [ ] Kappa = one streaming pipeline with the **log as system of record** and replay for
      reprocessing; the pain is **expensive/awkward long-term history and OLAP** in Kafka.
- [ ] Lakehouse = one **transactional lake table format** (Iceberg/Delta/Hudi) serving SQL/BI; the
      limit is **batch/micro-batch latency** (and small files on frequent commits).
- [ ] Streamhouse = a **stream-native storage layer** (Paimon/Fluss) serving **both** a
      streaming/changelog read and a batch/OLAP read from **one copy**; the cost is a **younger,
      more complex stack**.
- [ ] Classification is by **storage + pipeline shape**, not the vendor name.
- [ ] Can state when a streamhouse is **not** worth it (hourly freshness, log-only consumers, or a
      genuine two-computation Lambda).
- [ ] Connects it to their own batch lakehouse and what a streamhouse would add underneath it.
