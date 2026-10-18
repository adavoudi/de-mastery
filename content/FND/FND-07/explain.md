# Explain it (Feynman)

Explain to a new engineer who freezes when asked "how big will this be?":

> "Walk me through sizing a 2M-events/sec feed for a year — raw throughput, stored bytes, and a
> rough yearly cost — and show me which two assumptions the whole number depends on."

## Rubric (self-check — aim for 5/6)

- [ ] Converts events/sec + bytes/event into **bytes/sec** (throughput).
- [ ] Scales to a year with `seconds/year ≈ 3.15 × 10^7`.
- [ ] Applies **compression** (down) and **replication** (up) to get **stored** bytes. Says that
      these are the dominant assumptions.
- [ ] Separates **storage cost** (`$/GB-month`) from **query cost** (`$/TB-scanned`).
- [ ] States that pruning (FND-06) reduces `TB_scanned` and so reduces the bill.
- [ ] Frames the output as an **order of magnitude with explicit assumptions**, not a precise
      number.
