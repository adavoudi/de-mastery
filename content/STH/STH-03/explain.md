# STH-03 · Explain it back (Feynman)

**Prompt A.** "Explain Apache Fluss: what a log table and a PK table are, what lake tiering does, and
why it lets one logical table serve both a sub-second lookup and a cheap historical batch scan.
Contrast it with running Kafka plus a separate lakehouse."

**Prompt B.** "State the read-consistency invariant for a tiered Fluss table and why it matters.
Describe how you would *test* that a streaming read and a batch read of the lake tier return the same
rows, and why the batch read must be bounded with a deadline."

## Rubric — a strong explanation hits these (5–7):

- [ ] Fluss is a columnar, low-latency streaming storage layer; log tables are append-only, PK
      tables are upsertable (latest value per key).
- [ ] Lake tiering (`table.datalake.enabled`) continuously moves committed data from Fluss's hot
      store into a lake format (Paimon), as **one logical table**.
- [ ] Reading the table name gives the real-time view; reading `` `table$lake` `` gives the tiered
      (batch-readable) lake view.
- [ ] The invariant: after tiering quiesces, a streaming read and a batch read of the lake tier
      return the **same set of rows** (the G6 streaming-vs-batch oracle).
- [ ] Tiering is asynchronous, so a batch `$lake` read must be **bounded with a wall-clock
      deadline** (never block on an unbounded stream).
- [ ] Needs a PK table + `datalake.format=paimon` + warehouse configured; `$lake` read needs
      paimon-flink on the classpath.
- [ ] Maps it to collapsing a low-latency store + a warehouse into one consistent logical table.
