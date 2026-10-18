# STH-03 · Explain it back (Feynman)

**Prompt A.** "Explain Apache Fluss. Say what a log table and a PK table are. Say what lake tiering
does. Say why it lets one logical table serve both a sub-second lookup and a cheap historical batch
scan. Compare it with running Kafka plus a separate lakehouse."

**Prompt B.** "State the read-consistency invariant for a tiered Fluss table and say why it matters.
Describe how you would *test* that a streaming read and a batch read of the lake tier return the
same rows. Say why the batch read must be bounded with a deadline."

## Rubric — a strong explanation hits these (5–7):

- [ ] Fluss is a columnar, low-latency streaming storage layer. Log tables are append-only. PK
      tables are upsertable (latest value per key).
- [ ] Lake tiering (`table.datalake.enabled`) continuously moves committed data from the hot store
      of Fluss into a lake format (Paimon), as **one logical table**.
- [ ] Reading the table name gives the real-time view. Reading `` `table$lake` `` gives the tiered
      lake view, which you can read in batch.
- [ ] The invariant: after tiering quiesces, a streaming read and a batch read of the lake tier
      return the **same set of rows** (the G6 streaming-vs-batch oracle).
- [ ] Tiering is asynchronous. So a batch `$lake` read must be **bounded with a wall-clock
      deadline** (never block on an unbounded stream).
- [ ] You need a PK table, `datalake.format=paimon` and a configured warehouse. The `$lake` read
      needs paimon-flink on the classpath.
- [ ] You map it to the collapse of a low-latency store and a warehouse into one consistent logical
      table.
