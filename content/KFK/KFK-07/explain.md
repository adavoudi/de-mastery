# Explain it back (KFK-07)

Explain to a dbt-fluent teammate: *how a Debezium change stream becomes an SCD2
dimension, and why it is better than a daily snapshot.*

Rubric — a good answer covers:

1. CDC reads the DB **transaction log (WAL)**, not polling, so no intermediate
   state is lost and the OLTP system isn't hammered.
2. Kafka **Connect** hosts the connector; connector → tasks; converters/SMTs/DLQ.
3. The Debezium **envelope**: `op` (`r`/`c`/`u`/`d`), `before`, `after`, `source`.
4. **Snapshot** `op=r` = initial load; treat like an insert. **Tombstone** (null
   value) = compaction marker, **skipped** by SCD2.
5. SCD2 fold: insert/read → open current; update → **close old + open new**;
   delete → close current, open nothing.
6. Ordering by commit `ts_ms` (LSN tiebreaker) per key is what makes it correct.
7. Analogy: this is a continuous, lossless `dbt snapshot`; the outbox pattern is
   publishing a curated contract instead of raw tables.
