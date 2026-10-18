# Explain it back (KFK-07)

Explain this to a teammate who knows dbt well: *how a Debezium change stream becomes an SCD2
dimension, and why this is better than a daily snapshot.*

Rubric — a good answer covers:

1. CDC reads the database **transaction log (WAL)**. It does not poll. So you lose no intermediate
   state, and the OLTP system does not get a heavy load.
2. Kafka **Connect** hosts the connector. A connector has tasks. Connect also has
   converters, SMTs, and a DLQ.
3. The Debezium **envelope**: `op` (`r`/`c`/`u`/`d`), `before`, `after`, `source`.
4. **Snapshot** `op=r` = initial load. Treat it like an insert. **Tombstone** (null value) =
   compaction marker. SCD2 **skips** it.
5. SCD2 fold: insert/read → open current. Update → **close old + open new**. Delete → close current
   and open nothing.
6. Ordering by commit `ts_ms` (LSN tiebreaker) for each key makes the result correct.
7. Analogy: this is a continuous, lossless `dbt snapshot`. The outbox pattern publishes a curated
   contract instead of raw tables.
