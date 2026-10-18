# Interview drills (KFK-07)

**1. Walk me through what Debezium emits when a connector starts on a table with existing rows.
Then a row is updated, and later it is deleted.**

At start, Debezium takes a consistent **snapshot**. It emits one `op=r` event for each existing
row. Then it streams the WAL changes:

- The update is `op=u` with both `before` and `after` images.
- The delete is `op=d` with `before` filled and `after` null. A **tombstone** follows it (same key,
  null value) for compaction.

The `source` of each event carries the LSN and the commit `ts_ms`.

**2. Why CDC instead of polling the table every minute?**

Polling misses states that appear and disappear within the interval. It lags by up to the interval.
It also loads the OLTP database with repeated scans. CDC reads the WAL, so it captures every
committed change in commit order. (Delivery is normally at-least-once, so the consumer must handle duplicates.) It has low latency and a minimal load on the source.

**3. How do you turn `r/c/u/d` events into an SCD2 dimension?**

Order the events by commit time for each key.

- `r`/`c` for a new key opens a current version (`valid_from=ts`, `valid_to=null`,
  `is_current=true`).
- `u` closes the current version (`valid_to=ts`) and opens a new one from `after`.
- `d` closes the current version and opens nothing.
- Skip tombstones.

The result has every historical state with exact validity windows.

**4. A junior applies tombstones as deletes and ends up double-closing rows. What went wrong?**

The `op=d` event already closed the current version. The tombstone that follows is only a
**compaction marker** (null value, no new state). If you treat it as a second delete, you try to
close a row that is already closed or does not exist. This gives a double-close or an error. SCD2
derivation must skip null-value tombstones.

**5. What are the operational risks of enabling CDC on a production Postgres?**

Logical replication needs `wal_level=logical` and a **replication slot**. The slot retains WAL until
the connector consumes it. If the connector stalls or dies, WAL accumulates. It can fill the disk
and threaten the primary. You also add snapshot load when the connector first starts. Monitor the
slot lag and the disk, and set up heartbeats.

**6. Why would you use the outbox pattern instead of CDC-ing business tables directly?**

You publish a stable **event contract** that is decoupled from the internal schema. Services write
domain events to an `outbox` table in the same transaction as the state change (atomic). Debezium
streams the outbox. You can refactor the business tables without breaking consumers. The events are
shaped for consumption and do not mirror the normalized storage.
