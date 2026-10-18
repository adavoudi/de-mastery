# Interview drills (KFK-07)

**1. Walk me through what Debezium emits when a connector starts on a table with
existing rows, then a row is updated and later deleted.**

On start, a consistent **snapshot**: one `op=r` event per existing row. Then it
streams WAL changes: the update is `op=u` with both `before` and `after` images;
the delete is `op=d` with `before` populated and `after` null, followed by a
**tombstone** (same key, null value) for compaction. Each event's `source` carries
the LSN and commit `ts_ms`.

**2. Why CDC instead of polling the table every minute?**

Polling misses states that appear and disappear within the interval, lags by up to
the interval, and loads the OLTP database with repeated scans. CDC reads the WAL,
so every committed change is captured exactly once in commit order, with low
latency and minimal source load.

**3. How do you turn `r/c/u/d` events into an SCD2 dimension?**

Order per key by commit time. `r`/`c` for a new key opens a current version
(`valid_from=ts`, `valid_to=null`, `is_current=true`). `u` closes the current
version (`valid_to=ts`) and opens a new one from `after`. `d` closes the current
version and opens nothing. Tombstones are skipped. The result is every historical
state with exact validity windows.

**4. A junior applies tombstones as deletes and ends up double-closing rows. What
went wrong?**

The `op=d` event already closed the current version. The tombstone that follows is
only a **compaction marker** (null value, no new state). Treating it as a second
delete tries to close an already-closed/absent row — a double-close or an error.
SCD2 derivation must skip null-value tombstones.

**5. What are the operational risks of enabling CDC on a production Postgres?**

Logical replication needs `wal_level=logical` and a **replication slot**. The slot
retains WAL until the connector consumes it; if the connector stalls or dies, WAL
accumulates and can fill the disk, threatening the primary. You also add snapshot
load on first start. Monitor slot lag and disk, and set up heartbeats.

**6. Why would you use the outbox pattern instead of CDC-ing business tables
directly?**

To publish a stable **event contract** decoupled from internal schema. Services
write domain events to an `outbox` table in the same transaction as the state
change (atomic), and Debezium streams the outbox. You can refactor the business
tables without breaking consumers, and the events are shaped for consumption rather
than mirroring normalized storage.
