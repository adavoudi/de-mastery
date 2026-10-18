# Stage 8 — Replication & the high watermark

In a replicated cluster the **high watermark (HW)** is the offset up to which
records are committed on enough replicas to be visible to consumers. On your
single node there are no followers, so once a record is durably written
(flush + fsync) the HW advances to the log-end offset.

- Track a per-partition HW; advance it on durable append.
- Serve Fetch / ListOffsets(latest) from the HW and never return records at or
  beyond it.
- Restore the HW on startup so it (and all offsets) survive a restart from the
  same data dir.

Tests: HW equals the produced record count; a consumer reads everything up to
the HW; HW and data survive a restart.
