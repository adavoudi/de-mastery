# Stage 6 — Retention and log compaction

Topics carry a `cleanup.policy` (set through CreateTopics configs).

- `compact`: keep only the **latest record per key**, preserving each
  survivor's original offset, WITHOUT changing the log-end offset.
- `delete` (default): keep every version (optionally drop whole old segments by
  `retention.bytes`).

Compaction rewrites the log in place. Decode the records out of each v2 batch to
find the key of each record.

Tests: a compacted topic returns one record per key (the newest value); the log
end offset is unchanged; a delete-policy topic keeps all versions.
