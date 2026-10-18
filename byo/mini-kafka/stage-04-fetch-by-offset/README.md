# Stage 4 — Fetch by offset

Implement **Fetch** (api 1) and **ListOffsets** (api 2) so a real consumer can
read your log.

- Return the raw record batches covering offsets `>= fetch_offset` in the
  `records` field; use the offset index to seek and skip batches below the
  requested offset.
- Report `high_watermark`, `last_stable_offset`, `log_start_offset`.
- ListOffsets answers earliest (ts −2) and latest (ts −1).

Tests: a manually-assigned consumer reads everything from offset 0, reads from a
mid-log offset, sees nothing past the end, and `get_watermark_offsets` returns
`(0, N)`.
