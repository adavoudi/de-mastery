# Stage 3 — Produce: RecordBatch v2 + CRC32C

Implement **Produce** (api 0). The client sends a serialized **RecordBatch v2**
per partition; you append it to the log and reply with the base offset you
assigned.

- Assign the current log-end offset as the batch's base; stamp it into the
  batch header (first 8 bytes) before writing.
- The batch header carries a **CRC32C** (Castagnoli) over the bytes after the
  crc field — the tests re-check it, so your stored bytes must stay valid.
- Advance the log-end offset by the record count.

Tests: a real producer gets contiguous offsets (0,1,2,...), a second produce
continues the offsets, and the stored batch is magic=2 with a valid CRC32C.
