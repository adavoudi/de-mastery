# Stage 2 — A segmented log with an offset index

Kafka stores each partition as an append-only **log split into segments**, with
a sparse **offset index** mapping offsets to byte positions so reads can seek.

Give every `(topic, partition)` its own directory under the data dir. Store
record batches in `<baseOffset>.log` files and index entries in
`<baseOffset>.index` (8 bytes each: relative-offset int32, byte-position int32).

Tests produce records (via Stage 3's Produce path — the reference implements
both) and then inspect your `.log`/`.index` files directly: a segment named by
base offset 0, a non-empty index whose first entry maps offset 0 to position 0,
and index positions that fall inside the log.
