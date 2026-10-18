# KFK-03 · Explain it back (Feynman)

**Prompt A.** "Explain how you'd tune a producer for higher throughput on a JSON event feed, and
why doing so does NOT have to cost you ordering or create duplicates. Name the exact settings and
what each does."

**Prompt B.** "A colleague sees duplicate and out-of-order account events after a network blip.
Walk through the two mechanisms (duplicate vs. reorder) and the single setting that fixes both,
including the in-flight-requests caveat."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes batching: records buffer into per-partition batches sent by an I/O thread.
- [ ] Names `linger.ms`, `batch.size`, `compression.type` as the throughput knobs and what each does.
- [ ] Explains per-key ordering = keyed records hash to one partition; order is per-partition only.
- [ ] Distinguishes the duplicate mechanism (lost ack → retry rewrites) from the reorder mechanism
      (retry lands after a later success with >1 in-flight).
- [ ] Names `enable.idempotence=true` as fixing both via producer id + sequence numbers.
- [ ] States the `max.in.flight <= 5` caveat for ordering under retry.
- [ ] (Bonus) Notes idempotence implies `acks=all` + bounded retries, so throughput tuning and
      safety coexist.
