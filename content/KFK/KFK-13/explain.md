# KFK-13 · Explain it back (Feynman)

**Prompt A.** "Define RPO and RTO. Then explain why an active/passive MirrorMaker 2 design has a
non-zero RPO that equals the replication lag, and what determines its RTO. Contrast it with a
stretched cluster. Link it to a warehouse DR runbook that you would actually write."

**Prompt B.** "A stakeholder wants 'zero data loss and instant failover, cheaply, across two
regions'. Explain which parts of this request conflict. Explain what you would actually commit to, with
numbers."

## Rubric — a strong explanation covers these points (5–7):

- [ ] RPO = tolerable data loss (time). RTO = tolerable downtime (time).
- [ ] States that MM2 is asynchronous, so RPO = replication lag (never 0).
- [ ] Says that cutover automation decides most of the RTO for active/passive.
- [ ] Contrasts a stretched cluster: synchronous, RPO ≈ 0, higher latency, needs ≥3 failure domains.
- [ ] Mentions offset translation, so that failed-over consumers resume correctly.
- [ ] Notes that the topic prefix prevents loops in active/active.
- [ ] Frames the whole topic as a trade of cost/complexity vs RPO/RTO. Notes that "0 RPO + instant
      failover + cheap + 2 regions" is not possible at the same time.
