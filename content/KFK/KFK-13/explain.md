# KFK-13 · Explain it back (Feynman)

**Prompt A.** "Define RPO and RTO, then explain why an active/passive MirrorMaker 2 design has a
non-zero RPO equal to the replication lag, and what determines its RTO. Contrast with a stretched
cluster. Tie it to a warehouse DR runbook you'd actually write."

**Prompt B.** "A stakeholder wants 'zero data loss and instant failover, cheaply, across two
regions'. Explain which parts of that are in tension and what you'd actually commit to, with
numbers."

## Rubric — a strong explanation hits these (5–7):

- [ ] RPO = tolerable data loss (time); RTO = tolerable downtime (time).
- [ ] States MM2 is asynchronous, so RPO = replication lag (never 0).
- [ ] Says RTO for active/passive is dominated by cutover automation.
- [ ] Contrasts a stretched cluster: synchronous, RPO ≈ 0, higher latency, needs ≥3 failure domains.
- [ ] Mentions offset translation so failed-over consumers resume correctly.
- [ ] Notes the active/active topic-prefix loop prevention.
- [ ] Frames the whole thing as a cost/complexity vs RPO/RTO trade (and that "0 RPO + instant +
      cheap + 2 regions" is not simultaneously achievable).
