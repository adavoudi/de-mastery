# KFK-13 · Interview drills

**1. "RPO vs RTO in one line each."**
> RPO = how much data (as a time window) you can afford to lose. RTO = how long you can afford to be
> down. A DR design is mostly a negotiation of these two numbers against cost and complexity.

**2. "Why is MirrorMaker 2's RPO non-zero, and what sets its value?"**
> MM2 replicates asynchronously. It consumes from the source and produces to the target after the
> fact, so the target always lags. You lose whatever has not replicated when the source fails. The RPO
> equals that replication lag (seconds to minutes, depending on throughput and MM2 tuning).

**3. "Active/passive vs active/active vs stretched — pick one for 'RPO 0'. Trade-offs?"**
> Only a stretched cluster (synchronous `acks=all` replicas in multiple regions) gets RPO ≈ 0. The
> cost is a cross-region write latency on every produce. It also needs ≥3 failure domains for quorum.
> Active/passive and active/active use async mirroring, so RPO = lag. Active/active also lowers the
> RTO, because the other region is already serving. The price is that you must handle loops and
> duplicates.

**4. "A consumer fails over to the DR region. What has to be true for it to resume correctly?"**
> Its offsets must be translated to the offsets of the target cluster. MM2 publishes offset-sync and
> checkpoint topics for this. Cluster Linking preserves offsets byte-for-byte. Without translation,
> the consumer either reprocesses from the start or skips the gap.

**5. "Give me defensible RPO/RTO numbers for an active/passive MM2 commerce platform and justify
them."**
> Something like this: RPO ≈ 1–5 min. MM2 lag under peak throughput bounds it, and you measure it
> instead of guessing. RTO ≈ 10–30 min for a manual but rehearsed cutover, or a few minutes if the
> cutover is automated. The numbers follow from the architecture: async lag drives RPO, and cutover
> automation drives RTO.

**6. "Why not stretch a cluster across just two regions?"**
> Quorum needs a majority of an odd number of failure domains. With two regions, the loss of one
> leaves no majority. Then the controller quorum (and leader election) stalls. Use three regions/AZs,
> or use an async two-region design.
