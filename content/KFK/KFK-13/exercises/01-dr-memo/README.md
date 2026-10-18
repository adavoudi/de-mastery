# KFK-13 · 2-region DR design memo (rubric-graded)

Three teams ask you to design a 2-region (or multi-region) DR posture for their Kafka platform.
Record your decisions in `dr_design.yaml`. The grader does **not** read prose. For each scenario, it
checks facts that a machine can check:

- The **topology** that you chose.
- Your **RPO/RTO** numbers are in the band that the topology can really deliver.
- Your **factors** list names the required justifications.

Edit `dr_design.yaml`. For each of `scenario_1`, `scenario_2`, `scenario_3`, set these keys:

- `topology`: one of `active_passive`, `active_active`, `stretched`.
- `rpo_seconds`: tolerable data loss, in seconds (a number).
- `rto_seconds`: tolerable downtime, in seconds (a number).
- `factors`: a list of factor keys from the controlled vocabulary below. Include **all required**
  factors. Extra factors are allowed.

## The scenarios

**Scenario 1 — Cost-sensitive warm standby.** A commerce platform wants DR without doubling its
spend. A small, bounded data-loss window is acceptable. A rehearsed manual cutover within the hour is
fine. One region serves, and the other region is kept warm.

**Scenario 2 — Zero-data-loss, regulated.** A payments ledger must lose **no** committed data when one
region fails. It can accept a higher write latency and more infrastructure to get this. It must
survive the loss of one region without losing quorum.

**Scenario 3 — Both regions serving locally.** A global app wants a nearby cluster to serve the users
in each region with low latency. Each region mirrors to the other for resilience. The design must
avoid data looping between the two clusters.

## Controlled vocabulary for `factors`

`async_replication`, `synchronous_replication`, `warm_standby`, `offset_translation`,
`three_failure_domains`, `loop_prevention_prefix`, `cross_region_latency`, `local_low_latency`,
`mirrormaker2`, `cluster_linking`, `manual_cutover`, `automated_cutover`.

## What the grader requires (facts, cited to docs)

| Scenario | `topology` | RPO band (s) | RTO band (s) | Required `factors` |
|---|---|---|---|---|
| scenario_1 | `active_passive` | 1 – 600 (async, **non-zero**) | 60 – 3600 | `async_replication`, `offset_translation`, `warm_standby` |
| scenario_2 | `stretched` | 0 – 1 (synchronous) | 0 – 300 | `synchronous_replication`, `three_failure_domains` |
| scenario_3 | `active_active` | 1 – 600 (async) | 0 – 120 | `async_replication`, `loop_prevention_prefix`, `local_low_latency` |

These are the single answers that you can defend (see `lesson.md` and the cited docs). The grader
fails you in these cases:

- The topology is wrong.
- An RPO or RTO is outside the band that the topology can deliver (for example, RPO 0 for an async
  design).
- A required factor is missing.

> Optional extra layer: if the native Kafka tarball is vendored, a MirrorMaker 2 smoke check may run
> as a non-graded extra. The rubric above is the graded part and needs no Docker.
