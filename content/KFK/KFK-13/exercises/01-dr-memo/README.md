# KFK-13 · 2-region DR design memo (rubric-graded)

Three teams ask you to design a 2-region (or multi-region) DR posture for their Kafka platform.
Record your decisions in `dr_design.yaml`. The grader does **not** read prose — per scenario it
checks machine-checkable facts: the **topology** you chose, that your **RPO/RTO** numbers fall in
the band that topology can actually deliver, and that your **factors** list names the required
justifications.

Edit `dr_design.yaml`. For each of `scenario_1`, `scenario_2`, `scenario_3` set:

- `topology`: one of `active_passive`, `active_active`, `stretched`.
- `rpo_seconds`: tolerable data loss, in seconds (a number).
- `rto_seconds`: tolerable downtime, in seconds (a number).
- `factors`: a list of factor keys from the controlled vocabulary below (include **all required**;
  extras are allowed).

## The scenarios

**Scenario 1 — Cost-sensitive warm standby.** A commerce platform wants DR without doubling spend.
A small, bounded data-loss window is acceptable, and a rehearsed manual cutover within the hour is
fine. One region serves; the other is kept warm.

**Scenario 2 — Zero-data-loss, regulated.** A payments ledger must lose **no** committed data on a
single-region failure, and can accept higher write latency and more infrastructure to get it. Must
survive one region going down without losing quorum.

**Scenario 3 — Both regions serving locally.** A global app wants users in each region served by a
nearby cluster with low latency, each region mirroring to the other for resilience. Must avoid data
looping between the two clusters.

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

These are the defensible single answers (see `lesson.md` and the cited docs). The grader fails you
if the topology is wrong, an RPO/RTO is outside the band that topology can deliver (e.g. RPO 0 for an
async design), or a required factor is missing.

> Optional extra layer: if the native Kafka tarball is vendored, a MirrorMaker 2 smoke check may run
> as a non-graded extra. The rubric above is the graded part and needs no Docker.
