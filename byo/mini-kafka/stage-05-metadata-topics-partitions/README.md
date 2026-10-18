# Stage 5 — Metadata + multiple topics & partitions

Make the broker multi-topic and multi-partition.

- **Metadata** (api 3) lists each topic's partitions, with this broker as leader
  and sole replica/ISR. Unknown requested topics are auto-created.
- Key your logs by `(topic, partition)`; a Produce/Fetch names the partition.
- **CreateTopics** (api 19) lets `AdminClient.create_topics` set the partition
  count — store it and report it in Metadata.

Tests: two topics stay isolated; producing to partition 2 of a 3-partition
topic is readable only on partition 2; per-partition end offsets are
independent.
