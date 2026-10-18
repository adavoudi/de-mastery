# KFK-11 · Explain it back (Feynman)

**Prompt A.** "The replica list of a partition is `[0, 1]`. You must move it onto the broker set
`{0, 2, 3}` at replication factor 2. Brokers 0 and 2 are in rack A. Broker 3 is in rack B. Describe how
you pick the new replica list to (a) satisfy the rf and no-duplicate rules, (b) keep data movement
minimal, and (c) respect the rack spread. Which constraint wins if they conflict?"

**Prompt B.** "Explain why 'rebalance by regenerating every assignment' is dangerous on a busy
cluster. Explain how `--execute` / `--verify` / `--throttle` let you move partitions safely. Link it to
the resize of a Redshift cluster at peak."

## Rubric — a strong explanation covers these points (5–7):

- [ ] States the three validity rules: length == rf, no duplicate brokers, rack spread when racks
      are configured.
- [ ] Defines minimal movement as "keep as many current replicas as the constraints allow".
- [ ] Shows that a reorder of the same set is free but a new broker copies a whole replica.
- [ ] Explains rack awareness: replicas on distinct racks so a rack/AZ outage still leaves a replica up.
- [ ] Describes --execute, then --verify until complete, and --throttle to protect production traffic.
- [ ] Names the risk of a full reshuffle (network saturation, under-replicated partitions).
- [ ] Maps it to WLM/Redshift rebalancing or a resize at peak.
