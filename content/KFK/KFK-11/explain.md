# KFK-11 · Explain it back (Feynman)

**Prompt A.** "A partition's replica list is `[0, 1]` and you must move it onto the broker set
`{0, 2, 3}` at replication factor 2, with brokers 0 and 2 in rack A and broker 3 in rack B. Walk
through how you pick the new replica list to (a) satisfy rf and no-duplicate rules, (b) keep data
movement minimal, and (c) respect rack spread. Which constraint wins if they conflict?"

**Prompt B.** "Explain why 'rebalance by regenerating every assignment' is dangerous on a busy
cluster, and how `--execute` / `--verify` / `--throttle` let you move partitions safely. Tie it to
resizing a Redshift cluster at peak."

## Rubric — a strong explanation hits these (5–7):

- [ ] States the three validity rules: length == rf, no duplicate brokers, rack spread when racks
      are configured.
- [ ] Defines minimal movement as "keep as many current replicas as the constraints allow".
- [ ] Shows that reordering the same set is free but adding a new broker copies a whole replica.
- [ ] Explains rack awareness: replicas across distinct racks so a rack/AZ outage keeps a replica up.
- [ ] Describes --execute then --verify-until-complete, and --throttle to protect production traffic.
- [ ] Names the risk of a full reshuffle (network saturation, under-replicated partitions).
- [ ] Maps it to WLM/Redshift rebalancing or a peak-time resize.
