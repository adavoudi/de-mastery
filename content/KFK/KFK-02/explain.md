# KFK-02 · Explain it back (Feynman)

**Prompt A.** "Explain, as if to an auditor who knows databases but not Kafka, why `acks=1` can
silently lose an acknowledged write, and how `acks=all` + `min.insync.replicas=2` + RF=3 prevents
it. Walk through what the ISR does as brokers fail."

**Prompt B.** "A teammate set RF=3, acks=all, min.insync.replicas=3 'to be extra safe' and now
every produce fails the moment any broker restarts. Explain what's wrong and what they should set
instead, and what they'd be trading away."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines leader/followers and that clients write to the leader.
- [ ] Defines the ISR as the dynamically-maintained caught-up replica set (leader included).
- [ ] Explains each `acks` level and that `acks=1` loses data on leader crash before replication.
- [ ] Explains `min.insync.replicas` as the ISR floor for accepting an `acks=all` write.
- [ ] Shows RF=3/acks=all/misr=2 tolerates one failure and refuses writes under two.
- [ ] Explains that misr=3 on RF=3 makes any single broker loss stop all writes (the teammate's bug).
- [ ] (Bonus) Notes unclean leader election as the availability-vs-silent-loss trade; KRaft runs
      elections.
