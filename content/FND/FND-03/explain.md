# Explain it (Feynman)

Explain to a data analyst who trusts S3 durability but has never heard of Raft:

> "How do a bunch of unreliable machines agree on a single history of writes without ever
> electing two leaders — and why is that the same reason S3 never loses an acknowledged object?"

## Rubric (self-check — aim for 5/6)

- [ ] States why replication is needed (single machines fail) and the new problem it creates
      (which copy is the truth / split brain).
- [ ] Defines a **quorum** and the rule `W + R > N`, and names the **majority** `⌊N/2⌋ + 1`.
- [ ] Explains **why majorities work**: any two majorities overlap on at least one member.
- [ ] States Raft's two election rules (one vote per term; majority to win) and that they give
      **at most one leader per term**.
- [ ] Connects to Kafka **ISR** / `acks=all` / `min.insync.replicas` as a concrete quorum.
- [ ] Maps to S3 durability (quorum-acknowledged replicated write) and why Athena never loses an
      acked object.
