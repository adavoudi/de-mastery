# Interview drills — FND-03

### 1. "Explain how a quorum guarantees you read the latest write."
**Model answer.** Suppose you have `N` replicas. Require `W` acks on write and `R` replicas
consulted on read. If `W + R > N`, the write set and the read set must overlap on at least one
replica. That replica holds the newest value, so the read can find it (take the value with the
highest version).

A majority on both sides (`W = R = ⌊N/2⌋ + 1`) is the common safe choice. Any two majorities of
the same set intersect.

### 2. "How does Raft guarantee a single leader per term?"
**Model answer.** Two rules. First, each node votes at most once per term. Second, a candidate
must collect a majority of votes to become the leader.

Suppose two candidates both won the same term. Each would need a majority. Any two majorities of
the same cluster share at least one node. That node would have voted twice in one term, and the
first rule forbids this. So there is at most one leader per term.

Candidates also get votes only if their log is at least as up to date as the log of the voter.
This preserves committed entries.

### 3. "A teammate sets acks=all and calls it durable. What do you check?"
**Model answer.** `acks=all` waits only for the *current* in-sync replica set. So I check
`min.insync.replicas`. If it is 1, the ISR can shrink to just the leader. Then a single failure
after the ack loses data. That is not durable.

For majority durability on `N` replicas, you want `min.insync.replicas = ⌊N/2⌋ + 1` (for example,
2 for RF=3). Then an ack means the record is on a majority. The record survives the loss of a
minority of brokers. A SOX completeness control depends on exactly this kind of detail.

### 4. "Why does S3/Athena never lose an object you successfully wrote?"
**Model answer.** S3 stores each object redundantly across multiple devices and facilities. It
returns success on a `PUT` only when the object is durably stored on enough of them. This is a
quorum write. The ack is a promise of durable replication.

Athena only reads those objects, so it inherits that durability. Iceberg on top commits by an
atomic swap of one catalog pointer. This is a tiny single-winner consensus over table versions.
