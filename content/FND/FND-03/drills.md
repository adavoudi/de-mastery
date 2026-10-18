# Interview drills — FND-03

### 1. "Explain how a quorum guarantees you read the latest write."
**Model answer.** With `N` replicas, require `W` acks on write and `R` replicas consulted on read.
If `W + R > N`, the write set and the read set must overlap on at least one replica, and that
replica holds the newest value, so the read can find it (take the one with the highest version).
A majority on both sides (`W = R = ⌊N/2⌋ + 1`) is the common safe choice because any two majorities
of the same set intersect.

### 2. "How does Raft guarantee a single leader per term?"
**Model answer.** Two rules. First, each node votes at most once per term. Second, a candidate must
collect a majority of votes to become leader. If two candidates both won the same term, each would
need a majority, and any two majorities of the same cluster share at least one node — that node
would have had to vote twice in one term, which the first rule forbids. So at most one leader per
term. Candidates also only get votes if their log is at least as up to date as the voter's, which
preserves committed entries.

### 3. "A teammate sets acks=all and calls it durable. What do you check?"
**Model answer.** `acks=all` only waits for the *current* in-sync replica set, so I check
`min.insync.replicas`. If it's 1, the ISR can shrink to just the leader and a single failure after
ack loses data — not durable. For majority durability on `N` replicas you want
`min.insync.replicas = ⌊N/2⌋ + 1` (e.g. 2 for RF=3), so an ack means the record is on a majority
and survives the loss of a minority of brokers. This is exactly the kind of thing a SOX
completeness control depends on.

### 4. "Why does S3/Athena never lose an object you successfully wrote?"
**Model answer.** S3 stores each object redundantly across multiple devices and facilities and only
returns success on a `PUT` once the object is durably stored on enough of them — a quorum write. The
ack is a promise of durable replication. Athena just reads those objects, so it inherits that
durability; and Iceberg on top commits by atomically swapping one catalog pointer, which is a tiny
single-winner consensus over table versions.
