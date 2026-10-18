# FND-03 · Replication & consensus

## Why it exists

A single machine will fail — disk dies, process crashes, network partitions it off. If the data
only existed there, it is gone. **Replication** is the answer: keep copies on several machines so
the system survives the loss of some of them. But the moment there is more than one copy, a new
problem appears: **which copy is the truth?** If two replicas accept different writes, you have
*split brain* and silent corruption. **Consensus** is the set of rules that lets a group of
machines agree on a single, ordered history of events even while some of them fail or lag.

Kafka's ISR, ZooKeeper/KRaft, Flink's checkpointing coordinator, and the catalog behind your
lakehouse all stand on this. Learn the shape of consensus once and these stop being magic.

## How it works

**Leader/follower replication.** One replica is the **leader**; it orders all writes. Followers
copy the leader's log in order. Reads can come from the leader (always fresh) or followers
(possibly stale). If the leader dies, a follower must be promoted — and *only one*, or you get two
leaders.

**Quorums.** With `N` replicas, require writes to be acknowledged by `W` of them and reads to
consult `R` of them. If `W + R > N`, every read overlaps at least one replica that saw the latest
write, so you can always find the newest value. A common safe choice is `W = R = ⌊N/2⌋ + 1` (a
**majority**). Majorities are special because any two majorities of the same set must share at
least one member — that overlap is what prevents two conflicting decisions.

**Kafka's ISR (in-sync replicas).** Kafka makes this concrete: a partition has a leader and
followers; the **ISR** is the set of replicas currently caught up. With `acks=all` and
`min.insync.replicas = M`, a produce is acknowledged only once `M` in-sync replicas have it. Set
`M = ⌊N/2⌋ + 1` and you have a majority quorum: you can lose a minority of brokers without losing
acknowledged data.

**Raft (consensus you can whiteboard).** Raft elects a leader by **terms**. Time is divided into
numbered terms; each term has at most one leader. A node that hears nothing from a leader becomes a
*candidate*, increments the term, votes for itself, and requests votes. A node grants its vote in a
term **only once**, and only to a candidate whose log is at least as up to date as its own. A
candidate that collects a **majority** of votes becomes leader for that term. The two rules —
one-vote-per-term and majority-to-win — guarantee **at most one leader per term**, because two
different winners would each need a majority, and two majorities must overlap on a voter who
illegally voted twice. That is the whole safety argument for leader election, and it is what you
will implement.

## From your world

You already trust this every day. **Amazon S3** stores each object redundantly across multiple
devices and facilities and only acknowledges a `PUT` once it is durably stored on enough of them —
a quorum write. That is why **Athena never "loses" an S3 object**: the durability you rely on is
replication + quorum acknowledgement, exactly the mechanism here. Your Iceberg tables inherit it,
because the data and metadata files live in S3, and the atomic commit swaps a single catalog
pointer — one ordered log of table versions, which is itself a tiny consensus problem (only one
commit can win a given version).

When you reason about a SOX control like "no acknowledged transaction is ever lost," you are
reasoning about `W` and the ISR: an ack must mean *durably replicated to a majority*, or the
control is a lie.

## When NOT to use it

- **Don't require consensus for everything.** It costs a network round trip to a majority on every
  write. For data that can be re-derived or that tolerates eventual consistency, cheaper
  replication (async, read-your-writes) may be fine.
- **Don't set `min.insync.replicas = 1` and claim durability.** With `acks=all` but `M = 1`, a
  single broker failure after ack loses data — the ISR provides no quorum.
- **Don't allow an even split to proceed.** Quorums need a strict majority; a 2-of-4 "tie" must not
  commit, or two partitions could each think they won.
- **Don't read from a follower when you need strong consistency** — followers lag; use the leader
  or a quorum read (`W + R > N`).

## Further reading

- Ongaro, D. & Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm (Raft).*
  https://raft.github.io/raft.pdf
- Lamport, L. (1998). *The Part-Time Parliament (Paxos).*
  https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf
- Apache Kafka Documentation — *Replication and in-sync replicas (ISR), `acks`,
  `min.insync.replicas`.* https://kafka.apache.org/documentation/#replication
- Amazon S3 — *Data protection and durability.*
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html
