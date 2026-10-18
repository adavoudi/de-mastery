# KFK-02 · Topics, partitions, replication — leaders, ISR, acks, durability

## Why replication exists

A single log on a single disk can lose data if one power supply fails. SOX asks you to prove
**completeness**. This means a write that your system acknowledged must still exist after a
failure. Kafka answers with **replication**. Each partition has `replication.factor` copies on
different brokers. One copy is the **leader**. The other copies are **followers**. Clients talk
only to the leader. Followers pull from the leader to stay in sync. If the broker of the leader
dies, Kafka promotes a follower. This whole module is about one question. *With which settings does
an acknowledged write survive a broker failure? With which settings do you only think that it
survives?*

## How durability is configured

Three settings decide the guarantee. You must think about them **together**, not one at a time.

- **`replication.factor` (RF).** The number of copies. RF=3 means that three brokers each hold the
  partition. RF=1 means one copy. If you lose that one broker, you lose the data of that partition
  permanently.
- **ISR — the in-sync replica set.** The ISR is the set of replicas that are caught up with the
  leader now (within `replica.lag.time.max.ms`). A follower drops out of the ISR if it falls behind
  or if its broker dies. It rejoins when it catches up. The **leader is always in its own ISR**.
  The ISR shrinks and grows when brokers fail and recover.
- **`acks` (producer).** The number of replicas that must confirm a write before the producer
  treats it as done:
  - `acks=0` — fire and forget. The producer never waits. Kafka can lose a record even when no
    failure happens (for example, when the leader has not received it yet). This setting has the
    highest throughput and the weakest guarantee.
  - `acks=1` — the **leader** has written the record. The record survives only if the leader stays
    up. If the leader acks and then crashes before a follower copies the record, Kafka **loses**
    the record.
  - `acks=all` (also written `-1`) — **all replicas in the ISR** have written the record. This is
    the durable setting. But it is only as strong as the ISR is large.
- **`min.insync.replicas` (misr, a topic or broker config).** The minimum ISR size for the leader
  to *accept* an `acks=all` write. If the ISR has fewer than `misr` members, the leader **rejects**
  the produce request with `NOT_ENOUGH_REPLICAS`. It does not acknowledge a write that is not
  durable enough. `misr` has an effect only with `acks=all`.

The standard durable config is **RF=3, acks=all, min.insync.replicas=2**. It tolerates one broker
down. In that case the ISR shrinks from 3 to 2, which is still ≥ misr. Writes continue, and they
are on at least 2 brokers. If a second broker is lost, the ISR goes from 2 to 1, which is < misr.
Then Kafka refuses writes. So you never acknowledge a write that lives on only one disk. `misr=2`
with RF=3 is the best balance. You keep availability under one failure and safety under two.

## The decision table you must learn

For a produce to a partition, this table uses the number of replicas that are in the ISR now:

| acks | min.insync.replicas | ISR size | Accepted? | If accepted, durable against 1 more broker loss? |
|---|---|---|---|---|
| 0 | any | ≥1 | yes (not even leader-confirmed) | no — may be lost with no failure at all |
| 1 | any | ≥1 | yes (leader only) | **no** — leader crash after ack loses it |
| all | 1 | ≥1 | yes | no — only the leader may hold it |
| all | 2 | ≥2 | **yes** | **yes** — on ≥2 brokers |
| all | 2 | 1 | **no** (`NOT_ENOUGH_REPLICAS`) | — rejected, nothing lost |
| all | 3 | 2 | no | — rejected |

In the exercise, you write code for exactly this table. Then you watch a real cluster behave in the
same way.

## Leaders, elections, and unclean election

When the broker of a leader dies, the controller elects a new leader **from the ISR**. A replica in
the ISR is known to be caught up, so Kafka loses no acknowledged data. You can enable **unclean
leader election** (`unclean.leader.election.enable=true`). Then the controller may elect a replica
that was *not* in the ISR, when every in-sync replica is gone. This restores availability. But it
**silently loses** the records that only the dead in-sync replicas had. The default is off. For
SOX-style completeness, keep it off. Accept unavailability instead of silent loss. **KRaft**
(Kafka 4.x, no ZooKeeper) runs the controller quorum that does these elections. The controller
metadata log is also replicated.

![An acks=all write reaches every ISR member; when the leader dies, an ISR follower is elected and the acknowledged record survives](diagrams/isr-failover.svg)

## From your world

| Lakehouse / SOX | Kafka durability | Note |
|---|---|---|
| "a committed row must survive a node failure" | `acks=all` + `misr=2` + RF=3 | the completeness guarantee |
| A write that "succeeded" but vanished | `acks=1` + leader crash | the silent-loss trap you must avoid |
| Refusing to commit when you can't guarantee durability | `NOT_ENOUGH_REPLICAS` under misr | fail loud, not silent |
| Choosing availability over correctness | unclean leader election | usually the WRONG trade for finance data |

## When NOT to set durability to the maximum

- **Metrics / logs / lossy telemetry.** Sometimes it is fine to drop an occasional record. Then
  `acks=1` (or even `0`) and RF=2 give more throughput and cost less. Do not pay for `acks=all` if
  you do not need it.
- **A dev/single-broker cluster.** RF must be 1, because there is only one broker. `misr=2` on RF=1
  makes every `acks=all` write fail. Set misr to match RF.
- **Latency-critical paths that you can replay from the source.** Sometimes the true source of
  record is elsewhere and you can replay it. Then you may accept weaker acks and rebuild the data
  after a loss.

## Further reading

Official docs only:

- Replication & ISR:
  <https://kafka.apache.org/documentation/#replication>
- Producer `acks`:
  <https://kafka.apache.org/documentation/#producerconfigs_acks>
- `min.insync.replicas` (topic config):
  <https://kafka.apache.org/documentation/#topicconfigs_min.insync.replicas>
- Unclean leader election:
  <https://kafka.apache.org/documentation/#brokerconfigs_unclean.leader.election.enable>
- KRaft overview:
  <https://kafka.apache.org/documentation/#kraft>
