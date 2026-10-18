# Fluss PK table + lake tiering — streaming and batch reads must match

A Fluss primary-key table tiers its data down into a lake table (Paimon). The streamhouse promise is
**one copy, two latencies, the same answer**: once tiering has quiesced, a **streaming** read and a
**batch** read of the tiered data must return the **same set of rows**. The lake tier Fluss writes
into *is* a Paimon PK table, so in this exercise you author that tiered table and prove the
read-consistency invariant on it directly — no Docker.

Implement, in `solution.py`:

```python
def tier_table_ddl(table: str) -> str:
    """CREATE TABLE for the tiered lake table `table` with columns
    (id INT, bal INT), configured so a streaming read and a batch read return
    the SAME final state (latest value per id)."""

def upsert_inserts(table: str, upserts) -> list[str]:
    """INSERT statements upserting each (id, bal) in `upserts` into `table`,
    in order. Later writes for the same id supersede earlier ones."""
```

`upserts` is a list of `(id, bal)` tuples applied in order; the final state of the table is the
**last `bal` per `id`**.

### Requirements

- The table must declare **`PRIMARY KEY(id) NOT ENFORCED`** and **`'bucket'='1'`** so the layout is
  deterministic and the latest write per key wins.
- It must be configured so a **bounded streaming scan** and a **batch scan** return the identical
  final state — i.e. the table collapses multiple writes per key (the `deduplicate` merge engine)
  and defines a changelog (`'changelog-producer'='input'`) so the streaming scan is well defined.
- An append-only table (no primary key) would keep every version, so a streaming scan would show
  rows a batch scan has collapsed — that is exactly the inconsistency to avoid.

### What the tests do (G6 oracle)

1. **Read consistency (always runs, embedded Paimon — the lake-tier format):** your DDL + upserts
   are executed, then read once in **batch** and once as a **bounded streaming scan** (with a
   deadline so it never hangs). The two results are compared to each other and to an **independent
   Python oracle** (last-value-per-id) with `sth_oracle` (order-insensitive multiset equality).
2. **Native Fluss path (SKIPs if the vendored Fluss server is absent):** a real Fluss local cluster
   (ZooKeeper + coordinator + tablet server) is started on free ports and torn down, proving the
   streaming-store half of the tier exists on a bare, no-Docker machine.

If everything can only skip, run `bash labs/fetch_streamhouse.sh` (and `bash labs/fetch_flink.sh`) —
the fixtures print the exact command.
