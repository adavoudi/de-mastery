# Fluss PK table + lake tiering — streaming and batch reads must match

A Fluss primary-key table tiers its data down into a lake table (Paimon). The streamhouse promise is
**one copy, two latencies, the same answer**. Tiering has quiesced. Then a **streaming** read and a
**batch** read of the tiered data must return the **same set of rows**. The lake tier that Fluss
writes into *is* a Paimon PK table. So in this exercise you author that tiered table. You prove the
read-consistency invariant on it directly, with no Docker.

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

`upserts` is a list of `(id, bal)` tuples that you apply in order. The final state of the table is
the **last `bal` per `id`**.

### Requirements

- The table must declare **`PRIMARY KEY(id) NOT ENFORCED`** and **`'bucket'='1'`**. Then the layout
  is deterministic and the latest write per key wins.
- You must configure the table so a **bounded streaming scan** and a **batch scan** return the
  identical final state. To do this:
  - The table collapses multiple writes per key (the `deduplicate` merge engine).
  - The table defines a changelog (`'changelog-producer'='input'`). Then the streaming scan is well
    defined.
- An append-only table (no primary key) would keep every version. A streaming scan would then show
  rows that a batch scan has collapsed. This is exactly the inconsistency to avoid.

### What the tests do (G6 oracle)

1. **Read consistency (always runs, embedded Paimon — the lake-tier format):** The test runs your
   DDL and upserts. Then it reads the table once in **batch** and once as a **bounded streaming
   scan** (with a deadline, so it never hangs). The test compares the two results to each other. It
   also compares them to an **independent Python oracle** (last value per id) with `sth_oracle`
   (order-insensitive multiset equality).
2. **Native Fluss path (SKIPs if the vendored Fluss server is absent):** The test starts a real
   Fluss local cluster (ZooKeeper + coordinator + tablet server) on free ports and then stops it.
   This proves that the streaming-store half of the tier exists on a bare machine with no Docker.

Everything may only skip. Then run `bash labs/fetch_streamhouse.sh` (and `bash labs/fetch_flink.sh`).
The fixtures print the exact command.
