# LKH-08 · Explain it back (Feynman)

**Prompt A.** "You ran the same `WHERE region = 'r1' AND amt > 50 GROUP BY cat` on Spark and DuckDB
over the same Iceberg table. The results agree. Explain how you would read the plan of Spark to
prove two facts. The engine pruned the partition. The engine pushed the amount filter down. Also
explain what you would see if it did not."

**Prompt B.** "Explain to a BI analyst why the same query is fast on Athena/Trino, but you would
use Spark for the nightly ETL and DuckDB for an ad-hoc query on a laptop. It is the same table
with three engines."

## Rubric — a strong explanation hits these (5–7):

- [ ] Distinguishes **partition pruning** from **predicate pushdown**. Pruning skips whole
      partitions with a partition-column predicate (it comes first and is coarse). Pushdown pushes
      the predicate into the scan (file and row-group skipping with stats).
- [ ] Says how to *verify* it in a Spark plan: the partition predicate and the data predicate both
      appear in the Iceberg `filters=` on the `BatchScan` (or `PartitionFilters` / `PushedFilters`
      on a file source). They do not appear as a late `Filter` node over a full scan.
- [ ] Explains that two correct engines on the **same committed snapshot** must agree. So a second
      engine (DuckDB) is a valid oracle. A disagreement is a bug in the query logic.
- [ ] Explains that **sort order and file min-max** make pushdown skip more files (this connects to
      LKH-05).
- [ ] Names engine fit: Trino/Athena for interactive BI, Spark for heavy ETL and streaming, DuckDB
      for single-node and local work. The choice depends on the workload, not on folklore.
- [ ] Mentions **statistics / Puffin** that feed cost-based join planning (and the Athena
      bytes-scanned cost, which is a reason for pruning).
- [ ] (Bonus) Connects to Redshift `EXPLAIN`, `ANALYZE` and SORTKEY zone maps.
