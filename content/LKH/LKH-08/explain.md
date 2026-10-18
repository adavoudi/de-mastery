# LKH-08 · Explain it back (Feynman)

**Prompt A.** "You ran the same `WHERE region = 'r1' AND amt > 50 GROUP BY cat` on Spark and DuckDB
over the same Iceberg table and they agree. Walk me through how you'd read Spark's plan to prove the
engine pruned the partition and pushed the amount filter — and what you'd see if it didn't."

**Prompt B.** "Explain to a BI analyst why the same query is snappy on Athena/Trino but you'd use
Spark for the nightly ETL and DuckDB for a laptop ad-hoc — same table, three engines."

## Rubric — a strong explanation hits these (5–7):

- [ ] Distinguishes **partition pruning** (skip whole partitions via a partition-column predicate,
      first & coarse) from **predicate pushdown** (push the predicate into the scan; file/row-group
      skipping via stats).
- [ ] Says how to *verify* it in a Spark plan: the partition predicate and the data predicate both
      appear in the Iceberg `filters=` on the `BatchScan` (or `PartitionFilters` / `PushedFilters` on
      a file source) — not as a late `Filter` node over a full scan.
- [ ] Explains that two correct engines on the **same committed snapshot** must agree, so a second
      engine (DuckDB) is a valid oracle; a disagreement is a query-logic bug.
- [ ] Explains **sort order / file min-max** makes pushdown skip more files (ties back to LKH-05).
- [ ] Names engine fit: Trino/Athena for interactive BI, Spark for heavy ETL/streaming, DuckDB for
      single-node/local, by workload not folklore.
- [ ] Mentions **statistics / Puffin** feeding cost-based join planning (and the Athena
      bytes-scanned cost motive for pruning).
- [ ] (Bonus) Connects to Redshift `EXPLAIN` / `ANALYZE` / SORTKEY zone maps.
