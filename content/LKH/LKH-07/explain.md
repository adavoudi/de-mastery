# LKH-07 · Explain it back (Feynman)

**Prompt A.** "Three teams ask you which table format to use: (1) a Databricks shop with Unity
Catalog, (2) a vendor-neutral lake queried by Trino + Athena + DuckDB that will re-partition later,
(3) a CDC pipeline doing frequent primary-key upserts that needs 'what changed since T'. Give each
an answer and the one or two factors that decide it."

**Prompt B.** "A manager says 'just pick whatever the latest benchmark won.' Explain why that's the
wrong lens, and how UniForm/XTable change the stakes of the decision."

## Rubric — a strong explanation hits these (5–7):

- [ ] Databricks/Unity/liquid-clustering case → **Delta**, justified by native Databricks support.
- [ ] Vendor-neutral, many-engine, evolve-layout case → **Iceberg**, justified by engine neutrality +
      hidden partitioning / partition evolution.
- [ ] Upsert-heavy / CDC / incremental-pull case → **Hudi**, justified by record-level indexing +
      timeline / incremental queries.
- [ ] States that for most **batch** work Iceberg and Delta are feature-comparable, so **ecosystem
      fit** decides, not benchmarks.
- [ ] Explains **interop** (UniForm / XTable) lets one data copy be read as multiple formats, so the
      choice is less of a lock-in.
- [ ] Names the Hudi anti-pattern (append-only analytics doesn't need its upsert/index machinery).
- [ ] (Bonus) Mentions CoW vs MoR as a shared write/read trade-off across formats; names Paimon as the
      streaming-first option.
