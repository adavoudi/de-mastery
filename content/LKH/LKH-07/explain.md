# LKH-07 · Explain it back (Feynman)

**Prompt A.** "Three teams ask you which table format to use:

1. A Databricks shop with Unity Catalog.
2. A vendor-neutral lake that Trino, Athena and DuckDB query, and that will re-partition later.
3. A CDC pipeline that does frequent primary-key upserts and needs 'what changed since T'.

Give each team an answer. Give the one or two factors that decide it."

**Prompt B.** "A manager says 'just pick whatever the latest benchmark won.' Explain why this is
the wrong approach. Explain how UniForm/XTable change the stakes of the decision."

## Rubric — a strong explanation hits these (5–7):

- [ ] Databricks, Unity and liquid-clustering case → **Delta**. The reason is native Databricks
      support.
- [ ] Vendor-neutral, many-engine, evolve-layout case → **Iceberg**. The reasons are engine
      neutrality and hidden partitioning / partition evolution.
- [ ] Upsert-heavy, CDC and incremental-pull case → **Hudi**. The reasons are record-level indexing
      and the timeline / incremental queries.
- [ ] States that for most **batch** work Iceberg and Delta have comparable features. So **ecosystem
      fit** decides, not benchmarks.
- [ ] Explains that **interop** (UniForm / XTable) lets you read one data copy as multiple formats.
      So the choice is less of a lock-in.
- [ ] Names the Hudi anti-pattern (append-only analytics does not need its upsert and index
      machinery).
- [ ] (Bonus) Mentions CoW vs MoR as a write/read trade-off that all formats share. Names Paimon as
      the streaming-first option.
