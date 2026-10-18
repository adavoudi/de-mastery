# Explain it (Feynman)

Explain to an analyst who sees a big Athena bill:

> "Why does the same query cost far more on CSV than on Parquet, and how does a `WHERE` on a sorted
> column make Athena read only a fraction of the file?"

Walk through row groups, column chunks, and footer min/max stats.

## Rubric (self-check — aim for 5/6)

- [ ] Contrasts **row vs columnar** access patterns and why columnar reads fewer bytes.
- [ ] Names Parquet's layout: **row groups → column chunks → pages**, with stats in the **footer**.
- [ ] Explains **row-group pruning** using per-group `min/max` (skip when the range can't match).
- [ ] States that pruning needs **sorting/clustering**; random order prunes nothing.
- [ ] Connects to **Athena bytes-scanned billing** and column projection as the top cost lever.
- [ ] Mentions Iceberg manifest **column metrics / sort order** doing file-level pruning.
