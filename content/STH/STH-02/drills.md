# STH-02 · Interview drills

**1. "What is a Paimon primary-key table and how does it differ from a plain Iceberg table?"**
> A PK table declares `PRIMARY KEY(...) NOT ENFORCED` and stores rows in an LSM tree, merging rows
> that share a key via a merge engine at compaction/read time. A plain Iceberg table is
> append-and-snapshot — to update a row you rewrite files or stack delete files. The PK table gives
> cheap in-place upserts and a streaming changelog; Iceberg is better for immutable facts.

**2. "You are assembling a customer golden record from CRM, billing and loyalty feeds. Which merge
engine, and why?"**
> `partial-update`. Each source writes a row keyed by `customer_id` with only its own column filled
> and the rest NULL; `partial-update` keeps the latest **non-null** value per column, so the three
> partial rows combine into one complete record. The default `deduplicate` would keep only the last
> whole record and overwrite the other systems' columns with NULLs — data loss.

**3. "Does a later write of NULL erase a value set by an earlier write under partial-update?"**
> No. `partial-update` keeps the latest **non-null** value per column, so a NULL from a later writer
> is ignored for that column. Erasing a value on a retract/delete requires `sequence.field` plus
> `partial-update.remove-record-on-delete`, which is a separate, opt-in behaviour.

**4. "Why pin `'bucket'='1'` on a small table?"**
> With a fixed bucket count the physical layout is deterministic and reproducible, which matters for
> tests and small reference tables. Without it the bucket count is dynamic and the file layout (and
> sometimes merge timing) is not reproducible run to run.

**5. "What are the four merge engines and when do you use each?"**
> `deduplicate` (default) keeps the last whole record — a full-row upsert. `partial-update` keeps
> the latest non-null value per column — golden records assembled from partial sources.
> `aggregation` combines columns with per-column aggregate functions (sum, max) — running totals.
> `first-row` keeps the first record for a key and ignores later ones — dedup-on-arrival.

**6. "How does this relate to your dbt MDM pipeline?"**
> It is the same coalesce/precedence logic, moved from a nightly dbt golden-record model into the
> storage layer and run continuously. Primary key = business key, bucket = physical layout, merge
> engine = the COALESCE/precedence policy. The SOX risk is identical: one source silently
> overwriting another's attributes.
