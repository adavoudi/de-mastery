# STH-02 · Interview drills

**1. "What is a Paimon primary-key table and how does it differ from a plain Iceberg table?"**
> A PK table declares `PRIMARY KEY(...) NOT ENFORCED`. It stores rows in an LSM tree. A merge engine
> merges rows that share a key, at compaction time or at read time. A plain Iceberg table is
> append-and-snapshot. To update a row, you rewrite files or stack delete files. The PK table gives
> cheap in-place upserts and a streaming changelog. Iceberg is better for immutable facts.

**2. "You are assembling a customer golden record from CRM, billing and loyalty feeds. Which merge
engine, and why?"**
> `partial-update`. Each source writes a row keyed by `customer_id`. It fills only its own column
> and leaves the rest NULL. `partial-update` keeps the latest **non-null** value per column. So the
> three partial rows combine into one complete record. The default `deduplicate` would keep only the
> last whole record. It would overwrite the columns of the other systems with NULLs. This is data
> loss.

**3. "Does a later write of NULL erase a value set by an earlier write under partial-update?"**
> No. `partial-update` keeps the latest **non-null** value per column. So Paimon ignores a NULL from
> a later writer for that column. To erase a value on a retract or delete, you need `sequence.field`
> plus `partial-update.remove-record-on-delete`. That is a separate behaviour that you must turn on.

**4. "Why pin `'bucket'='1'` on a small table?"**
> A fixed bucket count makes the physical layout deterministic and reproducible. This matters for
> tests and small reference tables. Without it, the bucket count is dynamic. The file layout (and
> sometimes the merge timing) is then not reproducible from one run to the next.

**5. "What are the four merge engines and when do you use each?"**
> - `deduplicate` (default) keeps the last whole record. Use it for a full-row upsert.
> - `partial-update` keeps the latest non-null value per column. Use it for golden records that
>   you assemble from partial sources.
> - `aggregation` combines columns with per-column aggregate functions (sum, max). Use it for
>   running totals.
> - `first-row` keeps the first record for a key and ignores later ones. Use it for
>   dedup-on-arrival.

**6. "How does this relate to your dbt MDM pipeline?"**
> It is the same coalesce/precedence logic. You move it from a nightly dbt golden-record model into
> the storage layer, and it runs continuously. Primary key = business key. Bucket = physical layout.
> Merge engine = the COALESCE/precedence policy. The SOX risk is identical: one source silently
> overwrites the attributes of another source.
