# Interview drills — FND-07

### 1. "Size a pipeline for 5 PB/year. Where do you start?"
**Model answer.** I start from throughput and work up. If it's 2M events/sec at ~100 bytes each,
that's 2e8 B/s = 200 MB/s raw. Over a year (~3.15e7 s) that's ~6.3 PB raw. Then I apply the two
assumptions that actually move the number: columnar compression (~3× down → ~2.1 PB) and
replication for durability (~3× up → ~6.3 PB on disk). So "5 PB/year" is the right ballpark, and I'd
say out loud: "assuming 100 B/event, 3× compression, 3× replication." Change any of those and the
number changes — that's the point of stating them.

### 2. "A stakeholder asks what a query will cost on Athena. How do you answer?"
**Model answer.** Athena bills per TB scanned, so cost = `$/TB-scanned × TB_scanned`, roughly $5/TB
at the time of writing. The lever is `TB_scanned`: storing Parquet and selecting only needed columns
cuts it (column pruning), and partitioning + sort order lets the engine skip row groups/files for
selective filters (FND-06). I'd estimate scanned bytes from the columns and partitions the query
touches, not the full table, and give a range with the assumption stated.

### 3. "How much to retain this feed for 7 years?"
**Model answer.** Storage cost, not query cost: `GB_stored × $/GB-month × months`. Take the stored
bytes/year (raw × compression × replication), convert to GB, multiply by 7 × 12 months and the
$/GB-month rate. I'd flag that compression and replication assumptions dominate, and that lifecycle
tiering (moving old data to colder, cheaper storage) can cut the later years substantially — worth
mentioning for a 7-year SOX retention.

### 4. "Why not just load-test instead of estimating?"
**Model answer.** Because the estimate decides whether the design is even worth building and load
testing. A sixty-second envelope tells me if this is a one-box DuckDB job or a cluster, whether the
Athena bill is $100 or $100k, whether it fits in RAM. Load testing is expensive and comes *after*
the envelope narrows the options. The estimate's job is the right order of magnitude with
challengeable assumptions; load testing refines the survivor.
