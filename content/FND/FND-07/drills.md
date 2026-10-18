# Interview drills — FND-07

### 1. "Size a pipeline for 5 PB/year. Where do you start?"
**Model answer.** I start from throughput and work up. Suppose it is 2M events/sec at ~100 bytes
each. That is 2e8 B/s = 200 MB/s raw. Over a year (~3.15e7 s) that is ~6.3 PB raw.

Then I apply the two assumptions that really move the number:

- Columnar compression (~3× down → ~2.1 PB).
- Replication for durability (~3× up → ~6.3 PB on disk).

So "5 PB/year" is the right ballpark. I would say out loud: "assuming 100 B/event, 3× compression,
3× replication." If you change any of these, the number changes. That is the reason to state
them.

### 2. "A stakeholder asks what a query will cost on Athena. How do you answer?"
**Model answer.** Athena bills per TB scanned. So cost = `$/TB-scanned × TB_scanned`. The price is
roughly $5/TB at the time of writing.

The lever is `TB_scanned`:

- Store Parquet and select only the needed columns. This cuts `TB_scanned` (column pruning).
- Use partitioning and sort order. Then the engine skips row groups and files for selective
  filters (FND-06).

I would estimate the scanned bytes from the columns and partitions that the query touches, not
from the full table. I would give a range and state the assumption.

### 3. "How much to retain this feed for 7 years?"
**Model answer.** This is storage cost, not query cost: `GB_stored × $/GB-month × months`. Take
the stored bytes/year (raw × compression × replication). Convert them to GB. Multiply by 7 × 12
months and by the $/GB-month rate.

I would flag that the compression and replication assumptions dominate. I would also flag that
lifecycle tiering can cut the cost of the later years substantially. Lifecycle tiering moves old
data to colder, cheaper storage. It is worth mentioning for a 7-year SOX retention.

### 4. "Why not just load-test instead of estimating?"
**Model answer.** The estimate decides whether the design is worth building and load testing. A
sixty-second envelope tells me these things:

- Is this a one-box DuckDB job or a cluster?
- Is the Athena bill $100 or $100k?
- Does it fit in RAM?

Load testing is expensive. It comes *after* the envelope narrows the options. The job of the
estimate is the right order of magnitude with assumptions that people can challenge. Load testing
then refines the design that remains.
