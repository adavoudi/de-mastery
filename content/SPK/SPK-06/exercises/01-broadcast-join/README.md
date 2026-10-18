# SPK-06 / 01 · Broadcast the small dimension

Enrich every order with its **parent** company name. `orders` is the large fact. The dimension is
the set of **parent** companies only — those not acquired by anyone (`acquired_by IS NULL`). The
acquired company's orders have no row in this dimension, so they must survive the join with a NULL
name. The right strategy is a **broadcast hash join**: ship the tiny dimension to every executor so
the fact streams through with no shuffle — exactly like Redshift `DISTSTYLE ALL` on a small
dimension.

The test deliberately **disables auto-broadcast** (`autoBroadcastJoinThreshold = -1`) so you must
request the broadcast explicitly, which makes the choice verifiable in the plan.

## Task

Implement `enrich_orders(orders, companies)` which:

1. restricts `companies` to parent companies (`acquired_by IS NULL`),
2. **left**-joins `orders` to that dimension on `company_id` (keep every order), and
3. returns `order_id`, `company_id`, `name` (the parent company name, NULL for acquired-company
   orders).

## Constraints

- Explicitly broadcast the **small** side: `F.broadcast(...)`.
- Use a **left** join so acquired-company orders are not dropped.

## How it is graded

- **Data (DuckDB oracle):** equals `orders LEFT JOIN (companies WHERE acquired_by IS NULL)` on
  `company_id`, projected to the three columns (acquired-company orders have `name = NULL`).
- **Plan:** `BroadcastHashJoin` present and `SortMergeJoin` absent.
- Row/column order are not checked.
