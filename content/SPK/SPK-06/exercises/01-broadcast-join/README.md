# SPK-06 / 01 · Broadcast the small dimension

Add the name of the **parent** company to every order. `orders` is the large fact. The dimension
contains only the **parent** companies. These are the companies that no one acquired
(`acquired_by IS NULL`). The orders of the acquired company have no row in this dimension. They
must survive the join with a NULL name. The right strategy is a **broadcast hash join**. Spark
sends the tiny dimension to every executor, and the fact streams through with no shuffle. This
is exactly like Redshift `DISTSTYLE ALL` on a small dimension.

The test deliberately **disables auto-broadcast** (`autoBroadcastJoinThreshold = -1`). So you
must request the broadcast explicitly. Then you can verify the choice in the plan.

## Task

Implement `enrich_orders(orders, companies)`. It must do these steps:

1. Restrict `companies` to parent companies (`acquired_by IS NULL`).
2. **Left**-join `orders` to that dimension on `company_id` (keep every order).
3. Return `order_id`, `company_id`, `name` (the parent company name, NULL for orders of the
   acquired company).

## Constraints

- Explicitly broadcast the **small** side: `F.broadcast(...)`.
- Use a **left** join, so that you do not drop the orders of the acquired company.

## How it is graded

- **Data (DuckDB oracle):** equals `orders LEFT JOIN (companies WHERE acquired_by IS NULL)` on
  `company_id`, projected to the three columns (orders of the acquired company have
  `name = NULL`).
- **Plan:** `BroadcastHashJoin` is present and `SortMergeJoin` is absent.
- The test does not check row order or column order.
