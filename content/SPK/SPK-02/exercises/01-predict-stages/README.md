# Predict the stage count for six queries

You will *predict*, on paper, how many **stages** each of six queries runs. The test then builds
each query, reads its **actual** physical plan, counts the real `Exchange` (shuffle) nodes, and
checks your prediction against the truth. The answer key is the running engine, not a hand-written
number — if Spark disagrees with you, you were wrong (or you found a surprising optimization).

The rule you are calibrating: for a linear job, **stages = shuffles + 1**. Narrow transformations
(`filter`, `select`, `withColumn`) add no shuffle; wide ones (`groupBy`, `distinct`, `orderBy`,
`repartition`, and non-broadcast `join`) each add a shuffle. **The tests disable broadcast joins**,
so every `join` shuffles *both* input sides.

Implement one function in `solution.py`:

### `predicted_stages() -> dict[str, int]`
Return a dict mapping each query id below to the number of stages you predict. Use exactly these
six ids as keys:

| id | query (on the commerce `orders` / `order_items` tables) |
|---|---|
| `q1_filter_select` | `orders.filter(currency = 'USD').select(order_id, currency)` |
| `q2_group_count` | `orders.groupBy(currency).count()` |
| `q3_group_then_sort` | `orders.groupBy(currency).count().orderBy(currency)` |
| `q4_distinct` | `orders.select(currency).distinct()` |
| `q5_join` | `orders.join(order_items, "order_id")` — broadcast disabled |
| `q6_join_then_group` | `orders.join(order_items, "order_id").groupBy(order_items.company_id).count()` — broadcast disabled |

Each value must be a positive integer. You do not run Spark yourself; you reason about the plan.

Hint on method: count the wide transformations on the path to the result, add 1. A `distinct` is a
shuffle. A broadcast-disabled join is two shuffles (one per side). Then a following `groupBy` is
one more.
