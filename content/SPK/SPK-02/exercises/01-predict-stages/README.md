# Predict the stage count for six queries

You will *predict*, on paper, how many **stages** each of six queries runs. Then the test does
these steps:

1. It builds each query.
2. It reads the **actual** physical plan.
3. It counts the real `Exchange` (shuffle) nodes.
4. It checks your prediction against the result.

The answer key is the running engine, not a hand-written number. If Spark disagrees with you,
you were wrong (or you found a surprising optimization).

You are calibrating this rule: for a linear job, **stages = shuffles + 1**. Narrow
transformations (`filter`, `select`, `withColumn`) add no shuffle. Wide transformations
(`groupBy`, `distinct`, `orderBy`, `repartition`, and non-broadcast `join`) each add a shuffle.
**The tests disable broadcast joins**, so every `join` shuffles *both* input sides.

Implement one function in `solution.py`:

### `predicted_stages() -> dict[str, int]`
Return a dict. It maps each query id below to the number of stages you predict. Use exactly
these six ids as keys:

| id | query (on the commerce `orders` / `order_items` tables) |
|---|---|
| `q1_filter_select` | `orders.filter(currency = 'USD').select(order_id, currency)` |
| `q2_group_count` | `orders.groupBy(currency).count()` |
| `q3_group_then_sort` | `orders.groupBy(currency).count().orderBy(currency)` |
| `q4_distinct` | `orders.select(currency).distinct()` |
| `q5_join` | `orders.join(order_items, "order_id")` — broadcast disabled |
| `q6_join_then_group` | `orders.join(order_items, "order_id").groupBy(order_items.company_id).count()` — broadcast disabled |

Each value must be a positive integer. You do not run Spark yourself. You reason about the plan.

Hint on method: count the wide transformations on the path to the result, then add 1. A
`distinct` is a shuffle. A join with broadcast disabled is two shuffles (one for each side). A
`groupBy` after it is one more.
