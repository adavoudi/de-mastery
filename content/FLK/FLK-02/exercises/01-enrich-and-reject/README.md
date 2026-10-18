# Enrich orders and route rejects to a side output

This is a dbt staging model, re-cast as a continuous DataStream job: filter to a
valid-currency allowlist, enrich with a dimension lookup, and send the rows that
fail validation to a **dead-letter** stream — all in one pass.

## Task

Implement `de.flk.EnrichOrders.run(List<String[]> orders, Map<String,String> companyNames)`.

Each order is `[orderId, companyId, currency, amount]`. Build a DataStream job
that, in a single pass:

- **Valid orders** (currency in `VALID` = {USD, EUR, GBP, JPY}): emit to the main
  stream, enriched with the company name. Render as
  `orderId|companyName|currency|amount`. If the company id is not in
  `companyNames`, use `"UNKNOWN"` as the name (do **not** drop or reject it).
- **Invalid orders** (currency not in the allowlist): route to a **side output**
  (the dead-letter stream). Render as `orderId|companyId|currency`.

Return a `Result` holding the enriched list and the rejects list.

## What the tests check

- The enriched set equals an independent oracle's enriched set (compared as sets,
  so subtask ordering doesn't matter).
- The rejects set equals the oracle's rejects (lowercase `usd` is **not** USD).
- No order is lost or duplicated: every input id appears exactly once across the
  two streams.
- A valid order with an unknown company is enriched as `UNKNOWN`, never rejected.

## Hints

- Use a `ProcessFunction<String[], String>`. In `processElement`, `out.collect(..)`
  for valid orders and `ctx.output(rejectsTag, ..)` for invalid ones.
- Declare the tag as an anonymous subclass: `new OutputTag<String>("rejects"){}`.
- Read the side stream with `mainOperator.getSideOutput(rejectsTag)`. Collect the
  main and side streams with `executeAndCollect()`.
