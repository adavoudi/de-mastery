# Enrich orders and route rejects to a side output

This exercise is a dbt staging model written again as a continuous DataStream job. The job filters
orders to a valid-currency allowlist and enriches them with a dimension lookup. It sends the rows
that fail validation to a **dead-letter** stream. All of this happens in one pass.

## Task

Implement `de.flk.EnrichOrders.run(List<String[]> orders, Map<String,String> companyNames)`.

Each order is `[orderId, companyId, currency, amount]`. Build a DataStream job
that does this in a single pass:

- **Valid orders** (currency in `VALID` = {USD, EUR, GBP, JPY}): emit them to the main
  stream, enriched with the company name. Render them as
  `orderId|companyName|currency|amount`. If the company id is not in
  `companyNames`, use `"UNKNOWN"` as the name. Do **not** drop or reject the order.
- **Invalid orders** (currency not in the allowlist): route them to a **side output**
  (the dead-letter stream). Render them as `orderId|companyId|currency`.

Return a `Result` that holds the enriched list and the rejects list.

## What the tests check

- The enriched set equals the enriched set of an independent oracle. The test compares sets, so the
  order of the subtasks does not matter.
- The rejects set equals the rejects of the oracle. Lowercase `usd` is **not** USD.
- No order is lost or duplicated. Every input id appears exactly once across the
  two streams.
- A valid order with an unknown company is enriched as `UNKNOWN`. It is never rejected.

## Hints

- Use a `ProcessFunction<String[], String>`. In `processElement`, call `out.collect(..)`
  for valid orders and `ctx.output(rejectsTag, ..)` for invalid orders.
- Declare the tag as an anonymous subclass: `new OutputTag<String>("rejects"){}`.
- Read the side stream with `mainOperator.getSideOutput(rejectsTag)`. Collect the
  main and side streams with `executeAndCollect()`.
