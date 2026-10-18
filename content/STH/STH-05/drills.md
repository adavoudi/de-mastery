# STH-05 · Interview drills

**1. "How do you derive `valid_to` for an SCD2 dimension from a CDC changelog?"**
> Order the versions of each key by their business effective time and take `valid_to =
> LEAD(effective_from) OVER (PARTITION BY key ORDER BY effective_from)`. The newest version has no
> next row, so `valid_to` is left open (a far-future sentinel) and `is_current = true`. Each
> version's interval is `[valid_from, valid_to)`.

**2. "What is a point-in-time (as-of) join and why is it the correct enrichment for a fact?"**
> It joins each fact to the dimension version effective at the fact's event time: the row where
> `valid_from <= fact_ts < valid_to`. It is correct because a fact must be valued with the
> attributes that were true when it happened, not the current ones — the as-of rule.

**3. "Why is valuing an order at the customer's current tier a SOX defect, not a rounding error?"**
> It restates a historical fact. Reprocessing last quarter's orders with today's tier/address/FX
> silently changes reported numbers for closed periods, which violates accuracy and auditability
> controls. The SCD2 dimension exists precisely to pin each fact to its as-of version.

**4. "Streaming SCD2 vs nightly SCD2 MERGE — what's the same and what's different?"**
> The output is identical: the same versioned dimension with the same validity intervals. The input
> differs — a continuous CDC changelog instead of a nightly snapshot — and you must handle versions
> arriving over time (and possibly late), ordering by business effective time, not arrival order.

**5. "How do you prove a streaming dimensional model is correct?"**
> Reconcile it to an independent batch rebuild over the same events: compute the as-of join in a
> second engine (e.g. DuckDB's ASOF join) and assert the streaming `dim`/`fct` match it,
> order-insensitively. Two engines agreeing on the same semantics is the acceptance gate (G6), the
> streaming analogue of a dbt reconciliation test.

**6. "A late customer change arrives after some of her orders were already processed. What must be
true for the model to stay correct?"**
> The version's interval is decided by its business `effective_from`, so the late change slots into
> `[effective_from, next)` regardless of when it arrived; any order whose `order_ts` falls in that
> interval must be (re)valued against it. Correctness depends on event time and the validity ranges,
> not on processing order.
