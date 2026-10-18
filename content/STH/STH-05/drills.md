# STH-05 · Interview drills

**1. "How do you derive `valid_to` for an SCD2 dimension from a CDC changelog?"**
> Order the versions of each key by their business effective time. Then take `valid_to =
> LEAD(effective_from) OVER (PARTITION BY key ORDER BY effective_from)`. The newest version has no
> next row. So you leave `valid_to` open (a far-future sentinel) and set `is_current = true`. The
> interval of each version is `[valid_from, valid_to)`.

**2. "What is a point-in-time (as-of) join and why is it the correct enrichment for a fact?"**
> It joins each fact to the dimension version that was effective at the event time of the fact. This
> is the row where `valid_from <= fact_ts < valid_to`. It is correct because you must value a fact
> with the attributes that were true when it happened, not the current attributes. This is the as-of
> rule.

**3. "Why is valuing an order at the customer's current tier a SOX defect, not a rounding error?"**
> It restates a historical fact. Suppose you reprocess the orders of last quarter with today's tier,
> address or FX. This silently changes the reported numbers for closed periods. That violates the
> controls for accuracy and auditability. The SCD2 dimension exists to pin each fact to its as-of
> version.

**4. "Streaming SCD2 vs nightly SCD2 MERGE — what's the same and what's different?"**
> The output is identical: the same versioned dimension with the same validity intervals. The input
> is different. It is a continuous CDC changelog, not a nightly snapshot. You must handle versions
> that arrive over time (and possibly late). You order by business effective time, not by arrival
> order.

**5. "How do you prove a streaming dimensional model is correct?"**
> Reconcile it to an independent batch rebuild over the same events. Compute the as-of join in a
> second engine (for example the ASOF join of DuckDB). Assert that the streaming `dim`/`fct` match
> it, and the order of rows does not matter. Two engines agree on the same semantics. This is the
> acceptance gate (G6). It is the streaming version of a dbt reconciliation test.

**6. "A late customer change arrives after some of her orders were already processed. What must be
true for the model to stay correct?"**
> The business `effective_from` decides the interval of the version. So the late change goes into
> `[effective_from, next)` no matter when it arrived. You must (re)value any order whose `order_ts`
> falls in that interval against this version. Correctness depends on event time and the validity
> ranges, not on processing order.
