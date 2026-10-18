# STH-05 · Explain it back (Feynman)

**Prompt A.** "Explain how you turn a customer CDC changelog into an SCD2 `dim_customer` with
`valid_from`/`valid_to`/`is_current`. What SQL closes an old version, what sets the open interval on
the newest, and why do you order by the business effective time rather than arrival order?"

**Prompt B.** "You join `orders` to that dimension to build `fct_orders`. Explain the as-of rule,
why valuing an order at the customer's *current* tier instead of the tier effective at the order
date is a correctness (SOX) defect, and how you'd prove the streaming result is right."

## Rubric — a strong explanation hits these (5–7):

- [ ] A CDC change event becomes an SCD2 version (one row, open `valid_to`).
- [ ] `valid_to` of a version = the next version's `valid_from`, via `LEAD(effective_from)` ordered
      by effective time.
- [ ] The newest version per key has an open/sentinel `valid_to` and `is_current = true`.
- [ ] The as-of join picks the version with `valid_from <= order_ts < valid_to` (exactly one per
      order).
- [ ] Explains why "latest" instead of "as-of" silently restates history (a SOX accuracy defect).
- [ ] Orders by the business effective time, not arrival/processing order, so late changes slot into
      the right interval.
- [ ] Names reconciliation to an independent batch rebuild (DuckDB AS-OF join) as the acceptance
      gate (G6).
