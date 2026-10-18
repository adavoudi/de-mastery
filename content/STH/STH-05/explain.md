# STH-05 · Explain it back (Feynman)

**Prompt A.** "Explain how you turn a customer CDC changelog into an SCD2 `dim_customer` with
`valid_from`/`valid_to`/`is_current`. Which SQL closes an old version? What sets the open interval
on the newest version? Why do you order by the business effective time and not by arrival order?"

**Prompt B.** "You join `orders` to that dimension to build `fct_orders`. Explain the as-of rule.
Explain why it is a correctness (SOX) defect to value an order at the *current* tier of the
customer instead of the tier that was effective at the order date. Explain how you would prove that
the streaming result is right."

## Rubric — a strong explanation hits these (5–7):

- [ ] A CDC change event becomes an SCD2 version (one row, open `valid_to`).
- [ ] The `valid_to` of a version = the `valid_from` of the next version, via `LEAD(effective_from)`
      ordered by effective time.
- [ ] The newest version per key has an open/sentinel `valid_to` and `is_current = true`.
- [ ] The as-of join picks the version with `valid_from <= order_ts < valid_to` (exactly one per
      order).
- [ ] You explain why "latest" instead of "as-of" silently restates history (a SOX accuracy defect).
- [ ] You order by the business effective time, not by arrival/processing order. Then late changes
      go into the right interval.
- [ ] You name reconciliation to an independent batch rebuild (DuckDB AS-OF join) as the acceptance
      gate (G6).
