# STH-06 · Explain it back (Feynman)

**Prompt A.** "Explain the difference between filtering bad records with a `WHERE` clause and routing
them to a quarantine/DLQ. Why is a silent drop especially dangerous in a streaming mart, and what
single invariant catches it?"

**Prompt B.** "You add three DQ rules to the orders feed (not-null customer, positive amount, valid
customer reference). Explain how you split the stream into good and quarantine, why the two
predicates must be exact complements, and how a record that breaks two rules is handled."

## Rubric — a strong explanation hits these (5–7):

- [ ] In-flight DQ checks each record as it flows; there is no post-load "stop the run" in a stream.
- [ ] A `WHERE good` filter *silently drops* bad rows; a quarantine/DLQ *routes* them with a reason.
- [ ] The completeness invariant: `count(mart) + count(quarantine) == count(input)`, sets disjoint.
- [ ] A silent drop breaks completeness (counts don't reconcile) — the test that catches it.
- [ ] The good and bad predicates must be exact complements (no row in both, none in neither).
- [ ] A multi-rule failure yields ONE reason (first matching `CASE`) but still exactly one
      quarantine row.
- [ ] Maps the rules to dbt `not_null` / accepted-values / `relationships` and to SOX
      completeness/accuracy controls.
