# STH-06 · Explain it back (Feynman)

**Prompt A.** "Explain the difference between two ways to handle bad records: a `WHERE` clause that
filters them, and a quarantine/DLQ that receives them. Why is a silent drop especially dangerous in
a streaming mart? Which single invariant catches it?"

**Prompt B.** "You add three DQ rules to the orders feed (not-null customer, positive amount, valid
customer reference). Explain how you split the stream into good and quarantine. Explain why the two
predicates must be exact complements. Explain how you handle a record that breaks two rules."

## Rubric — a strong explanation hits these (5–7):

- [ ] In-flight DQ checks each record as it flows. A stream has no post-load "stop the run".
- [ ] A `WHERE good` filter *silently drops* bad rows. A quarantine/DLQ *routes* them with a reason.
- [ ] The completeness invariant: `count(mart) + count(quarantine) == count(input)`, sets disjoint.
- [ ] A silent drop breaks completeness (the counts do not reconcile). This is the test that catches
      it.
- [ ] The good and bad predicates must be exact complements (no row in both, none in neither).
- [ ] A failure of several rules yields ONE reason (the first matching `CASE`) but still exactly one
      quarantine row.
- [ ] You map the rules to dbt `not_null` / accepted-values / `relationships` and to SOX
      completeness/accuracy controls.
