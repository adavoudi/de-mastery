# Explain it (Feynman)

Explain to a colleague who knows SQL but not distributed systems:

> "Why does adding more machines eventually stop making my query faster, and how do I decide
> whether to use one big box or a cluster at all?"

Keep it to ~5 minutes of talking. Use a Redshift leader/compute analogy.

## Rubric (self-check — aim for 5/6)

- [ ] States the two limits that force distribution: **capacity** and **time/deadline**.
- [ ] Explains **Amdahl's law** in words: the serial fraction caps speedup at `1/(1-p)`.
- [ ] Names a concrete serial bottleneck (final sort, `collect()` to driver, leader-node merge).
- [ ] Contrasts **scale up vs scale out** and names the price of scale-out (the **shuffle** /
      network).
- [ ] Gives a bytes-based reason a single box + DuckDB can beat a cluster (columnar pruning →
      fewer **bytes read**).
- [ ] States at least one concrete "do NOT distribute" condition (fits in RAM, serial-bound, or
      latency-sensitive small query).
