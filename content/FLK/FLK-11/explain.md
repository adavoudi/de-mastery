# FLK-11 · Explain it back (Feynman)

**Prompt A.** "A Flink job's source is slow and the UI shows one operator red (backpressured).
Explain what backpressure is, how you'd find the bottleneck, and the two-part fix (parallelism +
redistribution). Why isn't raising parallelism alone enough?"

**Prompt B.** "Explain how you can prove a throughput fix from the job graph without timing it. Which
fields in the execution plan change, and why does a `FORWARD` edge mean work isn't being
redistributed? When would more parallelism NOT help?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Backpressure = buffer-fullness propagating upstream; the source runs at the slowest operator's rate.
- [ ] Find the bottleneck = the busy/backpressured vertex (UI or REST backpressure endpoint).
- [ ] Fix part 1 = raise the bottleneck operator's parallelism so more subtasks share the load.
- [ ] Fix part 2 = redistribute with REBALANCE/RESCALE so the subtasks are actually fed; FORWARD is one-to-one and doesn't balance.
- [ ] The plan changes: higher parallelism on the operator + a redistributing ship strategy instead of FORWARD.
- [ ] Skew (hot key under keyBy) is NOT fixed by parallelism — change/salt the key.
- [ ] A slow sink or state/checkpoint cost can cap the benefit of raw parallelism.
