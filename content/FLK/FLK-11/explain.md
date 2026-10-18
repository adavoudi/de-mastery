# FLK-11 · Explain it back (Feynman)

**Prompt A.** "The source of a Flink job is slow and the UI shows one operator in red (backpressured).
Explain what backpressure is. Explain how you find the bottleneck. Explain the fix, which has two parts (parallelism +
redistribution). Why is raising parallelism alone not enough?"

**Prompt B.** "Explain how you can prove a throughput fix from the job graph without timing it. Which
fields in the execution plan change? Why does a `FORWARD` edge mean that the work is not
redistributed? When would more parallelism NOT help?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Backpressure = full buffers spreading upstream. The source runs at the rate of the slowest operator.
- [ ] Find the bottleneck = the busy or backpressured vertex (UI or REST backpressure endpoint).
- [ ] Fix part 1 = raise the parallelism of the bottleneck operator, so more subtasks share the load.
- [ ] Fix part 2 = redistribute with REBALANCE/RESCALE, so the subtasks get records. FORWARD is one-to-one and does not balance.
- [ ] The plan changes: higher parallelism on the operator + a redistributing ship strategy instead of FORWARD.
- [ ] Parallelism does NOT fix skew (hot key under keyBy). Change or salt the key.
- [ ] A slow sink or the cost of state and checkpoints can limit the benefit of raw parallelism.
