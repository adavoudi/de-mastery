# SPK-09 · Interview drills

**1. "How do you tell from an execution plan that a Python UDF is hurting you?"**
> Look for a `BatchEvalPython` node. It means Spark serializes each row's inputs to a Python worker,
> runs your function, and deserializes the result — per row. It is also an optimizer fence: filters
> can't be pushed through it and whole-stage codegen stops there. The vectorized counterpart is
> `ArrowEvalPython` (pandas/Arrow UDF), which batches via Arrow.

**2. "When is a UDF unavoidable, and how do you minimize its cost?"**
> When the logic is genuinely non-SQL (a custom model, a bespoke parser) and no built-in composes
> to it. Minimize cost by using a pandas/Arrow UDF so data moves in Arrow batches, by using the
> iterator form to load any expensive resource once per worker, and by keeping the UDF as narrow as
> possible so the rest of the plan stays optimizable.

**3. "Scalar pandas UDF vs `applyInPandas` — when each?"**
> A scalar pandas UDF (`Series -> Series`) is for element-wise column math that happens to need
> Python. `applyInPandas` (grouped map) is for per-group logic where you need the whole group as a
> `pandas.DataFrame` at once — e.g. a per-customer calculation that references all of that
> customer's rows. Grouped map gives up partial aggregation, so don't use it where `groupBy().agg()`
> of built-ins suffices.

**4. "Your UDF calls `random` / an external service. What breaks, and what do you do?"**
> Non-deterministic UDFs can be evaluated more or fewer times than you expect (Catalyst may
> re-evaluate), so results aren't reproducible and retries can diverge. Mark it with
> `asNondeterministic()` so Spark won't reorder/collapse it, and prefer making it deterministic
> (seed it, or precompute the lookup as a joined table).

**5. "Why is the Pandas API on Spark not just 'pandas but slower'?"**
> Because it's lazy and distributed: operations build a Spark plan and run across executors, so it
> scales past one machine's memory, which real pandas can't. The trade-offs are partial
> API-compatibility and that some operations (and the default index) can shuffle. For data that
> fits in memory, real pandas or DuckDB is simpler.
