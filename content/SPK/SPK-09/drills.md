# SPK-09 · Interview drills

**1. "How do you tell from an execution plan that a Python UDF is hurting you?"**
> Look for a `BatchEvalPython` node. It means that Spark serializes the inputs of each row to a Python
> worker, runs your function, and deserializes the result. It does this per row. The node is also an
> optimizer fence. Filters cannot be pushed through it, and whole-stage codegen stops there. The
> vectorized counterpart is `ArrowEvalPython` (pandas/Arrow UDF). It sends data in batches via Arrow.

**2. "When is a UDF unavoidable, and how do you minimize its cost?"**
> A UDF is unavoidable when the logic is not SQL (a custom model, a bespoke parser) and no built-in
> can express it. To minimize the cost, do these three things:
> - Use a pandas/Arrow UDF, so that data moves in Arrow batches.
> - Use the iterator form to load any expensive resource once per worker.
> - Keep the UDF as narrow as possible, so that the rest of the plan stays optimizable.

**3. "Scalar pandas UDF vs `applyInPandas` — when each?"**
> Use a scalar pandas UDF (`Series -> Series`) for element-wise column math that needs Python. Use
> `applyInPandas` (grouped map) for per-group logic where you need the whole group as a
> `pandas.DataFrame` at once. An example is a per-customer calculation that uses all rows of that
> customer. Grouped map gives up partial aggregation. So do not use it where a `groupBy().agg()`
> of built-ins is enough.

**4. "Your UDF calls `random` / an external service. What breaks, and what do you do?"**
> Spark can evaluate non-deterministic UDFs more or fewer times than you expect (Catalyst may
> re-evaluate). So the results are not reproducible, and retries can give different results. Mark the UDF
> with `asNondeterministic()`, so that Spark does not reorder or collapse it. Also prefer to make it
> deterministic (seed it, or precompute the lookup as a joined table).

**5. "Why is the Pandas API on Spark not just 'pandas but slower'?"**
> It is lazy and distributed. Operations build a Spark plan and run across executors. So it scales
> past the memory of one machine, and real pandas cannot do this. The trade-offs are partial
> API compatibility, and that some operations (and the default index) can shuffle. For data that
> fits in memory, real pandas or DuckDB is simpler.
