# JVM-05 · Explain it back (Feynman)

**Prompt A.** "Show a colleague a GC log. Explain how to tell a memory leak from healthy
high-allocation churn. Say what 'before->after(total)' means. Say what the post-GC floor (live set)
tells you. Say what the end-state looks like."

**Prompt B.** "The heap-after of a Flink TaskManager is flat, but the OS keeps killing the
container because of OOM. Explain why the GC log looks fine. Say where you would actually look."

## Rubric — a strong explanation hits these (5–7):

- [ ] Reads `before->after(total)` correctly and tracks the **after** (live set) over time.
- [ ] Distinguishes a rising floor (leak → OOM) from a flat floor with frequent minor GCs (churn).
- [ ] Names the generations (young/old/metaspace). Says that promotion feeds the old gen.
- [ ] Explains that raising `-Xmx` only delays a real leak and makes pauses longer.
- [ ] Identifies off-heap/native memory as invisible to GC logs when RSS grows but heap-after is
      flat.
- [ ] Suggests concrete leak fixes: bounded caches, remove listeners, close resources.
- [ ] (Bonus) Ties it to Spark executor/driver OOM diagnosis (SPK-08).
