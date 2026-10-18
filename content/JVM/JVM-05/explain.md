# JVM-05 · Explain it back (Feynman)

**Prompt A.** "Show a colleague a GC log and explain how to tell a memory leak from healthy
high-allocation churn — what 'before->after(total)' means, what the post-GC floor (live set) tells
you, and what the end-state looks like."

**Prompt B.** "A Flink TaskManager's heap-after is flat but the container keeps getting OOM-killed by
the OS. Explain why the GC log looks fine and where you'd actually look."

## Rubric — a strong explanation hits these (5–7):

- [ ] Reads `before->after(total)` correctly and tracks the **after** (live set) over time.
- [ ] Distinguishes a rising floor (leak → OOM) from a flat floor with frequent minor GCs (churn).
- [ ] Names generations (young/old/metaspace) and that promotion feeds old gen.
- [ ] Explains that raising `-Xmx` only delays a real leak and lengthens pauses.
- [ ] Identifies off-heap/native memory as invisible to GC logs when RSS grows but heap-after is flat.
- [ ] Suggests concrete leak fixes: bounded caches, remove listeners, close resources.
- [ ] (Bonus) Ties it to Spark executor/driver OOM diagnosis (SPK-08).
