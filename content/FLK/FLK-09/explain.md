# FLK-09 · Explain it back (Feynman)

**Prompt A.** "Walk through the five streaming patterns (dedup, Top-N, split/merge,
MATCH_RECOGNIZE, OVER). For each, say what it computes and whether its output changelog is
**append-only** or **updating**. Why does the append-vs-updating distinction decide which sink you
can use?"

**Prompt B.** "Dedup-keep-first and dedup-keep-last are the same `ROW_NUMBER()` query with the
ORDER BY direction flipped, yet one is append-only and the other updating. Explain why. Then explain
why Top-N must emit retractions."

## Rubric — a strong explanation hits these (5–7):

- [ ] Dedup = `ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts) = 1`; keep-first (ASC) is append-only.
- [ ] Keep-last (DESC) is **updating** because a newer row retracts the previous "last".
- [ ] Top-N = `ROW_NUMBER() <= N`; it is **updating** — a new entrant retracts the displaced row.
- [ ] Split = several filtered reads; merge = `UNION ALL` (append-only) vs `UNION` (updating, keeps state).
- [ ] `MATCH_RECOGNIZE` finds a time-ordered sequence per partition (PATTERN/DEFINE/MEASURES); ORDER BY must be a time attribute.
- [ ] OVER emits one row per input with a running aggregate, append-only, without collapsing rows (unlike GROUP BY).
- [ ] Append-only → plain append sink; updating → `upsert-kafka` / upsert / primary-key sink.
