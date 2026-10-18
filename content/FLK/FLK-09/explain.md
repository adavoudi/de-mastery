# FLK-09 · Explain it back (Feynman)

**Prompt A.** "Walk through the five streaming patterns (dedup, Top-N, split/merge,
MATCH_RECOGNIZE, OVER). For each pattern, say what it computes. Say whether its output changelog is
**append-only** or **updating**. Why does the difference between append-only and updating decide which sink you
can use?"

**Prompt B.** "Dedup-keep-first and dedup-keep-last are the same `ROW_NUMBER()` query. Only the
ORDER BY direction is different. Yet one is append-only and the other is updating. Explain why. Then explain
why Top-N must emit retractions."

## Rubric — a strong explanation hits these (5–7):

- [ ] Dedup = `ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts) = 1`. Keep-first (ASC) is append-only.
- [ ] Keep-last (DESC) is **updating**, because a newer row retracts the previous "last" row.
- [ ] Top-N = `ROW_NUMBER() <= N`. It is **updating**: a new entrant retracts the displaced row.
- [ ] Split = several filtered reads. Merge = `UNION ALL` (append-only) vs `UNION` (updating, keeps state).
- [ ] `MATCH_RECOGNIZE` finds a time-ordered sequence per partition (PATTERN/DEFINE/MEASURES). ORDER BY must be a time attribute.
- [ ] OVER emits one row per input row with a running aggregate. It is append-only. It does not collapse rows (unlike GROUP BY).
- [ ] Append-only → plain append sink. Updating → `upsert-kafka` / upsert / primary-key sink.
