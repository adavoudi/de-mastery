# LKH-04 · Explain it back (Feynman)

**Prompt A.** "Walk me through applying one daily batch to an SCD2 dimension in Iceberg with
`MERGE INTO`: which rows get closed, which get inserted, and what `valid_from`/`valid_to`/
`is_current` look like for a key whose attribute changed, a key that's unchanged, and a brand-new
key."

**Prompt B.** "I ran the same DELETE on two Iceberg tables, one copy-on-write and one
merge-on-read. Explain, using the metadata (the `.files` table's `content` column), how I can
*prove* which table did which — without timing anything."

## Rubric — a strong explanation hits these (5–7):

- [ ] States that data files are immutable, so change means rewrite (CoW) or delete file (MoR).
- [ ] CoW: rewrite affected data files; fast reads, write amplification.
- [ ] MoR: write a delete file (position/equality/DV); cheap writes, read-time merge.
- [ ] Verifies the mode via metadata: MoR shows `content=1` delete files; CoW rewrites data files
      (only `content=0`, data-file set changes).
- [ ] SCD2: close the changed current row (`valid_to`, `is_current=false`) and insert a new open
      version; unchanged keys untouched; new keys inserted as current.
- [ ] Names a classic SCD2 bug (overwrite-in-place / open valid_to / off-by-one date) and its effect.
- [ ] (Bonus) Justifies CoW-vs-MoR choice by read/write ratio.
