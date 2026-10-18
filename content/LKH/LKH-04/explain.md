# LKH-04 · Explain it back (Feynman)

**Prompt A.** "Explain how to apply one daily batch to an SCD2 dimension in Iceberg with
`MERGE INTO`. Which rows does the batch close? Which rows does it insert? Show what `valid_from`,
`valid_to` and `is_current` look like for three kinds of key: a key whose attribute changed, a key
that is unchanged, and a brand-new key."

**Prompt B.** "I ran the same DELETE on two Iceberg tables. One table is copy-on-write and the
other is merge-on-read. Explain how I can *prove* which table did which, without timing anything.
Use the metadata (the `content` column of the `.files` table)."

## Rubric — a strong explanation hits these (5–7):

- [ ] States that data files are immutable. So a change means a rewrite (CoW) or a delete file
      (MoR).
- [ ] CoW: rewrite the affected data files. Reads are fast. Writes have amplification.
- [ ] MoR: write a delete file (position, equality or DV). Writes are cheap. Reads merge at read
      time.
- [ ] Verifies the mode with metadata. MoR shows `content=1` delete files. CoW rewrites data files
      (only `content=0`, and the set of data files changes).
- [ ] SCD2: close the changed current row (`valid_to`, `is_current=false`) and insert a new open
      version. Unchanged keys are untouched. New keys are inserted as current.
- [ ] Names a classic SCD2 bug (overwrite in place, open valid_to or off-by-one date) and its
      effect.
- [ ] (Bonus) Justifies the choice between CoW and MoR with the read/write ratio.
