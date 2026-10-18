# KFK-05 · Explain it back (Feynman)

**Prompt A.** "Explain how a transactional read-process-write loop achieves exactly-once. Name the
five calls (init / begin / produce / send_offsets / commit) and say what each call contributes. Why
does it matter to send the input offsets inside the transaction? What must the downstream consumer
set?"

**Prompt B.** "Explain what a 'zombie' producer is. Explain how the transactional.id and the epoch
of Kafka fence it. Why does a random transactional.id for each run bring back the duplicate that you
wanted to prevent?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Names idempotence as the base and that transactions require/enable it.
- [ ] Lists init_transactions / begin / produce / send_offsets_to_transaction / commit (or abort).
- [ ] Explains that committing input offsets *inside* the transaction makes consume+produce atomic.
- [ ] States the downstream consumer must use `isolation.level=read_committed`.
- [ ] Explains abort = all-or-nothing (aborted writes invisible to read_committed).
- [ ] Explains zombie fencing via a stable transactional.id + epoch (`ProducerFenced`).
- [ ] (Bonus) Says a random per-run id defeats fencing, so two instances could both commit.
