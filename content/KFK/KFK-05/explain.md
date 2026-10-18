# KFK-05 · Explain it back (Feynman)

**Prompt A.** "Explain how a transactional read-process-write loop achieves exactly-once, naming the
five calls (init / begin / produce / send_offsets / commit) and what each contributes. Why does
sending the input offsets inside the transaction matter? What must the downstream consumer set?"

**Prompt B.** "Explain what a 'zombie' producer is and how Kafka's transactional.id + epoch fences
it. Why would using a random transactional.id per run reintroduce the duplicate you were trying to
prevent?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Names idempotence as the base and that transactions require/enable it.
- [ ] Lists init_transactions / begin / produce / send_offsets_to_transaction / commit (or abort).
- [ ] Explains that committing input offsets *inside* the transaction makes consume+produce atomic.
- [ ] States the downstream consumer must use `isolation.level=read_committed`.
- [ ] Explains abort = all-or-nothing (aborted writes invisible to read_committed).
- [ ] Explains zombie fencing via a stable transactional.id + epoch (`ProducerFenced`).
- [ ] (Bonus) Says a random per-run id defeats fencing, so two instances could both commit.
