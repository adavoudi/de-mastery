# KFK-10 · Explain it back (Feynman)

**Prompt A.** "Define consumer lag precisely. Where do the two numbers come from, why is the
*committed* offset the right one, and how would you turn lag into an alert that pages a human only
when something is actually wrong (not on every nightly batch spike)?"

**Prompt B.** "You own a transactional copier app. Describe the testing pyramid you'd put around it:
what goes in the fast unit layer, what needs `TopologyTestDriver` or a real broker, and why you keep
the broker layer thin. Tie each layer to something you already do with dbt tests and CI."

## Rubric — a strong explanation hits these (5–7):

- [ ] States lag = `end_offset - committed_offset` per partition, summed across partitions.
- [ ] Explains the committed offset is the group's durable progress (what a restart resumes from).
- [ ] Names where the two numbers come from (committed-offsets API / end-offsets API, or the
      consumer's `committed()` + `get_watermark_offsets()`).
- [ ] Distinguishes a normal bursty spike from a stuck consumer (lag high AND not draining).
- [ ] Describes two alert severities (warn vs page) and why only the real thing pages.
- [ ] Puts pure logic (lag math, alert rule, transforms) in the fast unit layer; keeps the
      real-broker layer thin and about *wiring*.
- [ ] Maps to dbt source freshness / test severities / CI gating.
