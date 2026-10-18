# KFK-10 · Explain it back (Feynman)

**Prompt A.** "Define consumer lag precisely. Where do the two numbers come from? Why is the
*committed* offset the right one? How would you turn lag into an alert that pages a human only when
something is actually wrong (not on every nightly batch spike)?"

**Prompt B.** "You own a transactional copier app. Describe the testing pyramid that you would put
around it. What goes in the fast unit layer? What needs `TopologyTestDriver` or a real broker? Why do
you keep the broker layer thin? Link each layer to something that you already do with dbt tests and
CI."

## Rubric — a strong explanation covers these points (5–7):

- [ ] States that lag = `end_offset - committed_offset` per partition, summed across partitions.
- [ ] Explains that the committed offset is the durable progress of the group (what a restart resumes
      from).
- [ ] Names where the two numbers come from (committed-offsets API / end-offsets API, or the
      `committed()` and `get_watermark_offsets()` of the consumer).
- [ ] Distinguishes a normal bursty spike from a stuck consumer (lag is high AND not draining).
- [ ] Describes two alert severities (warn vs page) and explains why only the real problem pages.
- [ ] Puts pure logic (lag math, alert rule, transforms) in the fast unit layer. Keeps the
      real-broker layer thin and about *wiring*.
- [ ] Maps to dbt source freshness, test severities, and CI gating.
