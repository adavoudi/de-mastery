# JVM-01 · Explain it back (Feynman)

**Prompt A.** "You port a small Python helper (a function over a list of dicts) into Java for a
Kafka Streams job. Walk a Python developer through your type decisions. Say what becomes a record.
Say what becomes `List<...>` or `Map<...>`. Say where `Optional` replaces `None`. Say how you
signal a bad input."

**Prompt B.** "Explain the most common bug that a Python developer hits in the first week of Java:
comparing objects with `==`. Say why it happens and how to fix it."

## Rubric — a strong explanation hits these (5–7):

- [ ] Chooses a `record` for the immutable data carrier. Explains that it generates
      equals/hashCode/toString (dataclass analogy).
- [ ] Uses generic collections (`List<T>`, `Map<K,V>`). Says the compiler checks the element type.
- [ ] Uses `Optional<T>` (not `null`) to express "maybe absent". Shows `orElse`/`map`/`ifPresent`.
- [ ] Distinguishes checked from unchecked exceptions. Picks the right one for a bad argument
      (`IllegalArgumentException`, unchecked).
- [ ] Explains that `==` is reference identity on objects and `.equals()` is value equality.
- [ ] Notes that Java has no truthiness. Conditions must be `boolean` (`!list.isEmpty()`).
- [ ] (Bonus) Mentions the requirement that the package matches the directory layout.
