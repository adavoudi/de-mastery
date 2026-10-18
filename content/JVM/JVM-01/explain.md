# JVM-01 · Explain it back (Feynman)

**Prompt A.** "You are porting a small Python helper (a function over a list of dicts) into Java for
a Kafka Streams job. Walk a Python developer through the type decisions you make: what becomes a
record, what becomes `List<...>`/`Map<...>`, where `Optional` replaces `None`, and how you signal
a bad input."

**Prompt B.** "Explain the single most common bug a Python developer hits in their first week of
Java — comparing objects with `==` — why it happens, and the fix."

## Rubric — a strong explanation hits these (5–7):

- [ ] Chooses a `record` for the immutable data carrier and explains it generates equals/hashCode/
      toString (dataclass analogy).
- [ ] Uses generic collections (`List<T>`, `Map<K,V>`) and says the element type is compile-checked.
- [ ] Uses `Optional<T>` (not `null`) to express "maybe absent" and shows `orElse`/`map`/`ifPresent`.
- [ ] Distinguishes checked vs unchecked exceptions and picks the right one for a bad argument
      (`IllegalArgumentException`, unchecked).
- [ ] Explains `==` is reference identity on objects and `.equals()` is value equality.
- [ ] Notes Java has no truthiness: conditions must be `boolean` (`!list.isEmpty()`).
- [ ] (Bonus) Mentions the package → directory layout requirement.
