# JVM-06 · Explain it back (Feynman)

**Prompt A.** "Explain to a dbt/pytest user how you'd test a Kafka producer/consumer in Java:
which part is a fast JUnit/AssertJ unit test, which part needs a real broker, how you grade it
without Docker, and why you poll instead of sleep."

**Prompt B.** "Given a multi-`Caused by:` stack trace, walk through finding the actual bug — which
line is the root cause and which frame you open first — and why reading top-down is a trap."

## Rubric — a strong explanation hits these (5–7):

- [ ] Uses JUnit 5 `@Test` + AssertJ fluent assertions and float `within` tolerance.
- [ ] Uses `assertThatThrownBy` to assert exceptions.
- [ ] Distinguishes unit (pure logic) from integration (real broker) tests.
- [ ] States the no-Docker rule: native broker / TopologyTestDriver grades; Testcontainers is an
      extra that skips when Docker is down.
- [ ] Polls for expected records with a deadline instead of `sleep()`.
- [ ] Identifies the deepest `Caused by:` as the root cause and the first app-package frame as the
      place to look; notes `... N more` is elision.
- [ ] (Bonus) Mentions Java 21 helpful NPE messages.
