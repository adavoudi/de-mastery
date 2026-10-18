# JVM-06 · Explain it back (Feynman)

**Prompt A.** "Explain to a dbt/pytest user how you would test a Kafka producer/consumer in Java.
Say which part is a fast JUnit/AssertJ unit test. Say which part needs a real broker. Say how you
grade it without Docker. Say why you poll instead of sleep."

**Prompt B.** "You have a stack trace with several `Caused by:` sections. Walk through how to find
the actual bug. Say which line is the root cause. Say which frame you open first. Say why reading
top-down is a mistake."

## Rubric — a strong explanation hits these (5–7):

- [ ] Uses JUnit 5 `@Test` + AssertJ fluent assertions and the float `within` tolerance.
- [ ] Uses `assertThatThrownBy` to assert exceptions.
- [ ] Distinguishes unit tests (pure logic) from integration tests (real broker).
- [ ] States the no-Docker rule: the native broker or TopologyTestDriver grades the exercise.
      Testcontainers is an extra that skips when Docker is down.
- [ ] Polls for expected records with a deadline instead of `sleep()`.
- [ ] Identifies the deepest `Caused by:` as the root cause. Identifies the first app-package frame
      as the place to look. Notes that `... N more` leaves out frames.
- [ ] (Bonus) Mentions Java 21 helpful NPE messages.
