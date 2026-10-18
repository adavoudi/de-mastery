# JVM-06 · Interview drills

**1. "How do you find the real bug in a Java stack trace?"**
> Read to the **last `Caused by:`** — that's the root cause — then open the first stack frame whose
> class is in your own package. The top exception is usually a wrapper; JDK/library frames above
> your code are rarely the bug. `... N more` is elided shared frames.

**2. "Unit test vs integration test for a Kafka client?"**
> Unit-test the pure logic (parsing, tallying) with JUnit/AssertJ — fast, deterministic.
> Integration-test the actual produce/consume wiring against a real broker (native broker or
> Testcontainers). Don't integration-test what a unit test already covers.

**3. "How do you grade a Kafka test without Docker?"**
> Use the native broker started from the Kafka tarball (a real JVM, no container) or an in-process
> `TopologyTestDriver` (Kafka Streams). Keep any Testcontainers/lab assertion as an extra layer that
> *skips* when Docker/the lab is unavailable — a skipped test can't satisfy the "starter must fail"
> gate, so the native path must stand alone.

**4. "Why `assertThatThrownBy` instead of a try/catch in the test?"**
> It captures the thrown `Throwable` from a lambda and lets you fluently assert its type and
> message, failing clearly if nothing is thrown. A manual try/catch that forgets to fail on "no
> exception" is a classic too-weak test.

**5. "Why not `Thread.sleep` while waiting for Kafka in a test?"**
> It's flaky if too short and slow if too long. Poll (`consumer.poll` in a loop) until you have the
> expected records or hit a deadline — deterministic and as fast as the data allows.

**6. "What does a Java 21 `NullPointerException` tell you that older ones didn't?"**
> A helpful message naming exactly which reference was null (e.g. "because the return value of
> `getX()` is null"), so you don't have to guess which dereference on the line failed.
