# JVM-06 · Interview drills

**1. "How do you find the real bug in a Java stack trace?"**
> Read to the **last `Caused by:`**. That is the root cause. Then open the first stack frame whose
> class is in your own package. The top exception is usually a wrapper. The JDK and library frames
> above your code are rarely the bug. `... N more` stands for shared frames that the trace leaves
> out.

**2. "Unit test vs integration test for a Kafka client?"**
> Unit-test the pure logic (parsing, tallying) with JUnit/AssertJ. These tests are fast and
> deterministic. Integration-test the actual produce/consume wiring against a real broker (native
> broker or Testcontainers). Don't integration-test what a unit test already covers.

**3. "How do you grade a Kafka test without Docker?"**
> Use the native broker that starts from the Kafka tarball (a real JVM, no container). Or use an
> in-process `TopologyTestDriver` (Kafka Streams). Keep any Testcontainers/lab assertion as an
> extra layer that *skips* when Docker or the lab is unavailable. A skipped test cannot satisfy the
> "starter must fail" gate. So the native path must stand alone.

**4. "Why `assertThatThrownBy` instead of a try/catch in the test?"**
> It captures the thrown `Throwable` from a lambda. It lets you fluently assert the type and the
> message. It fails with a clear message if the code throws nothing. A manual try/catch that does
> not fail on "no exception" is a classic too-weak test.

**5. "Why not `Thread.sleep` while waiting for Kafka in a test?"**
> It is flaky if too short and slow if too long. Poll (`consumer.poll` in a loop) until you have
> the expected records or reach a deadline. This is deterministic and as fast as the data allows.

**6. "What does a Java 21 `NullPointerException` tell you that older ones didn't?"**
> It has a helpful message that names exactly which reference was null (for example "because the
> return value of `getX()` is null"). You do not have to guess which dereference on the line
> failed.
