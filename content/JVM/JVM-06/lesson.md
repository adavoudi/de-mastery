# JVM-06 · Testing and debugging

## Why this closes the track

You can write Java, package it, serialize data, and reason about memory. The last productive skill
is to prove that your code works and to fix it when it does not. This module covers three tools:

- **JUnit 5** + **AssertJ** for unit tests.
- **Testcontainers** for integration tests against a real Kafka or Postgres.
- The ability to **read a JVM stack trace** to find the actual cause of a failure.

This is the Java version of your dbt tests plus a Spark pipeline integration test. The real job
expects exactly this skill.

## How JUnit 5 and AssertJ work

A test is a method annotated `@Test` in a class whose name ends in `Test`:

```java
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class OrderTotalTest {
    @Test
    void sumsPerCurrency() {
        var totals = OrderStats.totalByCurrency(sampleOrders());
        assertThat(totals).containsEntry("USD", 12.5);     // fluent, readable
        assertThat(totals.get("EUR")).isCloseTo(5.0, within(1e-9));  // float tolerance
    }

    @Test
    void rejectsBadAmount() {
        assertThatThrownBy(() -> Helpers.safeParseAmount("x"))
            .isInstanceOf(IllegalArgumentException.class);   // assert on exceptions
    }
}
```

- **JUnit 5** provides the lifecycle: `@Test`, `@BeforeEach`/`@AfterEach`, `@BeforeAll`,
  `@ParameterizedTest`, `@DisplayName`. The JUnit Platform console launcher runs the tests. This
  launcher grades every Java exercise in this track.
- **AssertJ** provides *fluent* assertions: `assertThat(x).isEqualTo(...)`,
  `.containsExactly(...)`, `.hasSize(...)`, `.isCloseTo(v, within(tol))`,
  `assertThatThrownBy(...).isInstanceOf(...)`. They read like English and give detailed failure
  messages. They are the Java analogue of the plain `assert` of `pytest` plus good diffs. Use a
  float **tolerance** (`within`) for doubles. Never use `==`.

Good unit tests follow the same rules as the rest of this platform (docs/TESTING.md). Assert
behaviour and invariants. Use tolerances for floats. Do not assert on timing or on which API the
code called.

## How Testcontainers works (integration tests)

Unit tests are fast. But a Kafka producer or consumer needs a *real* broker. **Testcontainers**
starts temporary Docker containers (a Kafka broker, a Postgres) for the duration of a test. It
removes them after the test:

```java
@Testcontainers
class ProducerIT {
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));

    @Test
    void roundTrips() {
        String bootstrap = kafka.getBootstrapServers();
        // produce, then consume, then assert the tally
    }
}
```

Testcontainers is the standard for JVM integration tests. **But it needs Docker.** On this
platform, every graded Kafka exercise must also work with **no Docker**. So we grade the
producer/consumer round-trip against a **native broker**. The `kafka_broker` fixture starts a real
Kafka JVM from the vendored tarball, with no container. Any Docker/Testcontainers assertion is an
*optional extra layer*. It skips cleanly when Docker is down. This is the no-Docker rule in action.
The native path proves your code. The container path is a bonus when a lab is up.

## How to read a JVM stack trace

A Java exception prints the exception type, a message, and the frames, with the newest frame
first. It prints nested causes under `Caused by:`:

```
java.lang.RuntimeException: pipeline failed
    at de.mastery.job.Pipeline.run(Pipeline.java:42)
    at de.mastery.job.Main.main(Main.java:11)
Caused by: java.lang.NumberFormatException: For input string: "N/A"
    at java.base/java.lang.Long.parseLong(Long.java:xxx)
    at de.mastery.job.Parser.toLong(Parser.java:88)
    ... 3 more
```

These reading rules save hours:
- **The *root cause* is the last `Caused by:`.** Here it is `NumberFormatException: For input
  string: "N/A"`. It is not the top line. Start there.
- **Find the first stack frame in *your* package** (`de.mastery...`) under the root cause. Look
  there (`Parser.toLong(Parser.java:88)`). The JDK and library frames above it are usually not the
  bug.
- `... N more` means "the same frames as the enclosing trace". It leaves out frames. It is not a
  different path.
- A `NullPointerException` in Java 21 includes a *helpful message* that names what was null. Read
  it.

So "find the bug" is a parse. Take the deepest cause. Then take the top frame whose class is in
your package. That is the exercise.

## From your world

| dbt / Spark / Python | JVM | Note |
|---|---|---|
| `dbt test` (not_null, unique) | JUnit `@Test` + AssertJ | behaviour assertions |
| `pytest` assert + diff | `assertThat(...)` fluent + rich message | readable failures |
| `pytest.raises` | `assertThatThrownBy(...).isInstanceOf(...)` | assert exceptions |
| a Spark integration test on a cluster | Testcontainers / native broker | real dependency |
| a Python traceback (oldest last) | Java stack trace (newest first, `Caused by:` deepest = root) | reversed mental model |

## When NOT to do what

- **Don't integration-test what a unit test covers.** Starting a broker for pure logic is slow and
  flaky. Unit-test the logic. Integration-test only the wiring.
- **Don't assert exact floating-point equality.** Use `isCloseTo(v, within(tol))`.
- **Don't read a stack trace top-down and fix the first line.** The top is usually a wrapper. The
  **root cause** and the first frame in *your* code are the targets.
- **Don't rely on Docker for a graded test.** Use the native broker first (or `TopologyTestDriver`
  for Streams, KFK-08). Use Testcontainers as an optional extra that skips when Docker is
  unavailable.
- **Don't `sleep()` in a test** to wait for Kafka. Poll for the expected records with a deadline.

## Further reading (official)

- JUnit 5 User Guide: <https://junit.org/junit5/docs/current/user-guide/>
- AssertJ core assertions: <https://assertj.github.io/doc/#assertj-core-assertions-guide>
- Testcontainers for Java — Kafka module: <https://java.testcontainers.org/modules/kafka/>
- Reading stack traces / `Throwable`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Throwable.html>
- Helpful NullPointerExceptions (JEP 358): <https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html>
