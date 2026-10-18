# JVM-06 · Testing and debugging

## Why this closes the track

You can write Java, package it, serialize data, and reason about memory. The last productive skill
is proving it works and fixing it when it doesn't: **JUnit 5** + **AssertJ** for unit tests,
**Testcontainers** for integration tests against a real Kafka/Postgres, and the ability to **read a
JVM stack trace** to find the actual cause of a failure. This is the Java version of your dbt tests
plus a Spark pipeline integration test — and it's exactly what CCDAK's "application testing" domain
and the real job both expect.

## How JUnit 5 and AssertJ work

A test is a method annotated `@Test` in a class ending in `Test`:

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
  `@ParameterizedTest`, `@DisplayName`. The platform runs tests with the JUnit Platform console
  launcher (that's what grades every Java exercise in this track).
- **AssertJ** provides *fluent* assertions: `assertThat(x).isEqualTo(...)`,
  `.containsExactly(...)`, `.hasSize(...)`, `.isCloseTo(v, within(tol))`,
  `assertThatThrownBy(...).isInstanceOf(...)`. They read like English and give rich failure
  messages — the Java analogue of `pytest`'s plain `assert` plus good diffs. Use float **tolerance**
  (`within`) for doubles, never `==`.

Good unit tests follow the same rules as the rest of this platform (docs/TESTING.md): assert
behaviour and invariants, use tolerances for floats, don't assert on timing or which API was called.

## How Testcontainers works (integration tests)

Unit tests are fast but a Kafka producer/consumer needs a *real* broker. **Testcontainers** spins up
throwaway Docker containers (a Kafka broker, a Postgres) for the duration of a test and tears them
down after:

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

Testcontainers is the standard for JVM integration tests. **But it needs Docker.** In this platform,
every graded Kafka exercise must also work with **no Docker**, so we grade the producer/consumer
round-trip against a **native broker** (the `kafka_broker` fixture starts a real Kafka JVM from the
vendored tarball — no container), and keep any Docker/Testcontainers assertion as an *optional extra
layer that skips cleanly when Docker is down*. That's the no-Docker rule in action: the native path
is what proves your code; the container path is a bonus when a lab is up.

## How to read a JVM stack trace

A Java exception prints the exception type, a message, and frames newest-first, with nested causes
under `Caused by:`:

```
java.lang.RuntimeException: pipeline failed
    at de.mastery.job.Pipeline.run(Pipeline.java:42)
    at de.mastery.job.Main.main(Main.java:11)
Caused by: java.lang.NumberFormatException: For input string: "N/A"
    at java.base/java.lang.Long.parseLong(Long.java:xxx)
    at de.mastery.job.Parser.toLong(Parser.java:88)
    ... 3 more
```

Reading rules that save hours:
- **The *root cause* is the last `Caused by:`** (here `NumberFormatException: For input string:
  "N/A"`), not the top line. Start there.
- **The first stack frame in *your* package** (`de.mastery...`) under the root cause is where to
  look (`Parser.toLong(Parser.java:88)`) — JDK/library frames above it are usually not the bug.
- `... N more` means "the same frames as the enclosing trace"; it's elision, not a different path.
- A `NullPointerException` in Java 21 includes a *helpful message* naming what was null — read it.

So "find the bug" is a parse: take the deepest cause, then the top frame whose class is in your
package. That's the exercise.

## From your world

| dbt / Spark / Python | JVM | Note |
|---|---|---|
| `dbt test` (not_null, unique) | JUnit `@Test` + AssertJ | behaviour assertions |
| `pytest` assert + diff | `assertThat(...)` fluent + rich message | readable failures |
| `pytest.raises` | `assertThatThrownBy(...).isInstanceOf(...)` | assert exceptions |
| a Spark integration test on a cluster | Testcontainers / native broker | real dependency |
| a Python traceback (oldest last) | Java stack trace (newest first, `Caused by:` deepest = root) | reversed mental model |

## When NOT to do what

- **Don't integration-test what a unit test covers.** Spinning a broker for pure logic is slow and
  flaky; unit-test the logic, integration-test only the wiring.
- **Don't assert exact floating-point equality** — use `isCloseTo(v, within(tol))`.
- **Don't read a stack trace top-down and fix the first line.** The top is usually a wrapper; the
  **root cause** and the first frame in *your* code are the targets.
- **Don't rely on Docker for a graded test.** Native broker (or `TopologyTestDriver` for Streams,
  KFK-08) first; Testcontainers as an optional extra that skips when Docker is unavailable.
- **Don't `sleep()` in a test** to wait for Kafka. Poll for the expected records with a deadline.

## Further reading (official)

- JUnit 5 User Guide: <https://junit.org/junit5/docs/current/user-guide/>
- AssertJ core assertions: <https://assertj.github.io/doc/#assertj-core-assertions-guide>
- Testcontainers for Java — Kafka module: <https://java.testcontainers.org/modules/kafka/>
- Reading stack traces / `Throwable`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Throwable.html>
- Helpful NullPointerExceptions (JEP 358): <https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html>
