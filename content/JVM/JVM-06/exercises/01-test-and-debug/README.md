# Unit-test logic, round-trip through a real broker, read a stack trace

This capstone of the JVM track exercises the three debugging/testing skills together. You implement
two Java classes in package `de.mastery.jvm06`.

## 1. `StackTraces.java` — read a stack trace (graded by JUnit, no Docker)

Implement two pure static methods (no I/O):

- `String rootCause(String trace)` — given the full text of a Java stack trace, return the
  **root-cause exception class name**: the exception named by the **last** `Caused by:` line (or,
  if there is no `Caused by:`, the exception on the first line). Return just the fully-qualified
  class name, without the message. Example: for a trace ending in
  `Caused by: java.lang.NumberFormatException: For input string: "N/A"`, return
  `java.lang.NumberFormatException`.
- `String topAppFrame(String trace, String packagePrefix)` — return the first stack frame line's
  `class.method(File.java:line)` location whose class starts with `packagePrefix` (your code),
  skipping JDK/library frames. Return the content after `at ` (trimmed), e.g.
  `de.mastery.job.Parser.toLong(Parser.java:88)`. Return `null` if no frame matches.

## 2. `KafkaRoundTrip.java` — produce and consume against a real broker (graded with the native broker, no Docker)

Implement `public static void main(String[] args)`:
- `args[0]` = bootstrap servers, `args[1]` = topic, `args[2..]` = the string values to produce.
- Produce each value as a record (use the value as both key and value) with `acks=all`, then flush.
- Consume the topic from the beginning with a **fresh** `group.id` and `auto.offset.reset=earliest`,
  **polling with a deadline** (never `Thread.sleep`), until you have read all the produced values.
- Print a line `TALLY` then one `value=count` line per distinct value (any order). For input
  `a a b a b` print (in some order) `a=3` and `b=2`.

Use `StringSerializer`/`StringDeserializer` and close the producer/consumer (try-with-resources).

## Grading

- The JUnit layer (`StackTraces`) runs with no broker and no Docker and must pass.
- The native-broker layer starts a **real Kafka JVM from the vendored tarball** (no container),
  creates a unique topic, runs your `main`, and checks the printed tally — this is the no-Docker
  graded path.
- An optional **lab layer** runs the same round-trip against the `streaming` lab broker; it
  **skips cleanly** when the lab is down (it is an extra, not required to pass).

Constraints: keep the package and signatures; poll instead of sleeping; the native-broker suite is
Kafka-heavy and may take up to ~90 s under the 5× validation shuffle — that is expected and
documented.
