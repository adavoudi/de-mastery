# Unit-test logic, round-trip through a real broker, read a stack trace

This is the last exercise of the JVM track. It exercises the three debugging and testing skills
together. You implement two Java classes in package `de.mastery.jvm06`.

## 1. `StackTraces.java` — read a stack trace (graded by JUnit, no Docker)

Implement two pure static methods (no I/O):

- `String rootCause(String trace)` — you get the full text of a Java stack trace. Return the
  **root-cause exception class name**. This is the exception that the **last** `Caused by:` line
  names. If there is no `Caused by:`, it is the exception on the first line. Return only the
  fully-qualified class name, without the message. Example: a trace ends in
  `Caused by: java.lang.NumberFormatException: For input string: "N/A"`. Return
  `java.lang.NumberFormatException`.
- `String topAppFrame(String trace, String packagePrefix)` — return the
  `class.method(File.java:line)` location of the first stack frame line whose class starts with
  `packagePrefix` (your code). Skip JDK and library frames. Return the content after `at `
  (trimmed), for example `de.mastery.job.Parser.toLong(Parser.java:88)`. Return `null` if no frame
  matches.

## 2. `KafkaRoundTrip.java` — produce and consume against a real broker (graded with the native broker, no Docker)

Implement `public static void main(String[] args)`:
- `args[0]` = bootstrap servers, `args[1]` = topic, `args[2..]` = the string values to produce.
- Produce each value as a record. Use the value as both key and value. Use `acks=all`, then flush.
- Consume the topic from the beginning with a **fresh** `group.id` and `auto.offset.reset=earliest`.
  **Poll with a deadline** (never `Thread.sleep`) until you have read all the produced values.
- Print a line `TALLY`. Then print one `value=count` line for each distinct value (any order). For
  the input `a a b a b`, print `a=3` and `b=2` (in some order).

Use `StringSerializer`/`StringDeserializer`. Close the producer and the consumer
(try-with-resources).

## Grading

- The JUnit layer (`StackTraces`) runs with no broker and no Docker. It must pass.
- The native-broker layer starts a **real Kafka JVM from the vendored tarball** (no container). It
  creates a unique topic, runs your `main`, and checks the printed tally. This is the no-Docker
  graded path.
- An optional **lab layer** runs the same round-trip against the `streaming` lab broker. It
  **skips cleanly** when the lab is down. It is an extra. It is not required to pass.

Constraints: keep the package and the signatures. Poll instead of sleeping. The native-broker suite
uses Kafka heavily. It may take up to ~90 s under the 5× validation shuffle. This is expected and
documented.
