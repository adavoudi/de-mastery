# JVM: Java for data engineers (~20 h)

**Why.** Kafka and Flink are JVM projects. Kafka Streams is Java-only. The Flink DataStream API,
the state backends and most production Flink code are in Java. Exam samples for CCDAK use Java.
You also need to read JVM stack traces, tune heap and GC, and package jars. The goal is
**productive Java for data work**, not enterprise Java.

Can run in parallel with LKH. Exercises are graded with JUnit 5 + AssertJ through Gradle.

| ID | Module | h | Key ideas | Exercises (graded) |
|---|---|---|---|---|
| JVM-01 | Java for Python developers | 4 | Types, classes, records, generics, interfaces, `Optional`, exceptions | Port 5 small Python functions to Java |
| JVM-02 | Collections & streams | 3 | `List/Map/Set`, `java.util.stream`, lambdas, method references | Aggregate order events with streams |
| JVM-03 | Build & dependencies | 3 | Gradle (Kotlin DSL), Maven basics, shaded/fat jars, dependency conflicts | Package a Flink job jar that runs on the lab cluster |
| JVM-04 | Serialization | 3 | POJOs, Avro `SpecificRecord`, Jackson; why Kryo fallback is slow in Flink | Avro round-trip with a schema; the test fails if the code falls back to Kryo |
| JVM-05 | Concurrency & the JVM | 4 | Threads, executors, `CompletableFuture`; heap, GC, off-heap memory | Read a heap/GC log and find the leak |
| JVM-06 | Testing & debugging | 3 | JUnit 5, AssertJ, Testcontainers, reading stack traces, remote debugging | Testcontainers Kafka round-trip test |

Scala: never required. SPK-05 includes a short "reading Scala in Spark source and stack traces" sidebar.
