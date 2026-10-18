# JVM: Java for data engineers (~20 h)

**Why.** Kafka and Flink are JVM projects. Kafka Streams is Java-only. The Flink DataStream API,
the state backends and most production Flink code are in Java. Exam samples for CCDAK use Java.
You also need to read JVM stack traces, tune heap and GC, and package jars. The goal is
**productive Java for data work**, not enterprise Java.

Can run in parallel with LKH. Exercises are graded with JUnit 5 + AssertJ through Gradle.

Scala: never required. SPK-05 includes a short "reading Scala in Spark source and stack traces" sidebar.
