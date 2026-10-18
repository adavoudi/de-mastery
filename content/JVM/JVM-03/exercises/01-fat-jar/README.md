# Package a runnable fat jar with Gradle (Kotlin DSL)

You have a tiny data job, `de.mastery.jvm03.Job`. Its `main` prints a line that uses an Apache
**Avro** class (`org.apache.avro.Schema`). The job depends on Avro at runtime. Your task is to write
the Gradle build so that it produces a **runnable fat jar** that bundles Avro. This is exactly the
shape of a Flink or Kafka Streams job jar. (The Flink lab in Phase 4 reuses this packaging.)

Edit **`build.gradle.kts`**. It is the only file you change. The task provides these files, and you
must not edit them: `settings.gradle.kts` and `src/main/java/de/mastery/jvm03/Job.java`.

This lab is **offline**, so you must not use `mavenCentral()`. The build receives two Gradle
properties, and you must use them:

- `depsDir` — an absolute path to a directory that contains `avro-1.12.0.jar` (and its transitive
  jars). Depend on **all** jars in that directory.
- `mainClass` — the fully-qualified main class (`de.mastery.jvm03.Job`).

Your build must register a task named **`fatJar`** (type `Jar`). The task produces
`build/libs/<project>-all.jar` with these properties:

1. A manifest whose **`Main-Class`** is the value of `mainClass`.
2. Your compiled classes **and** all the dependency (`depsDir`) classes are bundled in. So
   `org.apache.avro.Schema` is present inside the jar.
3. **No duplicate entries and no signed-jar signature files.** Set
   `duplicatesStrategy = DuplicatesStrategy.EXCLUDE`. Exclude `META-INF/*.SF`, `META-INF/*.DSA` and
   `META-INF/*.RSA`. (A merged archive that keeps signature files fails the JVM verifier.)

The jar must run. `java -jar build/libs/<project>-all.jar` prints the job's line and exits 0.

Tips:
- Read a property with `project.property("depsDir") as String`. The property is always present.
- Depend on a directory of jars with `implementation(fileTree(depsDir) { include("*.jar") })`.
- The default `jar` task is thin. Register a *separate* `fatJar` task. Do not rely on `jar`.

The test runs the build offline with the vendored Gradle 8.14.3 and JDK 21. Then it inspects the
manifest and the entries of the jar, and it runs the jar. (The suite for this module may take about
30 s. A Gradle build is heavier than a `javac` run. The test runs exactly one build.)
