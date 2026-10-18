# Package a runnable fat jar with Gradle (Kotlin DSL)

You have a tiny data job, `de.mastery.jvm03.Job`, whose `main` prints a line that uses an Apache
**Avro** class (`org.apache.avro.Schema`). The job depends on Avro at runtime. Your task is to
write the Gradle build so that it produces a **runnable fat jar** that bundles Avro — exactly the
shape of a Flink/Kafka Streams job jar (Phase 4's Flink lab reuses this packaging).

Edit **`build.gradle.kts`** (the only file you change). The following are provided and must not be
edited: `settings.gradle.kts` and `src/main/java/de/mastery/jvm03/Job.java`.

Because this lab is **offline**, you must not use `mavenCentral()`. Two Gradle properties are passed
to the build and you must use them:

- `depsDir` — an absolute path to a directory containing `avro-1.12.0.jar` (and its transitive jars).
  Depend on **all** jars in that directory.
- `mainClass` — the fully-qualified main class (`de.mastery.jvm03.Job`).

Your build must register a task named **`fatJar`** (type `Jar`) that produces
`build/libs/<project>-all.jar` with:

1. A manifest whose **`Main-Class`** is the value of `mainClass`.
2. Your compiled classes **and** all the dependency (`depsDir`) classes bundled in (so
   `org.apache.avro.Schema` is present inside the jar).
3. **No duplicate entries and no signed-jar signature files** — set
   `duplicatesStrategy = DuplicatesStrategy.EXCLUDE` and exclude `META-INF/*.SF`, `META-INF/*.DSA`,
   `META-INF/*.RSA`. (A merged archive that keeps signature files fails the JVM verifier.)

The jar must run: `java -jar build/libs/<project>-all.jar` prints the job's line and exits 0.

Tips:
- Read a property with `project.property("depsDir") as String` (it is guaranteed present).
- Depend on a directory of jars with `implementation(fileTree(depsDir) { include("*.jar") })`.
- The default `jar` task is thin; register a *separate* `fatJar` task — don't rely on `jar`.

The test runs the build offline with the vendored Gradle 8.14.3 and JDK 21, then inspects the jar's
manifest and entries and actually executes it. (This is the one module whose suite may take ~30 s —
a Gradle build is heavier than a `javac` run; the test runs exactly one build.)
