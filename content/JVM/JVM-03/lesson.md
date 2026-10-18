# JVM-03 · Build and dependencies

## Why a build tool and a fat jar

To deploy a dbt project, you ship the repo. To deploy a Python job, you build a wheel or a
container. To deploy a **Flink or Kafka Streams job**, you ship a *jar*. Almost always it is a
**fat jar** (also called an uber jar or shaded jar). A fat jar is a single jar that contains your
code *and* the classes of all its dependencies. The cluster can run it with nothing else on the
classpath.

If you get this wrong, you see a common failure in Flink and Kafka work: the job runs locally, but
it throws `ClassNotFoundException` on the cluster. So build a fat jar by hand once.

Java has two mainstream build tools:

- **Maven** uses XML (`pom.xml`) and a fixed lifecycle.
- **Gradle** uses a programmable build script.

We use **Gradle with the Kotlin DSL** (`build.gradle.kts`). Most new Flink and Kafka projects use
it. Also, the typed Kotlin DSL gives you IDE completion. You will see Maven's `pom.xml` in real
projects, so the lesson shows the mapping. The exercise uses Gradle.

## How Gradle works (the parts you need)

A minimal `build.gradle.kts`:

```kotlin
plugins {
    java                       // adds compileJava, jar, test, etc.
}

repositories {
    mavenCentral()             // where dependencies are downloaded from
}

dependencies {
    implementation("org.apache.avro:avro:1.12.0")   // compile + runtime
    compileOnly("org.apache.flink:flink-streaming-java:1.20.0") // provided by the cluster
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
}
```

Key ideas:

- **Configurations = dependency scopes.** Each scope has its own use:
  - `implementation` is on the compile and runtime classpath. It *goes into the fat jar*.
  - `compileOnly` is for compile time only. Use it for libraries that the runtime already provides
    (Flink itself, the Kafka broker's classes). Then you do not bundle a second copy, and you avoid
    version conflicts.
  - `runtimeOnly` is for runtime only (for example a logging backend).
  - `testImplementation` is for tests only.

  Choosing the right scope is the most important fat-jar skill.
- **Tasks** are the units of work (`compileJava`, `jar`, `test`). You can register your own task,
  for example a `fatJar` task.
- **The default `jar` is thin.** It contains only *your* classes, not the dependencies. If you run
  it on a bare JVM, the JVM throws `NoClassDefFoundError` for the first dependency class that the
  code uses. A **fat jar** copies the dependency classes in.

![Thin jar has only your classes and fails with NoClassDefFoundError; fat jar bundles implementation dependencies while compileOnly Flink stays out](diagrams/thin-vs-fat-jar.svg)

### Building a fat jar

The production approach is the **Shadow plugin** (`com.gradleup.shadow`). It also *relocates*
(renames) bundled packages to avoid version clashes. For example, it can shade `com.google.guava`
to `myapp.shaded.guava`, so your Guava cannot collide with Flink's. In this offline lab, we build
the fat jar with a plain `Jar` task. Then you see exactly what goes in. The concepts transfer
directly:

```kotlin
tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    manifest { attributes("Main-Class" to "de.mastery.jvm03.Job") }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE          // handle collisions deterministically
    from(sourceSets.main.get().output)                      // your classes
    from(configurations.runtimeClasspath.get().map {        // + dependency classes
        if (it.isDirectory) it else zipTree(it)
    })
    // Drop other jars' signatures and module descriptors, or the fat jar won't verify / will clash:
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.MF.orig")
}
```

**Why the excludes matter (the no-conflict rule).** When you merge many jars, three things can
happen:

- Signed jars ship `META-INF/*.SF/.DSA/.RSA`. Suppose you copy them into a jar whose contents
  changed. Then the JVM's jar verifier throws `SecurityException: Invalid signature file digest`.
  You must drop these files.
- Two dependencies may each contain the same file (`module-info.class`, `META-INF/services/...`,
  `reference.conf`). Without a `duplicatesStrategy`, Gradle *fails the build*. With `EXCLUDE`, you
  keep the first file (this is deterministic). You should *merge* real services files instead.
  Shadow does this with `mergeServiceFiles()`, and this is why Shadow exists. But for a jar that
  runs, the minimum is to exclude signatures and to set a duplicates strategy.
- A correct fat jar has **exactly one** copy of each class, with no duplicate entries. It also has
  a `Main-Class`, so `java -jar app-all.jar` works.

## From your world

| Python / dbt | Gradle (JVM) | Note |
|---|---|---|
| `pip install` / `requirements.txt` | `dependencies { implementation(...) }` | pinned coordinates `group:name:version` |
| building a wheel / zip | the `jar` / `fatJar` task | artifact to ship |
| a container image with deps baked in | a fat/uber jar | self-contained for the cluster |
| `--no-deps` for provided libs | `compileOnly(...)` | don't bundle what the runtime provides |
| dependency resolution conflicts | version conflicts / relocation (shading) | shade to isolate |
| `python -m mypkg` | `Main-Class` in the manifest + `java -jar` | entry point |

## When NOT to build a fat jar (or do it differently)

- **Don't bundle the runtime.** Mark libraries that Flink, Spark or Kafka provide as `compileOnly`
  (`provided` in Maven). Suppose you bundle Flink into a Flink job jar. Then you get classloader
  conflicts, and the artifact becomes larger than it needs to be. This is a classic cause of
  `LinkageError` and `NoSuchMethodError` on the cluster.
- **Don't fat-jar a library.** Publish libraries as thin jars with declared dependencies. Only
  *application/job* artifacts are fat.
- **Don't ignore signature files or duplicate classes.** These two bugs make a fat jar compile
  without errors but fail at runtime.
- **Prefer the Shadow plugin in production.** It gives you relocation and service-file merging. The
  manual `Jar` task here helps you learn the mechanics offline.

> **FLK will reuse this.** The Flink lab in Phase 4 submits a job jar built exactly this way. Your
> code is bundled in the fat jar, and Flink is marked `compileOnly`. The packaging skill that you
> build here is the one you will use to run a real Flink job.

## Further reading (official)

- Gradle Java Library plugin / dependency configurations: <https://docs.gradle.org/current/userguide/java_library_plugin.html>
- Gradle Kotlin DSL primer: <https://docs.gradle.org/current/userguide/kotlin_dsl.html>
- Building Java applications / the `Jar` task: <https://docs.gradle.org/current/userguide/building_java_projects.html>
- `DuplicatesStrategy`: <https://docs.gradle.org/current/javadoc/org/gradle/api/file/DuplicatesStrategy.html>
- Flink: packaging / project configuration (fat jar, provided scope): <https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/configuration/overview/>
