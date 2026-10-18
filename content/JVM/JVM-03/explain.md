# JVM-03 · Explain it back (Feynman)

**Prompt A.** "A teammate's Flink job runs in the IDE but throws `ClassNotFoundException` when
submitted to the cluster. Walk through what a fat jar is, why the thin jar failed, which Gradle
scope Flink itself should use, and the two things that must be excluded/handled when merging jars."

**Prompt B.** "Explain relocation (shading): what problem it solves and why the manual `Jar` task we
built doesn't relocate but a production Shadow build would."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines a fat/uber jar as code + dependency classes in one artifact; thin jar = only your
      classes → `NoClassDefFoundError`.
- [ ] Uses `compileOnly`/provided for cluster-supplied libraries (Flink) and `implementation` for
      bundled deps.
- [ ] Names a `Main-Class` manifest attribute as the `java -jar` entry point.
- [ ] Explains excluding `META-INF/*.SF/.DSA/.RSA` (signature verification failure otherwise).
- [ ] Explains duplicate-entry handling (`DuplicatesStrategy`) and that service files should ideally
      be *merged*, not dropped.
- [ ] States relocation renames bundled packages to prevent version clashes (why Shadow exists).
- [ ] (Bonus) Notes a correct fat jar has exactly one copy of each class and is runnable with the
      vendored JDK.
