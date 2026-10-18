# JVM-03 · Explain it back (Feynman)

**Prompt A.** "A teammate's Flink job runs in the IDE. It throws `ClassNotFoundException` when you
submit it to the cluster. Explain what a fat jar is. Explain why the thin jar failed. Say which
Gradle scope Flink itself should use. Name the two things that you must exclude or handle when you
merge jars."

**Prompt B.** "Explain relocation (shading). Say what problem it solves. Say why the manual `Jar`
task that we built does not relocate, but a production Shadow build would."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines a fat/uber jar as code plus dependency classes in one artifact. A thin jar has only
      your classes, which leads to `NoClassDefFoundError`.
- [ ] Uses `compileOnly`/provided for libraries that the cluster supplies (Flink). Uses
      `implementation` for dependencies that you bundle.
- [ ] Names a `Main-Class` manifest attribute as the `java -jar` entry point.
- [ ] Explains why to exclude `META-INF/*.SF/.DSA/.RSA` (otherwise signature verification fails).
- [ ] Explains how to handle duplicate entries (`DuplicatesStrategy`). Says that you should
      ideally *merge* service files, not drop them.
- [ ] States that relocation renames bundled packages to prevent version clashes (why Shadow
      exists).
- [ ] (Bonus) Notes that a correct fat jar has exactly one copy of each class. Notes that you can
      run it with the vendored JDK.
