# JVM-03 · Interview drills

**1. "Thin jar vs fat/uber jar?"**
> A thin jar has only your compiled classes. It needs its dependencies on the classpath at runtime.
> A fat/uber jar has your classes plus all dependency classes merged in. The cluster runs it with
> nothing else. Flink and Kafka Streams jobs ship as fat jars.

**2. "Which dependency scope for Flink in a Flink job, and why?"**
> Use `compileOnly` (Maven `provided`). The cluster already has Flink. If you bundle it, you get
> classloader conflicts, `LinkageError`/`NoSuchMethodError`, and a larger jar. Bundle only your own
> libraries, with `implementation`.

**3. "You built a fat jar and `java -jar` says 'Invalid signature file digest'. Cause and fix?"**
> A merged dependency was a signed jar. Its `META-INF/*.SF/.DSA/.RSA` files no longer match the
> changed archive. Exclude those signature files when you assemble the fat jar.

**4. "What is shading/relocation and when do you need it?"**
> Relocation renames bundled packages (for example `com.google.guava` → `app.shaded.guava`). Then
> your dependency version cannot clash with a different version that is already on the runtime
> classpath. You need it when your fat jar and the platform both ship the same library at
> different versions. The Shadow plugin does relocation. It also merges `META-INF/services` files.

**5. "`java -jar app.jar` says 'no main manifest attribute'. What's wrong?"**
> The manifest has no `Main-Class`. Add `manifest { attributes("Main-Class" to "...") }` to the jar
> task. Then `java -jar` knows the entry point.

**6. "Gradle `implementation` vs `api`?"**
> Both are compile and runtime scopes. But `api` *exposes* the dependency to downstream consumers
> of your library (it is on their compile classpath). `implementation` hides it. This gives faster
> recompiles and cleaner boundaries. For an application or job, the choice rarely matters. For a
> library, prefer `implementation`, unless the type appears in your public API.
