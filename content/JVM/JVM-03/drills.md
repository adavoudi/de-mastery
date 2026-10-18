# JVM-03 · Interview drills

**1. "Thin jar vs fat/uber jar?"**
> Thin = only your compiled classes; needs its dependencies on the classpath at runtime. Fat/uber =
> your classes plus all dependency classes merged in, so the cluster runs it with nothing else.
> Flink/Kafka Streams jobs ship fat.

**2. "Which dependency scope for Flink in a Flink job, and why?"**
> `compileOnly` (Maven `provided`). The cluster already has Flink; bundling it causes classloader
> conflicts, `LinkageError`/`NoSuchMethodError`, and bloat. Bundle only your own libraries with
> `implementation`.

**3. "You built a fat jar and `java -jar` says 'Invalid signature file digest'. Cause and fix?"**
> A merged dependency was a signed jar; its `META-INF/*.SF/.DSA/.RSA` no longer match the changed
> archive. Exclude those signature files when assembling the fat jar.

**4. "What is shading/relocation and when do you need it?"**
> Renaming bundled packages (e.g. `com.google.guava` → `app.shaded.guava`) so your dependency
> version can't clash with a different one already on the runtime classpath. Needed when your fat
> jar and the platform both ship a common library at different versions. The Shadow plugin does
> relocation and merges `META-INF/services` files.

**5. "`java -jar app.jar` says 'no main manifest attribute'. What's wrong?"**
> The manifest has no `Main-Class`. Add `manifest { attributes("Main-Class" to "...") }` to the jar
> task so `java -jar` knows the entry point.

**6. "Gradle `implementation` vs `api`?"**
> Both are compile+runtime, but `api` *exposes* the dependency to downstream consumers of your
> library (it's on their compile classpath); `implementation` hides it (faster recompiles, cleaner
> boundaries). For an application/job it rarely matters; for a library, prefer `implementation`
> unless the type appears in your public API.
