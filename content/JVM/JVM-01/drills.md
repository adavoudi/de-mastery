# JVM-01 · Interview drills

**1. "Difference between `==` and `.equals()` for objects?"**
> `==` compares references (are these the same object?). `.equals()` compares value (do they mean
> the same thing?). For `String` and records, use `.equals()`. `==` is fine for primitives
> (`int`, `boolean`) and for identity checks like `x == null`.

**2. "What's a record and when do you use it?"**
> An immutable value type: `record Order(String currency, double amount) {}` generates the
> constructor, accessors, and value-based `equals`/`hashCode`/`toString`. Use it for data carriers —
> events, DTOs, keys — exactly where you'd use a frozen dataclass in Python. You can add a compact
> constructor for validation.

**3. "Checked vs unchecked exception — give an example of each and when you'd throw which."**
> Checked (`IOException`) must be caught or declared with `throws`; use for recoverable I/O
> conditions the caller should plan for. Unchecked (`IllegalArgumentException`,
> `NullPointerException`) extend `RuntimeException`; throw `IllegalArgumentException` for a bad
> argument that is a programming error. Over-using checked exceptions makes APIs noisy.

**4. "How does `Optional` improve on returning `null`?"**
> It puts "may be absent" in the type signature, so callers must consciously handle the empty case
> via `orElse`, `map`, `ifPresent`, or `orElseThrow`. It prevents the silent `NullPointerException`
> that a bare `null` return invites. Don't use `Optional` for fields or collection elements —
> prefer empty collections there.

**5. "A Python dev writes `if (orders)` to check a non-empty list and it won't compile. Why?"**
> Java has no truthiness; an `if` needs a `boolean`. Write `if (!orders.isEmpty())`. Likewise
> there's no implicit int↔bool. Being explicit is the Java idiom.

**6. "Why must `de/mastery/Money.java` sit under that exact directory path?"**
> Because the file declares `package de.mastery;`, and Java maps packages to directories under the
> source root. The compiler and every build tool rely on this, so a mismatched path fails to
> compile or to find the class.
