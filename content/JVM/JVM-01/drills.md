# JVM-01 · Interview drills

**1. "Difference between `==` and `.equals()` for objects?"**
> `==` compares references. It asks: are these the same object? `.equals()` compares values. It
> asks: do these mean the same thing? For `String` and records, use `.equals()`. `==` is fine for
> primitives (`int`, `boolean`) and for identity checks like `x == null`.

**2. "What's a record and when do you use it?"**
> A record is an immutable value type: `record Order(String currency, double amount) {}`. Java
> generates the constructor, the accessors, and value-based `equals`/`hashCode`/`toString`. Use it
> for data carriers (events, DTOs, keys). Use it where you would use a frozen dataclass in Python.
> You can add a compact constructor for validation.

**3. "Checked vs unchecked exception — give an example of each and when you'd throw which."**
> You must catch a checked exception (`IOException`) or declare it with `throws`. Use it for
> recoverable I/O conditions that the caller should plan for. Unchecked exceptions
> (`IllegalArgumentException`, `NullPointerException`) extend `RuntimeException`. Throw
> `IllegalArgumentException` for a bad argument that is a programming error. If you use too many
> checked exceptions, APIs become noisy.

**4. "How does `Optional` improve on returning `null`?"**
> It puts "may be absent" in the type signature. The caller must handle the empty case on purpose,
> with `orElse`, `map`, `ifPresent`, or `orElseThrow`. It prevents the silent
> `NullPointerException` that a bare `null` return invites. Don't use `Optional` for fields or
> collection elements. Prefer empty collections there.

**5. "A Python dev writes `if (orders)` to check a non-empty list and it won't compile. Why?"**
> Java has no truthiness. An `if` needs a `boolean`. Write `if (!orders.isEmpty())`. Java also has
> no implicit conversion between int and bool. The Java idiom is to be explicit.

**6. "Why must `de/mastery/Money.java` sit under that exact directory path?"**
> The file declares `package de.mastery;`. Java maps packages to directories under the source root.
> The compiler and every build tool rely on this. If the path does not match, the build fails to
> compile or to find the class.
