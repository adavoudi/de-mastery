# JVM-01 · Java for Python developers

## Why Java at all

You already think in SQL and Python. So why learn Java? Because the tools you are moving toward are
*written in Java and only speak Java at their core*. Kafka Streams has no Python API. The Flink
DataStream API, its state backends, and almost all production Flink code are Java. When a Kafka
producer throws, you read a Java stack trace. When a Flink job runs out of memory, you tune a JVM
heap. The CCDAK sample questions show Java. The goal of this track is **productive Java for data
work** — enough to read the source, write a connector or a Streams topology, and package a job —
*not* enterprise Java with Spring and reflection magic.

The good news: modern Java (17/21) is far closer to Python than the Java of 2010. Records give you
dataclass-like value types in one line, `var` gives you local type inference, `Optional` replaces
"return None", and the Streams API is a lazy, chainable pipeline that will feel like a typed,
compiled version of a Python generator expression.

## How Java differs from Python (the mental model)

**Static typing, compiled.** Every variable, parameter and return value has a type the compiler
checks *before* the program runs. A typo like calling `.toUppercase()` (wrong) instead of
`.toUpperCase()` is a compile error, not a runtime `AttributeError`. You trade some ceremony for
the compiler catching a whole class of bugs up front — the same safety a dbt `--warn-error` or a
typed schema gives you.

```java
int n = 5;                 // primitive
long big = 5_000_000_000L; // underscores allowed; L for long literals
double d = 1.5;
String s = "hi";           // objects, not primitives
var list = new ArrayList<String>();  // 'var' infers the type on the left
```

**Everything lives in a class, in a package.** A file `src/main/java/de/mastery/Money.java`
declares `package de.mastery;` and a `public class Money`. The folder path must match the package.
This is the layout every build tool (Maven, Gradle) and every Java exercise in this track uses.

**Records = dataclasses.** A `record` is an immutable value type with constructor, accessors,
`equals`, `hashCode` and `toString` generated for you:

```java
public record Order(String currency, double amount) {}
Order o = new Order("USD", 10.0);
o.currency();   // accessor is currency(), not o.currency
```

That one line is the Java equivalent of a frozen `@dataclass`. Note accessors are *methods*
(`o.currency()`), because Java has no properties.

**Generics = typed containers.** `List<String>`, `Map<String, Double>`. The `<...>` is the element
type, checked at compile time — like `list[str]` in a type hint, but enforced. There are no bare
`list`/`dict`; you always say what's inside.

**Interfaces = structural contracts (Protocols/ABCs).** An `interface` declares method signatures;
a class `implements` it. `Comparable<T>`, `Runnable`, `Function<T,R>` are interfaces. A **lambda**
`x -> x * 2` is shorthand for implementing a one-method (functional) interface — the compiled
cousin of a Python `lambda`.

**`Optional<T>` instead of `None`.** Java references can be `null`, and dereferencing `null` throws
`NullPointerException` (the Java `TypeError: NoneType`). `Optional<T>` makes "maybe absent" explicit
in the type, so you handle it deliberately:

```java
Optional<String> name = lookup(id);
String shown = name.orElse("unknown");         // default
name.map(String::toUpperCase).ifPresent(...);  // transform if present
```

**Exceptions: checked vs unchecked.** Python has only runtime exceptions. Java splits them:
*unchecked* (`RuntimeException`, e.g. `IllegalArgumentException`, `NullPointerException`) need no
declaration; *checked* (e.g. `IOException`) must be either caught or declared with `throws` on the
method. The compiler enforces that you acknowledge checked failures — annoying at first, but it is
why Kafka/Flink APIs force you to deal with I/O errors.

```java
public static int parse(String s) {
    if (s == null) throw new IllegalArgumentException("null input"); // unchecked
    return Integer.parseInt(s);  // throws NumberFormatException (unchecked) on bad input
}
```

**No truthiness, explicit equality.** `if (x)` requires a `boolean`; there is no "empty list is
false". And `==` on objects compares *references*, not contents — use `.equals()` for value
equality (`"a".equals(other)`), the single most common beginner bug coming from Python.

## From your world

| Python / dbt | Java | Note |
|---|---|---|
| `@dataclass(frozen=True)` | `record` | one-line immutable value type |
| `list[str]`, `dict[str,float]` | `List<String>`, `Map<String,Double>` | generics are enforced |
| type hints (optional) | types (mandatory, compiled) | compiler catches typos/mismatches |
| `None` / `Optional[T]` | `null` / `Optional<T>` | prefer `Optional` to signal absence |
| `lambda x: x*2` | `x -> x * 2` | implements a functional interface |
| `raise ValueError(...)` | `throw new IllegalArgumentException(...)` | unchecked |
| `a == b` (value) | `a.equals(b)` (value); `==` is identity | classic gotcha |
| `str(obj)` | `obj.toString()` | records generate a good one |

## When NOT to reach for Java

- **Don't** write Scala-style or framework-heavy Java here. Keep methods small, favor records and
  plain functions. (Scala is never required in this track; SPK-05 has a short "reading Scala"
  sidebar only.)
- **Don't** translate Python idioms literally. Mutating a list while iterating, relying on
  truthiness, or comparing strings with `==` will bite you. Learn the Java idiom (`equals`, typed
  loops, `Optional`).
- **Don't** use Java where PySpark/dbt already solves it. This track is for the *engine-internal*
  work (Streams, connectors, Flink jobs), not for re-implementing a staging model.

## Further reading (official)

- The Java Language — "Learn Java" basics: <https://dev.java/learn/>
- Records: <https://docs.oracle.com/en/java/javase/21/language/records.html>
- `Optional` API: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Optional.html>
- Exceptions tutorial: <https://docs.oracle.com/javase/tutorial/essential/exceptions/>
- `java.lang.Object.equals`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Object.html#equals(java.lang.Object)>
