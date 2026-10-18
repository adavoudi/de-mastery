# JVM-01 · Java for Python developers

## Why Java at all

You already think in SQL and Python. So why learn Java? The tools you are moving toward are
*written in Java and only speak Java at their core*. Kafka Streams has no Python API. The Flink
DataStream API, its state backends, and almost all production Flink code are Java. When a Kafka
producer throws an error, you read a Java stack trace. When a Flink job runs out of memory, you tune
a JVM heap. Most Kafka client examples are in Java.

The goal of this track is **productive Java for data work**. You learn enough to do three things:

1. Read the source code.
2. Write a connector or a Streams topology.
3. Package a job.

This track is *not* about enterprise Java with Spring and reflection magic.

The good news: modern Java (17/21) is much closer to Python than the Java of 2010.

- Records give you dataclass-like value types in one line.
- `var` gives you local type inference.
- `Optional` replaces "return None".
- The Streams API is a lazy, chainable pipeline. It feels like a typed, compiled version of a Python
  generator expression.

## How Java differs from Python (the mental model)

**Static typing, compiled.** Every variable, parameter and return value has a type. The compiler
checks the types *before* the program runs. Suppose you call `.toUppercase()` (wrong) instead of
`.toUpperCase()`. This is a compile error, not a runtime `AttributeError`. You write a little more
code, and the compiler catches a whole class of bugs early. A dbt `--warn-error` or a typed schema
gives you the same safety.

```java
int n = 5;                 // primitive
long big = 5_000_000_000L; // underscores allowed; L for long literals
double d = 1.5;
String s = "hi";           // objects, not primitives
var list = new ArrayList<String>();  // 'var' infers the type on the left
```

**Everything lives in a class, in a package.** A file `src/main/java/de/mastery/Money.java`
declares `package de.mastery;` and a `public class Money`. The folder path must match the package.
Every build tool (Maven, Gradle) and every Java exercise in this track uses this layout.

**Records = dataclasses.** A `record` is an immutable value type. Java generates the constructor,
the accessors, `equals`, `hashCode` and `toString` for you:

```java
public record Order(String currency, double amount) {}
Order o = new Order("USD", 10.0);
o.currency();   // accessor is currency(), not o.currency
```

That one line is the Java equivalent of a frozen `@dataclass`. Accessors are *methods*
(`o.currency()`), because Java has no properties.

**Generics = typed containers.** Examples are `List<String>` and `Map<String, Double>`. The `<...>`
is the element type. The compiler checks it. It is like `list[str]` in a type hint, but Java
enforces it. Java has no bare `list` or `dict`. You always say what is inside.

**Interfaces = structural contracts (Protocols/ABCs).** An `interface` declares method signatures.
A class `implements` it. `Comparable<T>`, `Runnable` and `Function<T,R>` are interfaces. A
**lambda** `x -> x * 2` is a short way to implement a one-method (functional) interface. It is the
compiled cousin of a Python `lambda`.

**`Optional<T>` instead of `None`.** Java references can be `null`. If you dereference `null`, Java
throws `NullPointerException` (the Java version of `TypeError: NoneType`). `Optional<T>` shows
"maybe absent" in the type, so you handle the case on purpose:

```java
Optional<String> name = lookup(id);
String shown = name.orElse("unknown");         // default
name.map(String::toUpperCase).ifPresent(...);  // transform if present
```

**Exceptions: checked vs unchecked.** Python has only runtime exceptions. Java has two kinds:

- *Unchecked* exceptions need no declaration. Examples are `RuntimeException`,
  `IllegalArgumentException` and `NullPointerException`.
- *Checked* exceptions (for example `IOException`) must be caught. Or you declare them with `throws`
  on the method.

The compiler makes you acknowledge checked failures. This is annoying at first. It is also the
reason why Kafka and Flink APIs force you to deal with I/O errors.

```java
public static int parse(String s) {
    if (s == null) throw new IllegalArgumentException("null input"); // unchecked
    return Integer.parseInt(s);  // throws NumberFormatException (unchecked) on bad input
}
```

**No truthiness, explicit equality.** `if (x)` requires a `boolean`. An empty list is not "false".
Also, `==` on objects compares *references*, not contents. Use `.equals()` for value equality
(`"a".equals(other)`). This is the most common beginner bug for people who come from Python.

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

- **Don't** write Scala-style or framework-heavy Java here. Keep methods small. Prefer records and
  plain functions. (This track never requires Scala. SPK-05 has a short "reading Scala" sidebar
  only.)
- **Don't** translate Python idioms literally. These three habits will cause bugs in Java:
  mutating a list while you iterate, relying on truthiness, and comparing strings with `==`. Learn
  the Java idiom (`equals`, typed loops, `Optional`).
- **Don't** use Java where PySpark or dbt already solves the problem. This track is for the
  *engine-internal* work (Streams, connectors, Flink jobs). It is not for re-implementing a staging
  model.

## Further reading (official)

- The Java Language — "Learn Java" basics: <https://dev.java/learn/>
- Records: <https://docs.oracle.com/en/java/javase/21/language/records.html>
- `Optional` API: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/Optional.html>
- Exceptions tutorial: <https://docs.oracle.com/javase/tutorial/essential/exceptions/>
- `java.lang.Object.equals`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Object.html#equals(java.lang.Object)>
