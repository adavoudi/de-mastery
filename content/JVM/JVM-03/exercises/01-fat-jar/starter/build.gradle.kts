// TODO: produce a runnable fat jar.
//
// Properties passed by the grader (both guaranteed present):
//   depsDir   -> absolute path to a dir containing avro-1.12.0.jar (+ transitive jars)
//   mainClass -> "de.mastery.jvm03.Job"
//
// Right now this only compiles the code and builds the DEFAULT (thin) jar, which does NOT
// contain Avro, so `java -jar build/libs/jvm03-job-all.jar` would fail (and there's no
// fatJar task yet). Fix it: register a `fatJar` Jar task that bundles the dependency
// classes, sets Main-Class, and handles duplicates/signatures.

plugins {
    java
}

val depsDir = project.property("depsDir") as String
val mainClassName = project.property("mainClass") as String

dependencies {
    implementation(fileTree(depsDir) { include("*.jar") })
}

// (no fatJar task yet)
