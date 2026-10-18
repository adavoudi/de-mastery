// MUTANT: fatJar bundles only our own classes, not the dependencies (a "fat" jar that isn't fat).

plugins {
    java
}

val depsDir = project.property("depsDir") as String
val mainClassName = project.property("mainClass") as String

dependencies {
    implementation(fileTree(depsDir) { include("*.jar") })
}

tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes("Main-Class" to mainClassName)
    }
    from(sourceSets.main.get().output)
    // BUG: never adds the dependency classes -> Avro is missing at runtime.
}
