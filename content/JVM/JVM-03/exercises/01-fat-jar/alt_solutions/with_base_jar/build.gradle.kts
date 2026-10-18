// Alternate: build fatJar by reusing the base `jar` spec with `with(...)`, then adding deps.

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
    // reuse everything the default jar already includes (our classes)...
    with(tasks.named<Jar>("jar").get())
    // ...and add the dependency jar contents
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith(".jar") }
            .map { zipTree(it) }
    })
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
