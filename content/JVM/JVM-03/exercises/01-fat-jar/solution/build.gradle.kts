// Reference: a runnable fat jar built with a plain Jar task (offline, no Shadow plugin).

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
    manifest {
        attributes("Main-Class" to mainClassName)
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    // our compiled classes
    from(sourceSets.main.get().output)
    // + all dependency classes (unzipped)
    from(configurations.runtimeClasspath.get().map {
        if (it.isDirectory) it else zipTree(it)
    })
    // drop signed-jar signatures so the merged archive verifies
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
