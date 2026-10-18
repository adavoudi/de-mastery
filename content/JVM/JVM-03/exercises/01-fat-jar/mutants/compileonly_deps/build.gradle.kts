// MUTANT: declares Avro as compileOnly, so it is NOT on runtimeClasspath and never bundled.

plugins {
    java
}

val depsDir = project.property("depsDir") as String
val mainClassName = project.property("mainClass") as String

dependencies {
    // BUG: compileOnly means "provided by the runtime" -> excluded from runtimeClasspath,
    // so the fat jar does not contain Avro even though we try to add runtimeClasspath.
    compileOnly(fileTree(depsDir) { include("*.jar") })
}

tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes("Main-Class" to mainClassName)
    }
    from(sourceSets.main.get().output)
    from(configurations.runtimeClasspath.get().map {
        if (it.isDirectory) it else zipTree(it)
    })
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
