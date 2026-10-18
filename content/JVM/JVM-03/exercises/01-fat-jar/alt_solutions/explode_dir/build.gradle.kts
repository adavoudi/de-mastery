// Alternate: explode dependency jars into a staging dir, then jar our classes + the staged classes.

plugins {
    java
}

val depsDir = project.property("depsDir") as String
val mainClassName = project.property("mainClass") as String

dependencies {
    implementation(fileTree(depsDir) { include("*.jar") })
}

val stageDeps = tasks.register<Copy>("stageDeps") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { zipTree(it) })
    into(layout.buildDirectory.dir("deps-exploded"))
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}

tasks.register<Jar>("fatJar") {
    dependsOn(stageDeps)
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes("Main-Class" to mainClassName)
    }
    from(sourceSets.main.get().output)
    from(layout.buildDirectory.dir("deps-exploded"))
}
