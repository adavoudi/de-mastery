// MUTANT: fat jar bundles everything but forgets the Main-Class manifest attribute.

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
    // BUG: no manifest Main-Class -> `java -jar` prints "no main manifest attribute".
    from(sourceSets.main.get().output)
    from(configurations.runtimeClasspath.get().map {
        if (it.isDirectory) it else zipTree(it)
    })
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
