import java.security.MessageDigest
import java.util.HexFormat

plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.serialization") version "2.3.21"
    id("com.gradleup.shadow") version "9.6.0"
    application
}

group = "net.portswigger"
version = "2.1.0"

application {
    mainClass.set("net.portswigger.MainKt")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.modelcontextprotocol:kotlin-sdk:0.14.0")
    implementation("io.ktor:ktor-client-cio:3.4.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.slf4j:slf4j-simple:2.0.17")

    testImplementation(kotlin("test"))
    testImplementation("io.ktor:ktor-server-cio:3.4.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(21)
}

tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

tasks.jar {
    manifest {
        attributes(
            mapOf(
                "Main-Class" to "net.portswigger.MainKt",
                "Implementation-Title" to project.name,
                "Implementation-Version" to project.version,
                "Implementation-Vendor" to "PortSwigger",
            )
        )
    }
}

tasks.shadowJar {
    archiveFileName.set("mcp-proxy-all.jar")
    mergeServiceFiles()
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude("META-INF/INDEX.LIST")
    exclude("META-INF/DEPENDENCIES")
    exclude("META-INF/NOTICE*")
    exclude("META-INF/LICENSE*")
    exclude("module-info.class")
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

val writeRuntimeComponents by tasks.registering {
    group = "documentation"
    description = "Writes deterministic resolved runtime component coordinates and artifact hashes."
    val outputFile = layout.buildDirectory.file("reports/runtime-components.txt")
    inputs.files(configurations.runtimeClasspath)
    outputs.file(outputFile)

    doLast {
        val lines = configurations.runtimeClasspath.get().resolvedConfiguration.resolvedArtifacts
            .map { artifact ->
                val id = artifact.moduleVersion.id
                val digest = MessageDigest.getInstance("SHA-256")
                artifact.file.inputStream().buffered().use { input ->
                    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                    while (true) {
                        val read = input.read(buffer)
                        if (read < 0) break
                        digest.update(buffer, 0, read)
                    }
                }
                "${id.group}:${id.name}:${id.version} ${HexFormat.of().formatHex(digest.digest())}"
            }
            .distinct()
            .sorted()
        val destination = outputFile.get().asFile
        destination.parentFile.mkdirs()
        destination.writeText(lines.joinToString("\n", postfix = "\n"), Charsets.UTF_8)
    }
}
