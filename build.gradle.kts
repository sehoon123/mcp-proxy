import java.security.MessageDigest
import java.util.HexFormat

plugins {
    kotlin("jvm") version "2.4.20"
    kotlin("plugin.serialization") version "2.4.20"
    id("com.gradleup.shadow") version "9.6.0"
    application
}

group = "io.github.sehoon123"
version = "2.2.1"

application {
    mainClass.set("net.portswigger.MainKt")
}

repositories {
    mavenCentral()
}

dependencyLocking {
    lockAllConfigurations()
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
                "Implementation-Title" to "Independent MCP Bridge stdio proxy",
                "Implementation-Version" to project.version,
                "Implementation-Vendor" to "sehoon123",
                "Implementation-Source" to "https://github.com/sehoon123/mcp-proxy",
                "Fork-Status" to "Unofficial independent fork; not supported by PortSwigger",
            )
        )
    }
}

tasks.shadowJar {
    dependsOn("writeRuntimeComponents")
    archiveFileName.set("mcp-proxy-all.jar")
    mergeServiceFiles()
    from(layout.buildDirectory.file("reports/runtime-components.txt")) {
        into("META-INF/independent-mcp-bridge")
    }
    from(layout.projectDirectory.file("LICENSE")) {
        into("META-INF/legal")
        rename { "GPL-3.0.txt" }
    }
    from(layout.projectDirectory.file("NOTICE.md")) { into("META-INF/legal") }
    from(layout.projectDirectory.file("FORK_NOTICE.md")) { into("META-INF/legal") }
    from(layout.projectDirectory.file("THIRD_PARTY_NOTICES.md")) { into("META-INF/legal") }
    from(layout.projectDirectory.file("CORRESPONDING_SOURCE.md")) { into("META-INF/legal") }
    from(layout.projectDirectory.dir("legal/licenses")) { into("META-INF/legal/licenses") }
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
        check(lines.isNotEmpty()) { "Resolved runtime component report must not be empty" }
        val destination = outputFile.get().asFile
        destination.parentFile.mkdirs()
        destination.writeText(lines.joinToString("\n", postfix = "\n"), Charsets.UTF_8)
    }
}
