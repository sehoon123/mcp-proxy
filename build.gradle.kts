plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.serialization") version "2.3.21"
    id("io.ktor.plugin") version "3.4.3"
}

group = "net.portswigger"
version = "2.0.0"

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
