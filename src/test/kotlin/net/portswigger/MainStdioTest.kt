package net.portswigger

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.nio.file.Files
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.io.path.readText
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Timeout(30, unit = TimeUnit.SECONDS)
class MainStdioTest {
    private val server = TestMcpServer()
    private var process: Process? = null

    @AfterEach
    fun cleanup() {
        process?.let {
            runCatching { it.outputStream.close() }
            if (!it.waitFor(3, TimeUnit.SECONDS)) {
                it.destroy()
                if (!it.waitFor(3, TimeUnit.SECONDS)) it.destroyForcibly()
            }
        }
        server.stop()
    }

    @Test
    fun `main never writes logging banners to the stdio protocol`() {
        val port = server.start()
        val errorLog = Files.createTempFile("mcp-proxy-main-", ".log")
        val javaExecutable = java.nio.file.Path.of(
            System.getProperty("java.home"),
            "bin",
            if (System.getProperty("os.name").startsWith("Windows")) "java.exe" else "java",
        )
        process = ProcessBuilder(
            javaExecutable.toString(),
            "-cp",
            System.getProperty("java.class.path"),
            "net.portswigger.MainKt",
            "--mcp-url",
            "http://127.0.0.1:$port/mcp",
        )
            .redirectError(errorLog.toFile())
            .apply { environment()["KOTLIN_LOGGING_STARTUP_MESSAGE"] = "true" }
            .start()

        val request = """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"stdio-cleanliness-test","version":"1.0"}}}"""
        process!!.outputStream.bufferedWriter().apply {
            write(request)
            newLine()
            flush()
        }

        val reader = process!!.inputStream.bufferedReader()
        val firstLine = CompletableFuture.supplyAsync { reader.readLine() }.get(15, TimeUnit.SECONDS)
            ?: error("Proxy closed stdout before responding; stderr=${errorLog.readText()}")
        assertTrue(firstLine.startsWith("{"), "stdout must start with JSON, got: $firstLine")
        val response = Json.parseToJsonElement(firstLine).jsonObject
        assertEquals("1", response["id"]?.jsonPrimitive?.content)
        assertEquals("1.0.0", response["result"]?.jsonObject
            ?.get("serverInfo")?.jsonObject
            ?.get("version")?.jsonPrimitive?.content)
    }
}
