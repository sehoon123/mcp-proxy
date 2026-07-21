package net.portswigger

import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import java.io.PipedInputStream
import java.io.PipedOutputStream
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class StreamableHttpProxyIntegrationTest {
    @Test
    fun `relays initialization tools ping and tool calls over Streamable HTTP`() = runBlocking {
        val server = TestMcpServer()
        val port = server.start()
        val harness = ProxyHarness("http://127.0.0.1:$port/mcp")
        val client = TestStdioMcpClient()

        try {
            withTimeout(10.seconds) { client.connectToServer(harness.clientInput, harness.clientOutput) }

            assertTrue(client.listTools().any { it.name == "kotlin-sdk-tool" })
            assertEquals(Unit, client.ping().let { Unit })
            assertEquals("Hello, world!", client.callTool("kotlin-sdk-tool").firstText())
        } finally {
            runCatching { client.close() }
            harness.close()
            server.stop()
        }
    }

    @Test
    fun `transparently relays server initiated sampling requests without deadlock`() = runBlocking {
        val server = TestMcpServer()
        val port = server.start()
        val harness = ProxyHarness("http://127.0.0.1:$port/mcp")
        val client = TestStdioMcpClient()

        try {
            withTimeout(10.seconds) { client.connectToServer(harness.clientInput, harness.clientOutput) }
            val result = withTimeout(10.seconds) { client.callTool("sampling-round-trip") }

            assertEquals("sampled through stdio", result.firstText())
        } finally {
            runCatching { client.close() }
            harness.close()
            server.stop()
        }
    }

    @Test
    fun `bounds concurrent upstream requests and backpressures a burst`() = runBlocking {
        val probe = ToolConcurrencyProbe(250.milliseconds)
        val server = TestMcpServer(concurrencyProbe = probe)
        val port = server.start()
        val harness = ProxyHarness(
            "http://127.0.0.1:$port/mcp",
            maxConcurrentRequests = 3,
            requestQueueCapacity = 4,
        )
        val client = TestStdioMcpClient()

        try {
            withTimeout(10.seconds) { client.connectToServer(harness.clientInput, harness.clientOutput) }
            withTimeout(15.seconds) {
                (1..12).map {
                    async { client.callTool("concurrency-probe") }
                }.awaitAll()
            }

            assertEquals(3, probe.maxActive)
        } finally {
            runCatching { client.close() }
            harness.close()
            server.stop()
        }
    }

    @Test
    fun `does not retry an ambiguously delivered custom request`() = runBlocking {
        val server = RawCustomResultServer()
        val port = server.start()
        val harness = ProxyHarness(
            "http://127.0.0.1:$port/mcp",
            retryPolicy = RetryPolicy(
                maxAttempts = 3,
                initialDelay = 50.milliseconds,
                maxDelay = 100.milliseconds,
            ),
        )
        val writer = harness.clientOutput.bufferedWriter()
        val reader = harness.clientInput.bufferedReader()

        suspend fun send(message: String) = withContext(Dispatchers.IO) {
            writer.write(message)
            writer.newLine()
            writer.flush()
        }

        suspend fun receive() = withContext(Dispatchers.IO) {
            reader.readLine() ?: error("Proxy closed stdout before responding")
        }

        try {
            send("""{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"custom-result-test","version":"1.0"}}}""")
            val initializeResponse = Json.parseToJsonElement(withTimeout(5.seconds) { receive() }).jsonObject
            assertEquals("1", initializeResponse["id"]?.jsonPrimitive?.content)

            send("""{"jsonrpc":"2.0","method":"notifications/initialized"}""")
            send("""{"jsonrpc":"2.0","id":2,"method":"custom/echo","params":{"value":"test"}}""")

            val errorResponse = Json.parseToJsonElement(withTimeout(5.seconds) { receive() }).jsonObject
            assertEquals("2", errorResponse["id"]?.jsonPrimitive?.content)
            assertTrue("error" in errorResponse)
            delay(250.milliseconds)
            assertEquals(1, server.customCallCount)
        } finally {
            runCatching { writer.close() }
            runCatching { reader.close() }
            harness.close()
            server.stop()
        }
    }

    @Test
    fun `buffers stdio initialization while Burp starts`() = runBlocking {
        val port = TestMcpServer.findAvailablePort()
        val harness = ProxyHarness("http://127.0.0.1:$port/mcp")
        val client = TestStdioMcpClient()
        val server = TestMcpServer()

        try {
            val connecting = async {
                withTimeout(15.seconds) { client.connectToServer(harness.clientInput, harness.clientOutput) }
            }

            delay(750.milliseconds)
            server.start(port)
            connecting.await()

            assertTrue(client.listTools().any { it.name == "kotlin-sdk-tool" })
        } finally {
            runCatching { client.close() }
            harness.close()
            server.stop()
        }
    }

    @Test
    fun `restores the HTTP session after Burp restarts`() = runBlocking {
        val server = TestMcpServer()
        val port = server.start()
        val harness = ProxyHarness("http://127.0.0.1:$port/mcp")
        val client = TestStdioMcpClient()

        try {
            withTimeout(10.seconds) { client.connectToServer(harness.clientInput, harness.clientOutput) }
            assertEquals("Hello, world!", client.callTool("kotlin-sdk-tool").firstText())

            server.stop()
            delay(300.milliseconds)
            server.start(port)

            val result = withTimeout(15.seconds) { client.callTool("kotlin-sdk-tool") }
            assertEquals("Hello, world!", result.firstText())
        } finally {
            runCatching { client.close() }
            harness.close()
            server.stop()
        }
    }

    @Test
    fun `graceful proxy shutdown terminates the HTTP session`() = runBlocking {
        val server = TestMcpServer()
        val port = server.start()
        val harness = ProxyHarness("http://127.0.0.1:$port/mcp")
        val client = TestStdioMcpClient()

        try {
            withTimeout(10.seconds) { client.connectToServer(harness.clientInput, harness.clientOutput) }
            assertEquals(1, server.activeSessionCount())

            client.close()
            harness.close()
            withTimeout(5.seconds) {
                while (server.activeSessionCount() != 0) delay(25.milliseconds)
            }

            assertEquals(0, server.activeSessionCount())
        } finally {
            runCatching { client.close() }
            harness.close()
            server.stop()
        }
    }
}

private fun io.modelcontextprotocol.kotlin.sdk.types.CallToolResult.firstText(): String =
    (content.first() as TextContent).text

private class ProxyHarness(
    mcpUrl: String,
    maxConcurrentRequests: Int = 16,
    requestQueueCapacity: Int = 64,
    retryPolicy: RetryPolicy = RetryPolicy(
        maxAttempts = 20,
        initialDelay = 100.milliseconds,
        maxDelay = 1.seconds,
    ),
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val clientToProxy = PipedOutputStream()
    private val proxyInput = PipedInputStream(clientToProxy, PIPE_BUFFER_SIZE)
    private val proxyToClient = PipedOutputStream()
    val clientInput = PipedInputStream(proxyToClient, PIPE_BUFFER_SIZE)
    val clientOutput: PipedOutputStream = clientToProxy

    private val proxy = StreamableHttpProxy(
        mcpUrl = mcpUrl,
        input = proxyInput,
        output = proxyToClient,
        retryPolicy = retryPolicy,
        maxConcurrentRequests = maxConcurrentRequests,
        requestQueueCapacity = requestQueueCapacity,
    )
    private val proxyJob = scope.launch { proxy.run() }

    suspend fun close() {
        runCatching { proxy.close() }
        runCatching { clientOutput.close() }
        runCatching { clientInput.close() }
        withTimeoutOrNull(5.seconds) { proxyJob.join() }
        if (proxyJob.isActive) proxyJob.cancelAndJoin()
        scope.cancel()
    }

    companion object {
        private const val PIPE_BUFFER_SIZE = 1024 * 1024
    }
}
