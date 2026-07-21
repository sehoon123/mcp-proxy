package net.portswigger

import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
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
}

private fun io.modelcontextprotocol.kotlin.sdk.types.CallToolResult.firstText(): String =
    (content.first() as TextContent).text

private class ProxyHarness(mcpUrl: String) {
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
        retryPolicy = RetryPolicy(
            maxAttempts = 20,
            initialDelay = 100.milliseconds,
            maxDelay = 1.seconds,
        ),
    )
    private val proxyJob = scope.launch { proxy.run() }

    suspend fun close() {
        runCatching { proxy.close() }
        runCatching { clientOutput.close() }
        runCatching { clientInput.close() }
        proxyJob.cancelAndJoin()
        scope.cancel()
    }

    companion object {
        private const val PIPE_BUFFER_SIZE = 1024 * 1024
    }
}
