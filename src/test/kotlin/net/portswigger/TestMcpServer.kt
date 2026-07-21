package net.portswigger

import io.ktor.server.cio.CIO
import io.ktor.server.cio.CIOApplicationEngine
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.modelcontextprotocol.kotlin.sdk.server.Server
import io.modelcontextprotocol.kotlin.sdk.server.ServerOptions
import io.modelcontextprotocol.kotlin.sdk.server.mcpStreamableHttp
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.CreateMessageRequest
import io.modelcontextprotocol.kotlin.sdk.types.CreateMessageRequestParams
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.Role
import io.modelcontextprotocol.kotlin.sdk.types.SamplingMessage
import io.modelcontextprotocol.kotlin.sdk.types.ServerCapabilities
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration

internal class ToolConcurrencyProbe(private val executionDelay: Duration) {
    private val active = AtomicInteger()
    private val maximum = AtomicInteger()

    val maxActive: Int
        get() = maximum.get()

    suspend fun execute() {
        val nowActive = active.incrementAndGet()
        maximum.accumulateAndGet(nowActive) { current, candidate -> maxOf(current, candidate) }
        try {
            delay(executionDelay)
        } finally {
            active.decrementAndGet()
        }
    }
}

/** A stateful Streamable HTTP MCP server used by proxy integration tests. */
internal class TestMcpServer(private val concurrencyProbe: ToolConcurrencyProbe? = null) {
    private var port: Int = 0
    private var serverEngine: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null
    private var mcpServer: Server? = null

    fun start(port: Int = 0): Int {
        check(serverEngine == null) { "Test server is already running" }
        this.port = if (port == 0) findAvailablePort() else port
        val configuredServer = configureServer()
        mcpServer = configuredServer

        serverEngine = embeddedServer(CIO, host = "127.0.0.1", port = this.port) {
            mcpStreamableHttp(path = "/mcp") {
                configuredServer
            }
        }.start(wait = false)

        Thread.sleep(300)
        return this.port
    }

    fun activeSessionCount(): Int = mcpServer?.sessions?.size ?: 0

    fun stop() {
        serverEngine?.stop(500, 1_000)
        serverEngine = null
        runBlocking { mcpServer?.close() }
        mcpServer = null
        port = 0
    }

    private fun findAvailablePort(): Int = ServerSocket(0).use { it.localPort }

    private fun configureServer(): Server = Server(
        serverInfo = Implementation(name = "mcp-proxy-test-server", version = "1.0.0"),
        options = ServerOptions(
            capabilities = ServerCapabilities(
                tools = ServerCapabilities.Tools(listChanged = false),
            ),
        ),
    ).apply {
        addTool(
            name = "kotlin-sdk-tool",
            description = "Returns a deterministic test result",
        ) {
            CallToolResult(content = listOf(TextContent("Hello, world!")))
        }

        concurrencyProbe?.let { probe ->
            addTool(
                name = "concurrency-probe",
                description = "Tracks concurrent proxy requests",
            ) {
                probe.execute()
                CallToolResult(content = listOf(TextContent("completed")))
            }
        }

        addTool(
            name = "sampling-round-trip",
            description = "Asks the stdio client to sample a response",
        ) {
            val sampled = createMessage(
                CreateMessageRequest(
                    CreateMessageRequestParams(
                        messages = listOf(SamplingMessage(Role.User, TextContent("sample this"))),
                        maxTokens = 32,
                    ),
                ),
            )
            val text = (sampled.content.firstOrNull() as? TextContent)?.text
                ?: "unexpected sampling response"
            CallToolResult(content = listOf(TextContent(text)))
        }
    }

    companion object {
        fun findAvailablePort(): Int = ServerSocket(0).use { it.localPort }
    }
}
