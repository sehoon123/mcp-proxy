package net.portswigger

import io.modelcontextprotocol.kotlin.sdk.client.Client
import io.modelcontextprotocol.kotlin.sdk.client.ClientOptions
import io.modelcontextprotocol.kotlin.sdk.client.StdioClientTransport
import io.modelcontextprotocol.kotlin.sdk.types.CallToolResult
import io.modelcontextprotocol.kotlin.sdk.types.ClientCapabilities
import io.modelcontextprotocol.kotlin.sdk.types.CreateMessageRequest
import io.modelcontextprotocol.kotlin.sdk.types.CreateMessageResult
import io.modelcontextprotocol.kotlin.sdk.types.EmptyResult
import io.modelcontextprotocol.kotlin.sdk.types.Implementation
import io.modelcontextprotocol.kotlin.sdk.types.Method
import io.modelcontextprotocol.kotlin.sdk.types.Role
import io.modelcontextprotocol.kotlin.sdk.types.TextContent
import io.modelcontextprotocol.kotlin.sdk.types.Tool
import kotlinx.io.asSink
import kotlinx.io.asSource
import kotlinx.io.buffered
import java.io.InputStream
import java.io.OutputStream

class TestStdioMcpClient {
    private val mcp = Client(
        clientInfo = Implementation(name = "test-stdio-client", version = "1.0.0"),
        options = ClientOptions(
            capabilities = ClientCapabilities(
                sampling = ClientCapabilities.Sampling(),
            ),
        ),
    ).apply {
        setRequestHandler<CreateMessageRequest>(Method.Defined.SamplingCreateMessage) { _, _ ->
            CreateMessageResult(
                role = Role.Assistant,
                content = TextContent("sampled through stdio"),
                model = "test-model",
            )
        }
    }

    suspend fun connectToServer(input: InputStream, output: OutputStream) {
        mcp.connect(
            StdioClientTransport(
                input = input.asSource().buffered(),
                output = output.asSink().buffered(),
            ),
        )
    }

    suspend fun ping(): EmptyResult = mcp.ping()

    suspend fun listTools(): List<Tool> = mcp.listTools().tools

    suspend fun callTool(name: String): CallToolResult = mcp.callTool(name, emptyMap())

    suspend fun close() {
        mcp.close()
    }
}
