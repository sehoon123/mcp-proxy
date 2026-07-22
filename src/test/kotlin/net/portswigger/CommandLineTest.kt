package net.portswigger

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CommandLineTest {
    private val validToken = "a".repeat(43)

    @Test
    fun `uses default Streamable HTTP URL when no arguments are provided`() {
        val config = parseCommandLineArgs(emptyArray())

        assertEquals("http://127.0.0.1:9876/mcp", config.mcpUrl)
        assertFalse(config.usedLegacySseArgument)
    }

    @Test
    fun `uses a custom numeric loopback Streamable HTTP URL`() {
        val config = parseCommandLineArgs(arrayOf("--mcp-url", "https://127.0.0.1:8080/mcp"))

        assertEquals("https://127.0.0.1:8080/mcp", config.mcpUrl)
        assertFalse(config.usedLegacySseArgument)
    }

    @Test
    fun `adds mcp path to a root URL`() {
        assertEquals("http://127.0.0.1:9876/mcp", normalizeMcpUrl("http://127.0.0.1:9876"))
        assertEquals("http://127.0.0.1:9876/mcp", normalizeMcpUrl("http://127.0.0.1:9876/"))
    }

    @Test
    fun `supports an IPv6 loopback endpoint`() {
        assertEquals("http://[::1]:9876/mcp", normalizeMcpUrl("http://[::1]:9876"))
    }

    @Test
    fun `migrates the legacy sse-url argument to Streamable HTTP`() {
        val config = parseCommandLineArgs(arrayOf("--sse-url", "http://127.0.0.1:9876"))

        assertEquals("http://127.0.0.1:9876/mcp", config.mcpUrl)
        assertTrue(config.usedLegacySseArgument)
    }

    @Test
    fun `rejects unsafe or malformed URLs`() {
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("file:///tmp/socket") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://user:secret@127.0.0.1:9876/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://127.0.0.1:9876/mcp?token=secret") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://localhost:9876/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://0.0.0.0:9876/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("https://example.com:8080/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://127.0.0.1:9876/custom-mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://127.0.0.1:09876/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://127.0.0.1:/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://127.0.0.1:9876/mc%70") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("not a URL") }
    }

    @Test
    fun `reads bearer token from an environment variable without exposing it in toString`() {
        val config = parseCommandLineArgs(
            arrayOf("--bearer-token-env", "BURP_MCP_TOKEN"),
            mapOf("BURP_MCP_TOKEN" to validToken),
        )

        assertEquals(validToken, config.bearerToken)
        assertFalse(config.toString().contains(validToken))
        assertTrue(config.toString().contains("<redacted>"))
    }

    @Test
    fun `accepts a direct bearer token for custom launchers`() {
        assertEquals(
            validToken,
            parseCommandLineArgs(arrayOf("--bearer-token", validToken)).bearerToken,
        )
    }

    @Test
    fun `startup error summaries redact credentials paths and control characters`() {
        val summary = safeProxyError(
            IllegalStateException(
                "Bearer $validToken failed at /home/alice/private/config.json\r\ntoken=another-secret"
            )
        )

        assertFalse(summary.contains(validToken))
        assertFalse(summary.contains("/home/alice"))
        assertFalse(summary.contains("another-secret"))
        assertFalse(summary.contains('\n'))
        assertTrue(summary.length <= 384)
    }

    @Test
    fun `rejects missing values unknown arguments and invalid bearer options`() {
        assertThrows<IllegalArgumentException> { parseCommandLineArgs(arrayOf("--mcp-url")) }
        assertThrows<IllegalArgumentException> { parseCommandLineArgs(arrayOf("--unknown")) }
        assertThrows<IllegalArgumentException> {
            parseCommandLineArgs(arrayOf("--bearer-token-env", "MISSING"), emptyMap())
        }
        assertThrows<IllegalArgumentException> {
            parseCommandLineArgs(arrayOf("--bearer-token", validToken, "--bearer-token", validToken))
        }
        assertThrows<IllegalArgumentException> {
            parseCommandLineArgs(arrayOf("--bearer-token", "contains whitespace"))
        }
    }
}
