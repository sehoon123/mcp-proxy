package net.portswigger

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CommandLineTest {
    @Test
    fun `uses default Streamable HTTP URL when no arguments are provided`() {
        val config = parseCommandLineArgs(emptyArray())

        assertEquals("http://localhost:9876/mcp", config.mcpUrl)
        assertFalse(config.usedLegacySseArgument)
    }

    @Test
    fun `uses a custom Streamable HTTP URL`() {
        val config = parseCommandLineArgs(arrayOf("--mcp-url", "https://example.com:8080/custom-mcp"))

        assertEquals("https://example.com:8080/custom-mcp", config.mcpUrl)
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
        val config = parseCommandLineArgs(arrayOf("--sse-url", "http://localhost:9876"))

        assertEquals("http://localhost:9876/mcp", config.mcpUrl)
        assertTrue(config.usedLegacySseArgument)
    }

    @Test
    fun `rejects unsafe or malformed URLs`() {
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("file:///tmp/socket") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://user:secret@localhost:9876/mcp") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("http://localhost:9876/mcp?token=secret") }
        assertThrows<IllegalArgumentException> { normalizeMcpUrl("not a URL") }
    }

    @Test
    fun `rejects missing values and unknown arguments`() {
        assertThrows<IllegalArgumentException> { parseCommandLineArgs(arrayOf("--mcp-url")) }
        assertThrows<IllegalArgumentException> { parseCommandLineArgs(arrayOf("--unknown")) }
    }
}
