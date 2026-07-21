package net.portswigger

import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.net.URI
import kotlin.system.exitProcess

private val logger = LoggerFactory.getLogger("net.portswigger.Main")

const val DEFAULT_MCP_URL = "http://localhost:9876/mcp"

private val usage = """
    Burp MCP stdio proxy

    Usage:
      java -jar mcp-proxy-all.jar [--mcp-url <url>]

    Options:
      --mcp-url <url>  Streamable HTTP MCP endpoint (default: $DEFAULT_MCP_URL)
      --sse-url <url>  Deprecated alias. A root URL is automatically migrated to /mcp.
      -h, --help       Show this help.
""".trimIndent()

data class ProxyConfig(
    val mcpUrl: String = DEFAULT_MCP_URL,
    val usedLegacySseArgument: Boolean = false,
)

/** Parse and validate command-line arguments without writing to stdout. */
fun parseCommandLineArgs(args: Array<String>): ProxyConfig {
    var mcpUrl = DEFAULT_MCP_URL
    var usedLegacySseArgument = false
    var index = 0

    while (index < args.size) {
        val option = args[index]
        when (option) {
            "--mcp-url", "--sse-url" -> {
                require(index + 1 < args.size) { "Missing URL after $option" }
                mcpUrl = normalizeMcpUrl(args[index + 1])
                usedLegacySseArgument = option == "--sse-url"
                index += 2
            }

            "-h", "--help" -> error("Help is handled before argument parsing")
            else -> throw IllegalArgumentException("Unknown argument: $option")
        }
    }

    return ProxyConfig(mcpUrl = mcpUrl, usedLegacySseArgument = usedLegacySseArgument)
}

/**
 * Validates an HTTP(S) endpoint and appends the standard `/mcp` path to a root URL.
 * Existing non-root paths are preserved for custom deployments.
 */
fun normalizeMcpUrl(value: String): String {
    val uri = runCatching { URI(value.trim()) }
        .getOrElse { throw IllegalArgumentException("Invalid MCP URL: $value", it) }

    require(uri.scheme == "http" || uri.scheme == "https") {
        "MCP URL must use http or https"
    }
    require(!uri.host.isNullOrBlank()) { "MCP URL must include a host" }
    require(uri.userInfo == null) { "Credentials must not be embedded in the MCP URL" }
    require(uri.query == null) { "MCP URL must not include query parameters" }
    require(uri.fragment == null) { "MCP URL must not include a fragment" }

    val path = when (uri.path) {
        null, "", "/" -> "/mcp"
        else -> uri.path
    }

    return URI(
        uri.scheme,
        null,
        uri.host,
        uri.port,
        path,
        uri.query,
        null,
    ).toASCIIString()
}

fun main(args: Array<String>) {
    if (args.any { it == "-h" || it == "--help" }) {
        println(usage)
        return
    }

    try {
        val config = parseCommandLineArgs(args)
        if (config.usedLegacySseArgument) {
            logger.warn("--sse-url is deprecated; using Streamable HTTP endpoint {}", config.mcpUrl)
        }

        logger.info("Starting Burp MCP stdio proxy with Streamable HTTP endpoint: {}", config.mcpUrl)
        runBlocking {
            StreamableHttpProxy(mcpUrl = config.mcpUrl).run()
        }
    } catch (error: Exception) {
        logger.error("Failed to start proxy: {}", error.message, error)
        exitProcess(1)
    }
}
