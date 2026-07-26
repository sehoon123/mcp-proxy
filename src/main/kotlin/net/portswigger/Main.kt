package net.portswigger

import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.net.URI
import kotlin.system.exitProcess

private val logger = LoggerFactory.getLogger("net.portswigger.Main")

const val DEFAULT_MCP_URL = "http://127.0.0.1:9876/mcp"

private val usage = """
    Independent MCP Bridge stdio proxy

    Usage:
      java -jar mcp-proxy-all.jar [--mcp-url <url>] [--bearer-token-env <name>]

    Options:
      --mcp-url <url>          Streamable HTTP MCP endpoint (default: $DEFAULT_MCP_URL)
      --bearer-token <token>   Bearer token (prefer --bearer-token-env to avoid process listings)
      --bearer-token-env <n>   Read the bearer token from environment variable <n>
      --sse-url <url>          Deprecated alias. A root URL is automatically migrated to /mcp.
      -h, --help               Show this help.
""".trimIndent()

data class ProxyConfig(
    val mcpUrl: String = DEFAULT_MCP_URL,
    val usedLegacySseArgument: Boolean = false,
    val bearerToken: String? = null,
) {
    override fun toString(): String =
        "ProxyConfig(mcpUrl=$mcpUrl, usedLegacySseArgument=$usedLegacySseArgument, bearerToken=${if (bearerToken == null) "<none>" else "<redacted>"})"
}

/** Parse and validate command-line arguments without writing to stdout. */
fun parseCommandLineArgs(
    args: Array<String>,
    environment: Map<String, String> = System.getenv(),
): ProxyConfig {
    var mcpUrl = DEFAULT_MCP_URL
    var usedLegacySseArgument = false
    var bearerToken: String? = null
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

            "--bearer-token" -> {
                require(index + 1 < args.size) { "Missing token after $option" }
                require(bearerToken == null) { "Specify only one bearer-token option" }
                bearerToken = validateBearerToken(args[index + 1])
                index += 2
            }

            "--bearer-token-env" -> {
                require(index + 1 < args.size) { "Missing environment variable name after $option" }
                require(bearerToken == null) { "Specify only one bearer-token option" }
                val variable = args[index + 1]
                require(variable.matches(Regex("[A-Za-z_][A-Za-z0-9_]{0,127}"))) {
                    "Invalid bearer-token environment variable name"
                }
                bearerToken = validateBearerToken(
                    environment[variable] ?: throw IllegalArgumentException(
                        "Bearer-token environment variable $variable is not set"
                    )
                )
                index += 2
            }

            "-h", "--help" -> error("Help is handled before argument parsing")
            else -> throw IllegalArgumentException("Unknown command-line option")
        }
    }

    return ProxyConfig(
        mcpUrl = mcpUrl,
        usedLegacySseArgument = usedLegacySseArgument,
        bearerToken = bearerToken,
    )
}

private fun validateBearerToken(value: String): String {
    require(value.length in 32..128 && value.none { it.isWhitespace() || it.isISOControl() }) {
        "Bearer token must contain 32 to 128 non-whitespace characters"
    }
    return value
}

/**
 * Validates a numeric-loopback HTTP(S) endpoint and appends the standard `/mcp` path to a root URL.
 */
fun normalizeMcpUrl(value: String): String {
    require(value == value.trim() && value.length in 1..2_048) { "Invalid MCP URL" }
    val uri = runCatching { URI(value) }
        .getOrElse { throw IllegalArgumentException("Invalid MCP URL", it) }

    require(uri.scheme == "http" || uri.scheme == "https") {
        "MCP URL must use http or https"
    }
    val host = uri.host?.removePrefix("[")?.removeSuffix("]")?.lowercase()
    require(host == "127.0.0.1" || host == "::1") {
        "MCP URL host must be the numeric loopback 127.0.0.1 or ::1"
    }
    require(uri.port == -1 || uri.port in 1..65_535) { "MCP URL port is invalid" }
    val authorityHost = if (host == "::1") "[::1]" else host
    val expectedAuthority = if (uri.port == -1) authorityHost else "$authorityHost:${uri.port}"
    require(uri.rawAuthority?.lowercase() == expectedAuthority) { "MCP URL authority is not canonical" }
    require(uri.userInfo == null) { "Credentials must not be embedded in the MCP URL" }
    require(uri.query == null) { "MCP URL must not include query parameters" }
    require(uri.fragment == null) { "MCP URL must not include a fragment" }

    val path = when (uri.rawPath) {
        null, "", "/" -> "/mcp"
        "/mcp" -> "/mcp"
        else -> throw IllegalArgumentException("MCP URL path must be /mcp")
    }

    return URI(
        uri.scheme,
        null,
        host,
        uri.port,
        path,
        uri.query,
        null,
    ).toASCIIString()
}

internal fun safeProxyError(error: Throwable): String {
    val message = error.message.orEmpty()
        .replace(Regex("(?i)Bearer\\s+[^\\s,;]+"), "Bearer <redacted>")
        .replace(Regex("(?i)(token|password|secret)\\s*[:=]\\s*[^\\s,;]+")) {
            "${it.groupValues[1]}=<redacted>"
        }
        .replace(Regex("(?i)\\b[A-Z]:[\\\\/](?:[^\\s:;]+[\\\\/])+[^\\s:;]*"), "<path>")
        .replace(Regex("(?<![A-Za-z0-9])/(?:[^/\\s:;]+/)+[^\\s:;]*"), "<path>")
        .replace(Regex("[\\r\\n\\t\\u0000-\\u001f\\u007f]+"), " ")
        .trim()
        .take(384)
    return if (message.isEmpty()) error::class.simpleName ?: "Exception" else message
}

fun main(args: Array<String>) {
    // kotlin-logging defaults to printing an initialization banner to stdout. Any non-JSON
    // output corrupts the MCP stdio stream, so force the library's startup message off.
    System.setProperty("kotlin-logging.logStartupMessage", "false")

    if (args.any { it == "-h" || it == "--help" }) {
        println(usage)
        return
    }

    try {
        val config = parseCommandLineArgs(args)
        if (config.usedLegacySseArgument) {
            logger.warn("--sse-url is deprecated; using Streamable HTTP endpoint {}", config.mcpUrl)
        }

        logger.info("Starting Independent MCP Bridge stdio proxy with Streamable HTTP endpoint: {}", config.mcpUrl)
        val proxy = StreamableHttpProxy(
            mcpUrl = config.mcpUrl,
            bearerToken = config.bearerToken,
        )
        val shutdownHook = Thread(
            { runBlocking { proxy.close() } },
            "mcp-proxy-shutdown",
        )
        Runtime.getRuntime().addShutdownHook(shutdownHook)
        try {
            runBlocking { proxy.run() }
        } finally {
            runCatching { Runtime.getRuntime().removeShutdownHook(shutdownHook) }
        }
    } catch (error: Exception) {
        logger.error("Failed to start proxy: {}", safeProxyError(error))
        exitProcess(1)
    }
}
