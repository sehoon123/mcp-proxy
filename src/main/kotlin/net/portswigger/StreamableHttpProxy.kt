package net.portswigger

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.HttpTimeoutConfig
import io.ktor.client.plugins.sse.SSE
import io.modelcontextprotocol.kotlin.sdk.client.ReconnectionOptions
import io.modelcontextprotocol.kotlin.sdk.client.StreamableHttpClientTransport
import io.modelcontextprotocol.kotlin.sdk.client.StreamableHttpError
import io.modelcontextprotocol.kotlin.sdk.server.StdioServerTransport
import io.modelcontextprotocol.kotlin.sdk.types.InitializeResult
import io.modelcontextprotocol.kotlin.sdk.types.JSONRPCError
import io.modelcontextprotocol.kotlin.sdk.types.JSONRPCMessage
import io.modelcontextprotocol.kotlin.sdk.types.JSONRPCNotification
import io.modelcontextprotocol.kotlin.sdk.types.JSONRPCRequest
import io.modelcontextprotocol.kotlin.sdk.types.JSONRPCResponse
import io.modelcontextprotocol.kotlin.sdk.types.RPCError
import io.modelcontextprotocol.kotlin.sdk.types.RequestId
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.io.asSink
import kotlinx.io.asSource
import kotlinx.io.buffered
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID
import kotlin.math.min
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val INITIALIZE_METHOD = "initialize"
private const val INITIALIZED_METHOD = "notifications/initialized"
private const val TOOLS_CALL_METHOD = "tools/call"
private val EVENT_STREAM_WARMUP_DELAY = 250.milliseconds

internal data class RetryPolicy(
    val maxAttempts: Int = 20,
    val initialDelay: Duration = 250.milliseconds,
    val maxDelay: Duration = 3.seconds,
) {
    init {
        require(maxAttempts > 0) { "maxAttempts must be positive" }
        require(initialDelay.isPositive()) { "initialDelay must be positive" }
        require(maxDelay >= initialDelay) { "maxDelay must not be shorter than initialDelay" }
    }

    fun delayBeforeAttempt(attempt: Int): Duration {
        val multiplier = 1 shl min(attempt - 1, 20)
        return minOf(initialDelay * multiplier, maxDelay)
    }
}

/**
 * A transparent stdio-to-Streamable-HTTP transport bridge.
 *
 * JSON-RPC messages are relayed without decoding them into a fixed list of MCP methods. This preserves
 * negotiated capabilities, custom methods, cancellation IDs, progress tokens, and future protocol additions.
 * If Burp restarts, the cached initialization handshake is replayed on a new HTTP session before safe requests
 * are retried.
 */
internal class StreamableHttpProxy(
    private val mcpUrl: String,
    input: InputStream = System.`in`,
    output: OutputStream = System.out,
    private val retryPolicy: RetryPolicy = RetryPolicy(),
    private val httpClient: HttpClient = defaultHttpClient(),
) {
    private val logger = LoggerFactory.getLogger(StreamableHttpProxy::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val finished = CompletableDeferred<Unit>()
    private val closed = AtomicBoolean(false)
    private val connectionMutex = Mutex()
    private val handshakeMutex = Mutex()
    private val initialized = CompletableDeferred<Unit>()

    private val stdioTransport = StdioServerTransport(
        input = input.asSource().buffered(),
        output = output.asSink().buffered(),
    )

    @Volatile
    private var connection: UpstreamConnection? = null

    @Volatile
    private var initializeRequest: JSONRPCRequest? = null

    @Volatile
    private var initializedNotification: JSONRPCNotification? = null

    suspend fun run() {
        stdioTransport.onMessage { message ->
            scope.launch(CoroutineName("StreamableHttpProxy.forwardToHttp")) {
                handleDownstreamMessage(message)
            }
        }
        stdioTransport.onError { error ->
            logger.error("stdio transport error: {}", error.message, error)
        }
        stdioTransport.onClose {
            finished.complete(Unit)
        }

        try {
            stdioTransport.start()
            finished.await()
        } finally {
            shutdown(closeStdio = false)
        }
    }

    suspend fun close() {
        shutdown(closeStdio = true)
    }

    private suspend fun handleDownstreamMessage(message: JSONRPCMessage) {
        if (closed.get()) return

        try {
            when {
                message is JSONRPCRequest && message.method == INITIALIZE_METHOD -> {
                    handshakeMutex.withLock {
                        initializeRequest = message
                        sendWithRecovery(message, replaySession = false)
                    }
                }

                message is JSONRPCNotification && message.method == INITIALIZED_METHOD -> {
                    handshakeMutex.withLock {
                        initializedNotification = message
                        sendWithRecovery(message, replaySession = true)
                        // The SDK starts the optional server-to-client event stream asynchronously.
                        // Gate normal requests briefly so an immediate sampling/elicitation request
                        // cannot race the stream setup and become stranded.
                        delay(EVENT_STREAM_WARMUP_DELAY)
                        initialized.complete(Unit)
                    }
                }

                else -> {
                    initialized.await()
                    sendWithRecovery(message, replaySession = true)
                }
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            logger.error("Failed to forward {} to Burp: {}", message.description(), error.message)
            sendFailureToStdio(message, error)
        }
    }

    private suspend fun sendWithRecovery(message: JSONRPCMessage, replaySession: Boolean) {
        var lastError: Throwable? = null

        for (attempt in 1..retryPolicy.maxAttempts) {
            val current = try {
                getOrCreateConnection(replaySession)
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                lastError = error
                if (attempt == retryPolicy.maxAttempts || !isSafeToRetry(message, error)) break
                logRetry(message, attempt, error)
                delay(retryPolicy.delayBeforeAttempt(attempt))
                continue
            }

            // Replaying a session already sends the cached initialized notification.
            if (message is JSONRPCNotification &&
                message.method == INITIALIZED_METHOD &&
                current.replayedSession
            ) {
                return
            }

            try {
                current.transport.send(message)
                return
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                lastError = error
                invalidate(current)
                if (attempt == retryPolicy.maxAttempts || !isSafeToRetry(message, error)) break
                logRetry(message, attempt, error)
                delay(retryPolicy.delayBeforeAttempt(attempt))
            }
        }

        throw UpstreamUnavailableException(
            "Burp MCP endpoint was unavailable after ${retryPolicy.maxAttempts} attempts",
            lastError,
        )
    }

    private suspend fun getOrCreateConnection(replaySession: Boolean): UpstreamConnection =
        connectionMutex.withLock {
            connection?.let { return@withLock it }

            val created = createConnection()
            try {
                if (replaySession && initializedNotification != null) {
                    replayInitialization(created)
                    created.replayedSession = true
                }
                connection = created
                created
            } catch (error: Throwable) {
                created.retired.set(true)
                runCatching { created.transport.close() }
                throw error
            }
        }

    private suspend fun createConnection(): UpstreamConnection {
        val transport = StreamableHttpClientTransport(
            client = httpClient,
            url = mcpUrl,
            reconnectionOptions = ReconnectionOptions(maxRetries = 5),
        )
        val created = UpstreamConnection(transport)

        transport.onMessage { message -> handleUpstreamMessage(created, message) }
        transport.onError { error ->
            if (!created.retired.get() && !closed.get()) {
                logger.debug("Streamable HTTP transport reported: {}", error.message)
            }
        }
        transport.onClose {
            if (!created.retired.get() && !closed.get()) {
                logger.debug("Streamable HTTP transport closed")
            }
        }
        transport.start()
        return created
    }

    private suspend fun replayInitialization(current: UpstreamConnection) {
        val cachedInitialize = checkNotNull(initializeRequest) {
            "Cannot restore an MCP session before initialize has been received"
        }
        val cachedInitialized = checkNotNull(initializedNotification) {
            "Cannot restore an MCP session before notifications/initialized has been received"
        }

        val replayRequest = cachedInitialize.copy(
            id = RequestId("mcp-proxy-reconnect-${UUID.randomUUID()}"),
        )
        val response = sendInternalRequest(current, replayRequest)
        when (response) {
            is JSONRPCResponse -> {
                val result = response.result as? InitializeResult
                    ?: error("Burp returned an invalid initialize response while restoring the session")
                current.transport.protocolVersion = result.protocolVersion
            }

            is JSONRPCError -> error("Burp rejected session restoration: ${response.error.message}")
            else -> error("Burp returned an unexpected initialize response while restoring the session")
        }

        current.transport.send(cachedInitialized)
        delay(EVENT_STREAM_WARMUP_DELAY)
        logger.info("Restored Streamable HTTP session after Burp became available")
    }

    private suspend fun sendInternalRequest(
        current: UpstreamConnection,
        request: JSONRPCRequest,
    ): JSONRPCMessage {
        val response = CompletableDeferred<JSONRPCMessage>()
        current.pendingResponses[request.id] = response
        return try {
            withTimeout(30.seconds) {
                current.transport.send(request)
                response.await()
            }
        } finally {
            current.pendingResponses.remove(request.id)
        }
    }

    private suspend fun handleUpstreamMessage(current: UpstreamConnection, message: JSONRPCMessage) {
        val responseId = when (message) {
            is JSONRPCResponse -> message.id
            is JSONRPCError -> message.id
            else -> null
        }
        val pending = responseId?.let { current.pendingResponses.remove(it) }
        if (pending != null) {
            pending.complete(message)
            return
        }

        if (current.retired.get() || closed.get()) return

        val cachedInitializeId = initializeRequest?.id
        if (message is JSONRPCResponse && message.id == cachedInitializeId) {
            (message.result as? InitializeResult)?.let {
                current.transport.protocolVersion = it.protocolVersion
            }
        }

        stdioTransport.send(message)
    }

    private suspend fun invalidate(expected: UpstreamConnection) {
        val shouldClose = connectionMutex.withLock {
            if (connection !== expected) {
                false
            } else {
                connection = null
                expected.retired.set(true)
                true
            }
        }

        if (shouldClose) {
            runCatching { expected.transport.close() }
        }
    }

    private fun isSafeToRetry(message: JSONRPCMessage, error: Throwable): Boolean {
        if (message !is JSONRPCRequest || message.method != TOOLS_CALL_METHOD) return true

        // A missing session means the server explicitly did not process the call. A refused TCP
        // connection also occurs before an HTTP request can be delivered. Other failures are
        // ambiguous, so never risk executing a security tool twice.
        return error.findCause<StreamableHttpError>()?.code == 404 ||
            error.findCause<ConnectException>() != null
    }

    private fun logRetry(message: JSONRPCMessage, attempt: Int, error: Throwable) {
        logger.warn(
            "Unable to forward {} (attempt {}/{}): {}",
            message.description(),
            attempt,
            retryPolicy.maxAttempts,
            error.message,
        )
    }

    private suspend fun sendFailureToStdio(message: JSONRPCMessage, error: Throwable) {
        if (message !is JSONRPCRequest || closed.get()) return

        val detail = error.message.orEmpty().take(300)
        runCatching {
            stdioTransport.send(
                JSONRPCError(
                    id = message.id,
                    error = RPCError(
                        code = RPCError.ErrorCode.CONNECTION_CLOSED,
                        message = "Burp MCP endpoint unavailable: $detail",
                    ),
                ),
            )
        }.onFailure {
            logger.error("Failed to report upstream error to stdio client: {}", it.message)
        }
    }

    private suspend fun shutdown(closeStdio: Boolean) {
        if (!closed.compareAndSet(false, true)) return

        if (closeStdio) {
            runCatching { stdioTransport.close() }
        }

        val current = connectionMutex.withLock {
            connection.also { connection = null }
        }
        current?.retired?.set(true)
        current?.let { runCatching { it.transport.close() } }
        httpClient.close()
        finished.complete(Unit)
        scope.cancel()
    }

    private class UpstreamConnection(
        val transport: StreamableHttpClientTransport,
        val pendingResponses: ConcurrentHashMap<RequestId, CompletableDeferred<JSONRPCMessage>> = ConcurrentHashMap(),
        val retired: AtomicBoolean = AtomicBoolean(false),
        var replayedSession: Boolean = false,
    )

    private class UpstreamUnavailableException(message: String, cause: Throwable?) : Exception(message, cause)

    companion object {
        private fun defaultHttpClient(): HttpClient = HttpClient(CIO) {
            install(SSE)
            install(HttpTimeout) {
                connectTimeoutMillis = 5_000
                requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                socketTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
            }
        }
    }
}

private fun JSONRPCMessage.description(): String = when (this) {
    is JSONRPCRequest -> "request '$method'"
    is JSONRPCNotification -> "notification '$method'"
    is JSONRPCResponse -> "response '$id'"
    is JSONRPCError -> "error response '$id'"
    else -> "JSON-RPC message"
}

private inline fun <reified T : Throwable> Throwable.findCause(): T? {
    var current: Throwable? = this
    while (current != null) {
        if (current is T) return current
        current = current.cause
    }
    return null
}
