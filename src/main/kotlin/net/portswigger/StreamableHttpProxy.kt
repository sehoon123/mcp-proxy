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
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
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
private const val MAX_CONCURRENT_UPSTREAM_REQUESTS = 16
private const val REQUEST_QUEUE_CAPACITY = 64
private const val CONTROL_QUEUE_CAPACITY = 64
private const val HANDSHAKE_QUEUE_CAPACITY = 2
private const val PROXY_OVERLOADED_ERROR_CODE = -32000
private val EVENT_STREAM_WARMUP_DELAY = 250.milliseconds
private val RETRYABLE_AVAILABILITY_STATUS_CODES = setOf(404, 408, 425, 429, 500, 502, 503, 504)

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
 * JSON-RPC methods and parameters are relayed without a proxy-side method allowlist. This preserves negotiated
 * capabilities, request IDs, cancellation IDs, and progress tokens for protocol shapes supported by the pinned
 * MCP SDK. If Burp restarts, the cached initialization handshake is replayed on a new HTTP session before only
 * definitively safe requests are retried.
 */
internal class StreamableHttpProxy(
    private val mcpUrl: String,
    input: InputStream = System.`in`,
    output: OutputStream = System.out,
    private val retryPolicy: RetryPolicy = RetryPolicy(),
    private val httpClient: HttpClient = defaultHttpClient(),
    private val maxConcurrentRequests: Int = MAX_CONCURRENT_UPSTREAM_REQUESTS,
    private val requestQueueCapacity: Int = REQUEST_QUEUE_CAPACITY,
) {
    private val logger = LoggerFactory.getLogger(StreamableHttpProxy::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val finished = CompletableDeferred<Unit>()
    private val closed = AtomicBoolean(false)
    private val connectionMutex = Mutex()
    private val handshakeMutex = Mutex()
    private val initialized = CompletableDeferred<Unit>()
    private val handshakeMessages = Channel<JSONRPCMessage>(HANDSHAKE_QUEUE_CAPACITY)
    private val requestMessages = Channel<JSONRPCMessage>(requestQueueCapacity)
    private val controlMessages = Channel<JSONRPCMessage>(CONTROL_QUEUE_CAPACITY)

    init {
        require(maxConcurrentRequests > 0) { "maxConcurrentRequests must be positive" }
        require(requestQueueCapacity > 0) { "requestQueueCapacity must be positive" }
    }

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
        startRelayWorkers()
        stdioTransport.onMessage(::enqueueDownstreamMessage)
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

    private fun startRelayWorkers() {
        scope.launch(CoroutineName("StreamableHttpProxy.handshakeRelay")) {
            for (message in handshakeMessages) handleDownstreamMessage(message)
        }
        repeat(maxConcurrentRequests) { worker ->
            scope.launch(CoroutineName("StreamableHttpProxy.requestRelay-$worker")) {
                for (message in requestMessages) handleDownstreamMessage(message)
            }
        }
        scope.launch(CoroutineName("StreamableHttpProxy.controlRelay")) {
            for (message in controlMessages) handleDownstreamMessage(message)
        }
    }

    private suspend fun enqueueDownstreamMessage(message: JSONRPCMessage) {
        when {
            message is JSONRPCRequest && message.method == INITIALIZE_METHOD -> handshakeMessages.send(message)
            message is JSONRPCNotification && message.method == INITIALIZED_METHOD -> handshakeMessages.send(message)
            message is JSONRPCRequest -> {
                if (requestMessages.trySend(message).isFailure && !closed.get()) {
                    stdioTransport.send(
                        JSONRPCError(
                            id = message.id,
                            error = RPCError(
                                code = PROXY_OVERLOADED_ERROR_CODE,
                                message = "Burp MCP proxy request queue is full; request was not forwarded",
                            ),
                        ),
                    )
                }
            }
            else -> controlMessages.send(message)
        }
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
                if (attempt == retryPolicy.maxAttempts || !isRetryableConnectionFailure(error)) break
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
                if (attempt == retryPolicy.maxAttempts || !isSafeToRetryAfterSend(message, error)) break
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

    private fun isRetryableConnectionFailure(error: Throwable): Boolean {
        if (error.findCause<ConnectException>() != null) return true
        return error.findCause<StreamableHttpError>()?.code in RETRYABLE_AVAILABILITY_STATUS_CODES
    }

    private fun isSafeToRetryAfterSend(message: JSONRPCMessage, error: Throwable): Boolean {
        // HTTP 404 means the old session was not found, and ConnectException occurs before a
        // connection can deliver the message. Any other post-send failure is ambiguous and must
        // not be retried for an arbitrary request: custom and future methods may have side effects.
        if (error.findCause<StreamableHttpError>()?.code == 404 ||
            error.findCause<ConnectException>() != null
        ) {
            return true
        }

        // Initialization has no tool side effects and is safe to replay while Burp is starting.
        val isHandshake = (message is JSONRPCRequest && message.method == INITIALIZE_METHOD) ||
            (message is JSONRPCNotification && message.method == INITIALIZED_METHOD)
        return isHandshake && isRetryableConnectionFailure(error)
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

        handshakeMessages.close()
        requestMessages.close()
        controlMessages.close()
        scope.cancel()

        val current = connectionMutex.withLock {
            connection.also { connection = null }
        }
        current?.retired?.set(true)
        current?.let {
            if (it.transport.sessionId != null) {
                val terminated = withTimeoutOrNull(2.seconds) {
                    try {
                        it.transport.terminateSession()
                        true
                    } catch (error: CancellationException) {
                        throw error
                    } catch (error: Exception) {
                        logger.debug("Unable to terminate MCP session: {}", error.message)
                        false
                    }
                }
                if (terminated != true) {
                    logger.debug("MCP session termination did not complete")
                }
            }
            runCatching { it.transport.close() }
        }

        if (closeStdio) {
            runCatching { stdioTransport.close() }
        }

        httpClient.close()
        finished.complete(Unit)
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
