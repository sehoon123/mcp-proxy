package net.portswigger

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.cio.CIO
import io.ktor.server.cio.CIOApplicationEngine
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.receiveText
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

/** Raw endpoint that returns a valid but SDK-unknown custom result shape. */
internal class RawCustomResultServer(
    private val transientDeleteFailures: Int = 0,
) {
    private var engine: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null
    private val customCallCounter = AtomicInteger()
    private val deleteCallCounter = AtomicInteger()
    val slowCallStarted = CompletableDeferred<Unit>()
    val cancellationRequestId = CompletableDeferred<String>()

    val customCallCount: Int
        get() = customCallCounter.get()

    val deleteCallCount: Int
        get() = deleteCallCounter.get()

    fun start(): Int {
        val port = TestMcpServer.findAvailablePort()
        engine = embeddedServer(CIO, host = "127.0.0.1", port = port) {
            routing {
                get("/mcp") { call.respond(HttpStatusCode.MethodNotAllowed) }
                delete("/mcp") {
                    val attempt = deleteCallCounter.incrementAndGet()
                    call.respond(
                        if (attempt <= transientDeleteFailures) {
                            HttpStatusCode.ServiceUnavailable
                        } else {
                            HttpStatusCode.OK
                        }
                    )
                }
                post("/mcp") {
                    val request = Json.parseToJsonElement(call.receiveText()).jsonObject
                    val method = request["method"]?.jsonPrimitive?.content
                    when (method) {
                        "notifications/initialized" -> call.respond(HttpStatusCode.Accepted)
                        "notifications/cancelled" -> {
                            val requestId = request.getValue("params").jsonObject
                                .getValue("requestId").jsonPrimitive.content
                            cancellationRequestId.complete(requestId)
                            call.respond(HttpStatusCode.Accepted)
                        }
                        "initialize" -> {
                            call.response.header("Mcp-Session-Id", "raw-custom-result-session")
                            call.respondText(
                                buildJsonObject {
                                    put("jsonrpc", "2.0")
                                    put("id", request.getValue("id"))
                                    put("result", buildJsonObject {
                                        put("protocolVersion", "2025-11-25")
                                        put("capabilities", buildJsonObject {})
                                        put("serverInfo", buildJsonObject {
                                            put("name", "raw-custom-result-server")
                                            put("version", "1.0.0")
                                        })
                                    })
                                }.toString(),
                                ContentType.Application.Json,
                            )
                        }
                        "custom/echo" -> {
                            customCallCounter.incrementAndGet()
                            call.respondText(
                                buildJsonObject {
                                    put("jsonrpc", "2.0")
                                    put("id", request.getValue("id"))
                                    put("result", buildJsonObject {
                                        put("foo", "bar")
                                        put("futureField", true)
                                    })
                                }.toString(),
                                ContentType.Application.Json,
                            )
                        }
                        "custom/slow" -> {
                            slowCallStarted.complete(Unit)
                            delay(10.seconds)
                            call.respondText(
                                buildJsonObject {
                                    put("jsonrpc", "2.0")
                                    put("id", request.getValue("id"))
                                    put("result", buildJsonObject { put("completed", true) })
                                }.toString(),
                                ContentType.Application.Json,
                            )
                        }
                        "ping" -> call.respondText(
                            buildJsonObject {
                                put("jsonrpc", "2.0")
                                put("id", request.getValue("id"))
                                put("result", buildJsonObject {})
                            }.toString(),
                            ContentType.Application.Json,
                        )
                        else -> call.respond(HttpStatusCode.BadRequest)
                    }
                }
            }
        }.start(wait = false)
        Thread.sleep(200)
        return port
    }

    fun stop() {
        engine?.stop(500, 1_000)
        engine = null
    }
}
