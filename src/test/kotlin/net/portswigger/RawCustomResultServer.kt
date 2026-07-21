package net.portswigger

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.cio.CIO
import io.ktor.server.cio.CIOApplicationEngine
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.util.concurrent.atomic.AtomicInteger

/** Raw endpoint that returns a valid but SDK-unknown custom result shape. */
internal class RawCustomResultServer {
    private var engine: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null
    private val customCallCounter = AtomicInteger()

    val customCallCount: Int
        get() = customCallCounter.get()

    fun start(): Int {
        val port = TestMcpServer.findAvailablePort()
        engine = embeddedServer(CIO, host = "127.0.0.1", port = port) {
            routing {
                get("/mcp") { call.respond(HttpStatusCode.MethodNotAllowed) }
                delete("/mcp") { call.respond(HttpStatusCode.OK) }
                post("/mcp") {
                    val request = Json.parseToJsonElement(call.receiveText()).jsonObject
                    val method = request["method"]?.jsonPrimitive?.content
                    when (method) {
                        "notifications/initialized" -> call.respond(HttpStatusCode.Accepted)
                        "initialize" -> call.respondText(
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
