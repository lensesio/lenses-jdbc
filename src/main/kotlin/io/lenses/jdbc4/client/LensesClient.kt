package io.lenses.jdbc4.client

import arrow.core.*
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.TextNode
import io.ktor.client.HttpClient
import io.ktor.client.features.compression.ContentEncoding
import io.ktor.client.features.json.JacksonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.features.websocket.WebSockets
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.accept
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.response.HttpResponse
import io.ktor.client.response.readText
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.util.KtorExperimentalAPI
import io.lenses.jdbc4.JacksonSupport
import io.lenses.jdbc4.normalizeRecord
import io.lenses.jdbc4.resultset.RowResultSet
import io.lenses.jdbc4.resultset.WebSocketResultSet
import io.lenses.jdbc4.resultset.WebsocketConnection
import io.lenses.jdbc4.resultset.emptyResultSet
import io.lenses.jdbc4.row.PairRow
import io.lenses.jdbc4.row.Row
import io.lenses.jdbc4.util.Logging
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.apache.avro.Schema
import org.glassfish.tyrus.client.ClientManager
import org.glassfish.tyrus.client.ClientProperties
import org.springframework.web.socket.*
import org.springframework.web.socket.client.WebSocketClient
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import java.net.URI
import java.util.concurrent.LinkedBlockingQueue

data class Token(val value: String)

sealed class JdbcError {
  open val cause: Throwable? = null

  data class AuthenticationFailure(val message: String) : JdbcError()
  data class InitialError(val message: String) : JdbcError()
  object ExecutionError : JdbcError()
  object NoData : JdbcError()
  data class ParseError(override val cause: Throwable) : JdbcError()
  data class UnsupportedRowType(val type: String) : JdbcError()
}

val frameToRecord: (String, Schema) -> Either<JdbcError.ParseError, Row> = { msg, schema ->
  Try { JacksonSupport.mapper.readTree(msg) }
      .toEither { JdbcError.ParseError(it) }
      .map { node ->
        val data = node["data"]
        val value = data["value"]
        val values = when (schema.type) {
          Schema.Type.RECORD -> when (value) {
            null -> normalizeRecord(schema, NullNode.instance)
            else -> normalizeRecord(schema, value)
          }
          else -> when (value) {
            null -> emptyList()
            else -> listOf("value" to value.toString())
          }
        }
        PairRow(values)
      }
}

class LensesClient(private val url: String,
                   private val credentials: Credentials,
                   private val weakSSL: Boolean) : AutoCloseable, Logging {

  companion object {
    const val LensesTokenHeader = "X-Kafka-Lenses-Token"
  }

  private fun parseSchema(node: JsonNode): Either<JdbcError, Schema> = Try {
    val json = when (val valueSchema = node["data"]["valueSchema"]) {
      is TextNode -> valueSchema.asText()
      is NullNode -> Schema.create(Schema.Type.NULL).toString(true)
      null -> Schema.create(Schema.Type.NULL).toString(true)
      else -> JacksonSupport.mapper.writeValueAsString(valueSchema)
    }
    Schema.Parser().parse(json)
  }.toEither { JdbcError.ParseError(it) }

  private val frameToRow: (String, Schema) -> Either<JdbcError.ParseError, Row?> = { msg, schema ->
    Try { JacksonSupport.mapper.readTree(msg) }
        .toEither { JdbcError.ParseError(it) }
        .flatMap { node ->
          when (val type = node["type"].textValue()) {
            "RECORD" -> frameToRecord(msg, schema)
            "END" -> Right(null)
            else -> throw UnsupportedOperationException("Unsupported row type $type")
          }
        }
  }

  private val wssclient = ClientManager.createClient().apply {
    this.properties[ClientProperties.REDIRECT_ENABLED] = true
  }

  @UseExperimental(KtorExperimentalAPI::class)
  private val client = HttpClient {
    install(WebSockets)
    install(ContentEncoding) {
      gzip()
      identity()
    }
    install(JsonFeature) {
      serializer = JacksonSerializer()
    }
  }

  private var isClosed: Boolean = false

  override fun close() {
    client.close()
    isClosed = true
  }

  fun isClosed(): Boolean = isClosed

  // attempts to authenticate, and returns the auth token if successful
  private suspend fun authenticate(): Either<JdbcError, Token> {

    val endpoint = "$url/api/login"
    logger.debug("Authenticating at $endpoint")

    val resp = client.request<HttpResponse>(endpoint) {
      method = HttpMethod.Post
      contentType(ContentType.Application.Json)
      accept(ContentType.Text.Plain)
      body = credentials
    }

    return if (resp.status == HttpStatusCode.OK)
      Token(resp.readText()).right()
    else
      JdbcError.AuthenticationFailure(resp.readText()).left()
  }

  @ObsoleteCoroutinesApi
  suspend fun execute(sql: String): Either<JdbcError, RowResultSet> {
    val endpoint = "$url/api/ws/v3/jdbc/execute"
    return withAuthenticatedWebsocket(endpoint, sql).flatMap {
      // we always need the first row to generate the schema unless it is an end then we have an empty resultset

      val msg = it.queue.take()
      val node = JacksonSupport.mapper.readTree(msg)

      when (node["type"].textValue()) {
        "END" -> emptyResultSet.right()
        "SCHEMA" -> parseSchema(node).map { schema ->
          WebSocketResultSet(null, schema, it, frameToRow)
        }
        else -> JdbcError.InitialError(node.toString()).left()
      }
    }
  }

  private suspend fun withAuthenticatedWebsocket(url: String, sql: String): Either<JdbcError, WebsocketConnection> {
    return authenticate().flatMap { token ->
      val uri = URI.create(url.replace("https://", "ws://").replace("http://", "ws://"))
      val headers = WebSocketHttpHeaders()
      headers.add(LensesTokenHeader, token.value)
      //Expected to read from env variables
      //val  sslContextConfigurator =  SslContextConfigurator()
      /*      sslContextConfigurator.setTrustStoreFile("...");
      *      sslContextConfigurator.setTrustStorePassword("...");
      *      sslContextConfigurator.setTrustStoreType("...");
      *      sslContextConfigurator.setKeyStoreFile("...");
      *      sslContextConfigurator.setKeyStorePassword("...");
      *      sslContextConfigurator.setKeyStoreType("...");
      */
      //val sslEngineConfigurator = SslEngineConfigurator(sslContextConfigurator, true,false, false)

      val clientManager:ClientManager = ClientManager.createClient()
      clientManager.properties[ClientProperties.REDIRECT_ENABLED] = true
      //clientManager.properties[ClientProperties.SSL_ENGINE_CONFIGURATOR] = sslEngineConfigurator
      val wsclient: WebSocketClient = StandardWebSocketClient(clientManager)

      val queue = LinkedBlockingQueue<String>(200)
      val jdbcRequest = JdbcRequestMessage(sql, token.value)
      val handler = object : WebSocketHandler {
        override fun handleTransportError(session: WebSocketSession,
                                          exception: Throwable) {
          logger.error("Websocket error", exception)
        }

        override fun afterConnectionClosed(session: WebSocketSession,
                                           closeStatus: CloseStatus) {
          logger.debug("Connection closed $closeStatus")
        }

        override fun handleMessage(session: WebSocketSession,
                                   message: WebSocketMessage<*>) {
          logger.debug("Handling message in thread ${Thread.currentThread().id}")
          when (message) {
            is TextMessage -> queue.put(message.payload)
            else -> {
              logger.error("Unsupported message type $message")
              throw java.lang.UnsupportedOperationException("Unsupported message type $message")
            }
          }
        }

        override fun afterConnectionEstablished(session: WebSocketSession) {
          logger.debug("Connection established. Sending the SQL to the server...")
          //send the SQL and the token
          val json = JacksonSupport.toJson(jdbcRequest)
          val message = TextMessage(json.toByteArray())
          session.sendMessage(message)

        }

        override fun supportsPartialMessages(): Boolean = false
      }

      logger.debug("Connecting to websocket at $uri")
      val sess = wsclient.doHandshake(handler, headers, uri).get()

      val conn = object : WebsocketConnection {
        override val queue = queue
        override fun close() {
          if(sess.isOpen) {
            try {
              sess.close()
            } catch (t: Throwable) {

            }
          }
        }
        override fun isClosed(): Boolean = !sess.isOpen
      }
      conn.right()
    }
  }

  private suspend fun <T> withAuthenticated(req: HttpRequestBuilder, f: (HttpResponse) -> T): Either<JdbcError, T> {
    return authenticate().flatMap { token ->
      req.header(LensesTokenHeader, token.value)
      val resp = client.request<HttpResponse>(req)
      if (resp.status == HttpStatusCode.OK) f(resp).right() else JdbcError.ExecutionError.left()
    }
  }
}