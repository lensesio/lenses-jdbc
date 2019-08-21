package com.landoop.jdbc4.client

import arrow.core.Either
import arrow.core.Right
import arrow.core.Try
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import arrow.core.rightIfNotNull
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import com.landoop.jdbc4.JacksonSupport
import com.landoop.jdbc4.client.domain.Credentials
import com.landoop.jdbc4.resultset.RowResultSet
import com.landoop.jdbc4.resultset.WebSocketResultSet
import com.landoop.jdbc4.row.ListRow
import com.landoop.jdbc4.row.Row
import com.landoop.jdbc4.util.Logging
import io.ktor.client.HttpClient
import io.ktor.client.features.compression.ContentEncoding
import io.ktor.client.features.json.JacksonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.features.websocket.WebSockets
import io.ktor.client.features.websocket.webSocketSession
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.accept
import io.ktor.client.request.header
import io.ktor.client.request.parameter
import io.ktor.client.request.request
import io.ktor.client.response.HttpResponse
import io.ktor.client.response.readText
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.Url
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.WebSocketSession
import io.ktor.http.contentType
import io.ktor.util.KtorExperimentalAPI
import io.ktor.util.flattenForEach
import kotlinx.coroutines.ObsoleteCoroutinesApi
import org.apache.avro.Schema
import java.math.BigDecimal
import java.net.URLEncoder
import java.sql.SQLException

data class Token(val value: String)

sealed class JdbcError {
  data class AuthenticationFailure(val message: String) : JdbcError()
  object ExecutionError : JdbcError()
  object NoData : JdbcError()
  data class ParseError(val t: Throwable) : JdbcError()
  data class UnsupportedRowType(val type: String) : JdbcError()
}

class LensesClient(private val url: String,
                   private val credentials: Credentials,
                   private val weakSSL: Boolean) : AutoCloseable, Logging {

  companion object {
    const val LensesTokenHeader = "X-Kafka-Lenses-Token"
  }

  private val frameToSchema: (Frame) -> Either<JdbcError, Schema> = { frame ->
    val node = JacksonSupport.mapper.readTree(frame.data)
    val json = when (val valueSchema = node["data"]["valueSchema"]) {
      is TextNode -> valueSchema.asText()
      else -> JacksonSupport.mapper.writeValueAsString(valueSchema)
    }
    Schema.Parser().parse(json).right()
  }

  private val frameToRecord: (Frame) -> Either<JdbcError.ParseError, Row> = { frame ->
    Try { JacksonSupport.mapper.readTree(frame.data) }
        .toEither { JdbcError.ParseError(it) }
        .map { node ->
          val data = node["data"]
          val key = data["value"]
          val value = data["value"]
          val keys = if (key == null) emptyList() else flattenJson(key)
          val values = if (value == null) emptyList() else flattenJson(value)
          ListRow(keys + values)
        }
  }

  private val frameToRow: (Frame) -> Either<JdbcError.ParseError, Row?> = { frame ->
    Try { JacksonSupport.mapper.readTree(frame.data) }
        .toEither { JdbcError.ParseError(it) }
        .flatMap { node ->
          when (val type = node["type"].textValue()) {
            "RECORD" -> frameToRecord(frame)
            "END" -> Right(null)
            else -> throw UnsupportedOperationException("Unsupported row type $type")
          }
        }
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

  private fun escape(url: String): String {
    return URLEncoder.encode(url.trim().replace(System.lineSeparator(), " "), "UTF-8").replace("%20", "+")
  }

  @ObsoleteCoroutinesApi
  suspend fun execute(sql: String, f: (Row) -> Row): Either<JdbcError, RowResultSet> {
    val escapedSql = escape(sql)
    val endpoint = "$url/api/ws/v3/jdbc/execute?sql=$escapedSql"
    return withAuthenticatedWebsocket(endpoint).flatMap {
      // we always need the first row to generate the schema
      it.incoming.receiveOrNull().rightIfNotNull { JdbcError.NoData }
          .flatMap(frameToSchema)
          .map { schema ->
            WebSocketResultSet(null, schema, it, frameToRow, f)
          }
    }
  }

  private suspend fun withAuthenticatedWebsocket(endpoint: String): Either<JdbcError, WebSocketSession> {
    return authenticate().flatMap { token ->
      val url = Url(endpoint)
      return client.webSocketSession(HttpMethod.Get, url.host, url.port, url.encodedPath) {
        header(LensesTokenHeader, token.value)
        url.parameters.flattenForEach { key, value -> parameter(key, value) }
      }.right()
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

fun flattenJson(n: JsonNode): List<Any> = when {
  n.isBigDecimal -> listOf(BigDecimal(n.bigIntegerValue()))
  n.isBigInteger -> listOf(n.bigIntegerValue())
  n.isBinary -> listOf(n.binaryValue())
  n.isBoolean -> listOf(n.asBoolean())
  n.isDouble -> listOf(n.doubleValue())
  n.isFloat -> listOf(n.floatValue())
  n.isInt -> listOf(n.intValue())
  n.isLong -> listOf(n.longValue())
  n.isShort -> listOf(n.shortValue())
  n.isTextual -> listOf(n.asText())
  n.isObject -> n.fields().asSequence().toList().flatMap { flattenJson(it.value) }
  n.isArray -> n.elements().asSequence().toList().flatMap { flattenJson(it) }
  else -> throw SQLException("Unsupported node type ${n.javaClass}:$n")
}

