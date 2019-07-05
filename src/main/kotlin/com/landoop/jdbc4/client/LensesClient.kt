package com.landoop.jdbc4.client

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.landoop.jdbc4.client.domain.Credentials
import com.landoop.jdbc4.util.Logging
import io.ktor.client.HttpClient
import io.ktor.client.features.json.JacksonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.features.websocket.DefaultClientWebSocketSession
import io.ktor.client.features.websocket.WebSockets
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.url
import io.ktor.client.response.HttpResponse
import io.ktor.client.response.readText
import io.ktor.http.ContentType
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.contentType
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.channels.ReceiveChannel
import java.net.URLEncoder

data class Token(val value: String)

class LensesClient(private val url: String,
                   private val credentials: Credentials) : AutoCloseable, Logging {

  companion object {
    const val LensesTokenHeader = "X-Kafka-Lenses-Token"
  }

  @UseExperimental(KtorExperimentalAPI::class)
  private val client = HttpClient {
    install(WebSockets)
    install(JsonFeature) {
      serializer = JacksonSerializer()
    }
  }

  private var isClosed: Boolean = false

  override fun close() {
    client.close()
    isClosed = true
  }

  sealed class Error {
    object AuthenticationFailure : Error()
    object ExecutionError : Error()
  }

  // attempts to authenticate, and returns the auth token if successful
  private suspend fun authenticate(): Either<Error, Token> {

    val endpoint = "$url/api/login"
    logger.debug("Authenticating at $endpoint")

    val resp = client.request<HttpResponse>(endpoint) {
      contentType(ContentType.Application.Json)
      body = credentials
    }

    return if (resp.status == HttpStatusCode.OK) Token(resp.readText()).right() else Error.AuthenticationFailure.left()
  }

  private fun escape(url: String): String {
    return URLEncoder.encode(url.trim().replace(System.lineSeparator(), " "), "UTF-8").replace("%20", "+")
  }

  suspend fun execute(sql: String): Either<Error, ReceiveChannel<Frame>> {
    val escapedSql = escape(sql)
    val endpoint = "$url/api/ws/v3/jdbc/execute?sql=$escapedSql"
    val req = request {
      method = HttpMethod.Post
      this.url(endpoint)
    }
    return withAuthenticatedWebsocket(req)
  }

  private suspend fun withAuthenticatedWebsocket(req: HttpRequestBuilder): Either<Error, ReceiveChannel<Frame>> {
    return authenticate().flatMap { token ->
      req.header(LensesTokenHeader, token.value)
      val resp = client.request<DefaultClientWebSocketSession>(req)
      if (resp.call.response.status == HttpStatusCode.OK) resp.incoming.right() else Error.ExecutionError.left()
    }
  }

  private suspend fun <T> withAuthenticated(req: HttpRequestBuilder, f: (HttpResponse) -> T): Either<Error, T> {
    return authenticate().flatMap { token ->
      req.header(LensesTokenHeader, token.value)
      val resp = client.request<HttpResponse>(req)
      if (resp.status == HttpStatusCode.OK) f(resp).right() else Error.ExecutionError.left()
    }
  }
}