package com.landoop.jdbc

import com.fasterxml.jackson.databind.node.ObjectNode
import com.landoop.jdbc.domain.LoginRequest
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.protocol.HttpContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.sql.SQLClientInfoException
import java.sql.SQLException
import java.sql.SQLInvalidAuthorizationSpecException
import java.sql.SQLPermission
import java.util.*


class LsqlRestClient(private val urls: List<String>) : AutoCloseable {

  companion object {
    val Logger = LoggerFactory.getLogger(LsqlRestClient::class.java)
  }

  private val httpClient: CloseableHttpClient
  var isClosed: Boolean = true
    private set

  init {
    httpClient = HttpClientBuilder.create()
        .setKeepAliveStrategy(object : DefaultConnectionKeepAliveStrategy() {
          override fun getKeepAliveDuration(response: HttpResponse?, context: HttpContext?): Long {
            var keepAlive = super.getKeepAliveDuration(response, context)
            if (keepAlive == -1L) {
              // Keep connections alive 5 seconds if a keep-alive value
              // has not be explicitly set by the server
              keepAlive = 5000
            }
            return keepAlive
          }
        })
        .build()
  }

  override fun close() {
    httpClient.close()
    isClosed = true
  }

  fun login(loginRequest: LoginRequest): Optional<String> {
    val requestEntity = StringEntity(JacksonJson.toJson(loginRequest))
    requestEntity.setContentType("application/json")

    fun loginInternal(url: String): Optional<String> {
      val apiUrl = "$url/api/login"
      val method = HttpPost(apiUrl)
      method.entity = requestEntity
      method.setHeader("Accept", "application/json");
      method.setHeader("Content-type", "application/json");

      val response = httpClient.execute(method)
      val statusCode = response.statusLine.statusCode
      if (statusCode == 401) throw SQLInvalidAuthorizationSpecException()

      if (statusCode != 200 && statusCode != 201) {
        throw SQLException("Received status code of $statusCode.${response.statusLine.reasonPhrase}")
      }

      val responseEntity = response.entity

      if (responseEntity != null) {
        val instream = responseEntity.content
        try {
          val json = JacksonJson.asJson(instream)
          when (json) {
            is ObjectNode ->
              return Optional.of(json.get("token")!!.textValue())
            else -> throw SQLException("Unexpected return content. (Response: ${json.toString()}")
          }
        } catch (ex: IOException) {
          throw ex
        } finally {
          instream.close()
        }
      }
      return Optional.empty()
    }

    for (url in urls) {
      try {
        return loginInternal(url)
      } catch (t: Throwable) {
        Logger.warn("Failed to connect to '$url'", t)
      }
    }
    throw SQLException("Could not connect to ${urls.joinToString { "," }}")
  }
}