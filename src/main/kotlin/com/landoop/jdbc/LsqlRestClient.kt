package com.landoop.jdbc

import com.fasterxml.jackson.databind.node.ObjectNode
import com.landoop.jdbc.domain.LsqlData
import com.landoop.jdbc.domain.LoginRequest
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.protocol.HttpContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException
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
      if (statusCode == 401) throw SQLException("Invalid credentials for user '${loginRequest.user}'")

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
    throw SQLException("Could not connect to ${urls.joinToString { "," }}.")
  }

  /**
   * loginRequest in case of an expired token
   */
  fun executeQuery(lsql: String, token: String, loginRequest: LoginRequest): LsqlData {
    fun handleResponse(stream: InputStream): LsqlData {
      try {
        return JacksonJson.fromJson(stream)
      } catch (ex: Throwable) {
        Logger.error("An error occurred while reading the response.$ex")
        throw SQLException("An error occurred while reading the response.$ex")
      } finally {
        stream.close()
      }
    }

    fun queryInternal(url: String): LsqlData {
      val apiUrl = "$url/api/jdbc/data"
      val method = HttpGet(apiUrl)
      method.setHeader("Accept", "application/json");
      method.setHeader(Constants.HttpHeaderKey, token)

      var response = httpClient.execute(method)
      val statusCode = response.statusLine.statusCode
      if (statusCode == 401) {
        val newToken = login(loginRequest)
        if (newToken.isPresent) {
          method.setHeader(Constants.HttpHeaderKey, newToken.get())
        } else {
          throw SQLException("Invalid credentials for user '${loginRequest.user}'")
        }

        //try again
        response = httpClient.execute(method)
      }

      return when (statusCode) {
        401 -> throw SQLException("Invalid credentials for user '${loginRequest.user}'")
        400 -> throw SQLSyntaxErrorException(if (response.statusLine.reasonPhrase != null) response.statusLine.reasonPhrase else "Invalid syntax")
        500 -> throw SQLException("An error occurred running the query.${response.statusLine.reasonPhrase}")
        200 -> {
          val responseEntity = response.entity
          if (responseEntity == null) {
            throw SQLException("Invalid response received. Expecting non-null content.")
          }
          return handleResponse(responseEntity.content)
        }
        else -> throw SQLException("An error occurred running the query. (Response code:${statusCode}; reason:${response.statusLine.reasonPhrase})")
      }
    }

    var lastException: SQLException? = null
    for (url in urls) {
      try {
        return queryInternal(url)
      } catch (ex: SQLException) {
        Logger.warn("There was an error retrieving data from '$url'", ex)
        lastException = ex
      } catch (t: Throwable) {
        Logger.warn("There was an error retrieving data from '$url'", t)
        lastException = SQLException("An error occurred retrieving data from '$url'.${t.message}.")
      }
    }
    throw lastException ?: SQLException("An error occurred retrieving data from '${urls.joinToString { "," }}'.")
  }
}