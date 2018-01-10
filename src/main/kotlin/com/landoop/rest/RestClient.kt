package com.landoop.rest

import com.landoop.jdbc.domain.LsqlData
import com.landoop.jdbc4.Constants
import com.landoop.jdbc4.JacksonSupport
import com.landoop.rest.domain.Credentials
import com.landoop.rest.domain.LoginResponse
import com.landoop.rest.domain.Topic
import org.apache.http.HttpEntity
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException


class RestClient(private val urls: List<String>,
                 private val credentials: Credentials) : AutoCloseable {

  private val logger = LoggerFactory.getLogger(RestClient::class.java)
  private val timeout = 60_000

  private val defaultRequestConfig = RequestConfig.custom()
      .setConnectTimeout(timeout)
      .setConnectionRequestTimeout(timeout)
      .setSocketTimeout(timeout)
      .build()

  private val httpClient = HttpClientBuilder.create().let {
    it.setKeepAliveStrategy(LsqlKeepAlive)
    it.setDefaultRequestConfig(defaultRequestConfig)
    it.build()
  }

  private var token: String? = null

  var isClosed: Boolean = true
    private set

  override fun close() {
    httpClient.close()
    isClosed = true
  }

  fun connectTimeout(): Int = defaultRequestConfig.connectTimeout
  fun connectionRequestTimeout(): Int = defaultRequestConfig.connectionRequestTimeout
  fun socketTimeout(): Int = defaultRequestConfig.socketTimeout

  // attempt a given connection for each url until one is successful
  fun <T> attempt(req: (String) -> AttemptResponse): T {
    var lastException: Throwable = RuntimeException("Invalid request")
    for (url in urls) {
      try {
        return req(url)
      } catch (t: Throwable) {
        lastException = t
      }
    }
    throw lastException
  }

  // attempts the given request at each url until one is successful
  // if the token is not set, or a 401 or 403 is received, then it will attempt
  // to authenticate before retrying
  fun <T> attemptOrAuthenticate(req: (String) -> T): T {
    for (url in urls) {
      try {
        return req(url)
      } catch (t: Throwable) {

      }
    }
  }

  private fun authenticate() {

    val entity = RestClient.jsonEntity(credentials)

    token = attempt {

      val endpoint = "$it/api/login"
      logger.debug("Attempting to authenticate at $endpoint")
      val request = RestClient.post(endpoint, entity)
      val response = httpClient.execute(request)

      when (response.statusLine.statusCode) {
        401 -> throw SQLException("Invalid credentials for user '${credentials.user}'")
        200 -> JacksonSupport.fromJson<LoginResponse>(response.entity.content).token
        else -> throw SQLException("Invalid status code ${response.statusLine.reasonPhrase}")
      }
    }
  }

  fun topics(): List<Topic> {
    return attempt {
      val endpoint = "$it/api/topics"
      logger.debug("Fetching topics from $endpoint")

      val request = RestClient.signedGet(endpoint, token!!)
      val response = httpClient.execute(request)
    }
  }

  fun messages(sql: String) {
    return attempt {
      val endpoint = "$it/api/sql/data?sql=$sql"
      logger.debug("Fetching messages from $endpoint")
    }
  }

  /**
   * loginRequest in case of an expired token
   */
  fun executeQuery(lsql: String, loginRequest: Credentials): LsqlData {
    fun handleResponse(stream: InputStream): LsqlData {
      try {
        return JacksonSupport.fromJson(stream)
      } catch (ex: Throwable) {
        logger.error("An error occurred while reading the response.$ex")
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
      method.params.setParameter("sql", lsql)

      var response = httpClient.execute(method)
      val statusCode = response.statusLine.statusCode
      if (statusCode == 401) {
        val newToken = authenticate(loginRequest)
        if (newToken.isPresent) {
          method.setHeader(Constants.HttpHeaderKey, newToken.get())
        } else {
          throw SQLException("Invalid credentials for user '${loginRequest.user}'")
        }

        //try again
        response = httpClient.execute(method)
      }

      when (statusCode) {
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
        logger.warn("There was an error retrieving data from '$url'", ex)
        lastException = ex
      } catch (t: Throwable) {
        logger.warn("There was an error retrieving data from '$url'", t)
        lastException = SQLException("An error occurred retrieving data from '$url'.${t.message}.")
      }
    }
    throw lastException ?: SQLException("An error occurred retrieving data from '${urls.joinToString { "," }}'.")
  }

  companion object RestClient {

    private val HttpHeaderKey = "x-kafka-lenses-token"

    fun <T> jsonEntity(t: T): HttpEntity {
      val entity = StringEntity(JacksonSupport.toJson(t))
      entity.setContentType("application/json")
      return entity
    }

    fun signedGet(endpoint: String, token: String): HttpGet {
      return HttpGet(endpoint).apply {
        this.setHeader("Accept", "application/json")
        this.setHeader(HttpHeaderKey, token)
      }
    }

    fun post(endpoint: String, entity: HttpEntity): HttpPost {
      return HttpPost(endpoint).apply {
        this.entity = entity
        this.setHeader("Accept", "application/json")
        this.setHeader("Content-type", "application/json")
      }
    }
  }

  /**
   * Returns true if the current token is still valid
   */
  fun isValid(): Boolean {

  }
}