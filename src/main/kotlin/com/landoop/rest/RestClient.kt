package com.landoop.rest

import com.landoop.jdbc4.Constants
import com.landoop.jdbc4.JacksonSupport
import com.landoop.rest.domain.Credentials
import com.landoop.rest.domain.JdbcData
import com.landoop.rest.domain.LoginResponse
import com.landoop.rest.domain.Message
import com.landoop.rest.domain.Topic
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.conn.ssl.NoopHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.TrustSelfSignedStrategy
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.ssl.SSLContextBuilder
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.net.URL
import java.sql.SQLException

class RestClient(private val urls: List<String>,
                 private val credentials: Credentials,
                 private val weakSSL: Boolean // if set to true then will allow self signed certificates
) : AutoCloseable {

  private val logger = LoggerFactory.getLogger(RestClient::class.java)
  private val timeout = 60_000

  private val defaultRequestConfig = RequestConfig.custom()
      .setConnectTimeout(timeout)
      .setConnectionRequestTimeout(timeout)
      .setSocketTimeout(timeout)
      .build()

  private val sslContext = SSLContextBuilder.create()
      .loadTrustMaterial(TrustSelfSignedStrategy())
      .build()

  private val allowAllHosts = NoopHostnameVerifier()

  private val connectionFactory = SSLConnectionSocketFactory(sslContext, allowAllHosts)

  private val httpClient = HttpClientBuilder.create().let {
    it.setKeepAliveStrategy(DefaultKeepAlive)
    it.setDefaultRequestConfig(defaultRequestConfig)
    if (weakSSL)
      it.setSSLSocketFactory(connectionFactory)
    it.build()
  }

  // the token received the last time we attempted to authenticate
  internal var token: String = authenticate()

  var isClosed: Boolean = true
    private set

  override fun close() {
    httpClient.close()
    isClosed = true
  }

  fun connectTimeout(): Int = defaultRequestConfig.connectTimeout
  fun connectionRequestTimeout(): Int = defaultRequestConfig.connectionRequestTimeout
  fun socketTimeout(): Int = defaultRequestConfig.socketTimeout

  // attempt a given request for each url until one is successful, or all have been exhausted
  // a 401 or 403 will result in a short circuit exit
  // an IOException, or an unsupported http status code will result in trying the next url
  // once all urls are exhausted, the last exception will be thrown
  fun <T> attempt(reqFn: (String) -> HttpUriRequest, respFn: (HttpResponse) -> T): T {
    var lastException: Throwable? = null
    for (url in urls) {
      lastException = try {
        val req = reqFn(url)
        val resp = httpClient.execute(req)
        when (resp.statusLine.statusCode) {
          200, 201, 202 -> return respFn(resp)
          401, 403 -> throw AuthenticationException("Invalid credentials for user '${credentials.user}'")
          else -> {
            val body = resp.entity.content.bufferedReader().use { it.readText() }
            throw SQLException("url=$url, req=$req, ${resp.statusLine.statusCode} ${resp.statusLine.reasonPhrase}: $body")
          }
        }
      } catch (e: SQLException) {
        e
      } catch (e: IOException) {
        e
      }
    }
    throw lastException!!
  }

  // attempts the given request with authentication by adding the current token as a header
  fun <T> attemptAuthenticated(reqFn: (String) -> HttpUriRequest, respFn: (HttpResponse) -> T): T {
    val reqWithTokenHeaderFn: (String) -> HttpUriRequest = {
      reqFn(it).apply {
        addHeader(Constants.LensesTokenHeader, token)
      }
    }
    return attempt(reqWithTokenHeaderFn, respFn)
  }

  // attempts the given request with authentication
  // if an authentication error is received, then it will attempt to
  // re-authenticate before retrying again
  // if auth is still invalid then it will give up
  fun <T> attemptAuthenticatedWithRetry(reqFn: (String) -> HttpUriRequest, respFn: (HttpResponse) -> T): T {
    return try {
      attemptAuthenticated(reqFn, respFn)
    } catch (e: AuthenticationException) {
      token = authenticate()
      attemptAuthenticated(reqFn, respFn)
    }
  }

  // attempts to authenticate, and returns the token if successful
  private fun authenticate(): String {

    val requestFn: (String) -> HttpUriRequest = {
      val entity = RestClient.jsonEntity(credentials)
      val endpoint = "$it/api/login"
      logger.debug("Authenticating at $endpoint")
      RestClient.jsonPost(endpoint, entity)
    }

    val responseFn: (HttpResponse) -> String = {
      val token = JacksonSupport.fromJson<LoginResponse>(it.entity.content).token
      logger.debug("Authentication token: $token")
      token
    }

    return attempt(requestFn, responseFn)
  }

  fun topics(): Array<Topic> {

    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/topics"
      logger.debug("Fetching topics @ $endpoint")
      RestClient.jsonGet(endpoint)
    }

    val responseFn: (HttpResponse) -> Array<Topic> = {
      JacksonSupport.fromJson(it.entity.content)
    }

    return attemptAuthenticated(requestFn, responseFn)
  }

  internal fun escape(url: String): String {
    val u = URL(url)
    val uri = URI(
        u.protocol,
        u.authority,
        u.path,
        u.query,
        u.ref
    )
    return uri.toURL().toString().replace("%20", "+")
  }

  fun query(sql: String): JdbcData {

    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/jdbc/data?sql=$sql"
      val escaped = escape(endpoint)
      logger.debug("Executing query $escaped")
      RestClient.jsonGet(escaped)
    }

    val responseFn: (HttpResponse) -> JdbcData = {
      JacksonSupport.fromJson(it.entity.content)
    }

    return attemptAuthenticatedWithRetry(requestFn, responseFn)
  }

  fun messages(sql: String): List<Message> {

    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/sql/data?sql=$sql"
      val escaped = escape(endpoint)
      logger.debug("Executing query $escaped")
      RestClient.jsonGet(escaped)
    }

    val responseFn: (HttpResponse) -> List<Message> = {
      JacksonSupport.fromJson(it.entity.content)
    }

    return attemptAuthenticated(requestFn, responseFn)
  }

  // returns true if the connection is still valid, it can do this by attempting to reauth
  fun isValid(): Boolean {
    token = authenticate()
    return true
  }

  companion object RestClient {

    private val HttpHeaderKey = Constants.LensesTokenHeader

    fun <T> jsonEntity(t: T): HttpEntity {
      val entity = StringEntity(JacksonSupport.toJson(t))
      entity.setContentType("application/json")
      return entity
    }

    fun jsonGet(endpoint: String): HttpGet {
      return HttpGet(endpoint).apply {
        this.setHeader("Accept", "application/json")
      }
    }

    fun jsonPost(endpoint: String, entity: HttpEntity): HttpPost {
      return HttpPost(endpoint).apply {
        this.entity = entity
        this.setHeader("Accept", "application/json")
        this.setHeader("Content-type", "application/json")
      }
    }
  }
}