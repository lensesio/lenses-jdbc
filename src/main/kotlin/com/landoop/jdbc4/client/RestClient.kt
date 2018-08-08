package com.landoop.jdbc4.client

import com.landoop.jdbc4.Constants
import com.landoop.jdbc4.JacksonSupport
import com.landoop.jdbc4.client.domain.Credentials
import com.landoop.jdbc4.client.domain.InsertRecord
import com.landoop.jdbc4.client.domain.InsertResponse
import com.landoop.jdbc4.client.domain.Message
import com.landoop.jdbc4.client.domain.PreparedInsertResponse
import com.landoop.jdbc4.client.domain.StreamingSelectResult
import com.landoop.jdbc4.client.domain.Table
import com.landoop.jdbc4.client.domain.Topic
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
import org.apache.http.util.EntityUtils
import org.glassfish.tyrus.client.ClientManager
import org.glassfish.tyrus.client.ClientProperties
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.net.URL
import java.net.URLEncoder
import java.sql.SQLException
import java.util.concurrent.Executors
import javax.websocket.ClientEndpointConfig
import javax.websocket.Endpoint
import javax.websocket.EndpointConfig
import javax.websocket.MessageHandler
import javax.websocket.Session

class RestClient(private val urls: List<String>,
                 private val credentials: Credentials,
                 private val weakSSL: Boolean // if set to true then will allow self signed certificates
) : AutoCloseable {

  private val client = ClientManager.createClient().apply {
    this.properties[ClientProperties.REDIRECT_ENABLED] = true
  }
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

  // some connection pools, eg org.apache.commons.dbcp, will check that the connection is open
  // before they hand it over to be used. Since a rest client is always stateless, we can just
  // return true here as there's nothing to close.
  var isClosed: Boolean = false
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
  private fun <T> attempt(reqFn: (String) -> HttpUriRequest, respFn: (HttpResponse) -> T): T {
    var lastException: Throwable? = null
    for (url in urls) {
      lastException = try {
        val req = reqFn(url)
        val resp = httpClient.execute(req)
        logger.debug("Response $resp")
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
  private fun <T> attemptAuthenticated(reqFn: (String) -> HttpUriRequest, respFn: (HttpResponse) -> T): T {
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
  private fun <T> attemptAuthenticatedWithRetry(reqFn: (String) -> HttpUriRequest, respFn: (HttpResponse) -> T): T {
    return try {
      attemptAuthenticated(reqFn, respFn)
    } catch (e: AuthenticationException) {
      token = authenticate()
      attemptAuthenticated(reqFn, respFn)
    }
  }

  private fun attemptAuthenticatedWithRetry(endpoint: Endpoint, uri: URI) {
    return try {
      attemptAuthenticated(endpoint, uri)
    } catch (e: Exception) {
      token = authenticate()
      attemptAuthenticated(endpoint, uri)
    }
  }

  private fun attemptAuthenticated(endpoint: Endpoint, uri: URI) {

    val configurator = object : ClientEndpointConfig.Configurator() {
      override fun beforeRequest(headers: MutableMap<String, MutableList<String>>) {
        headers[Constants.LensesTokenHeader] = mutableListOf(token)
      }
    }

    val config: ClientEndpointConfig = ClientEndpointConfig.Builder.create().configurator(configurator).build()
    client.connectToServer(endpoint, config, uri)
  }

  // attempts to authenticate, and returns the token if successful
  private fun authenticate(): String {

    val requestFn: (String) -> HttpUriRequest = {
      val entity = jsonEntity(credentials)
      val endpoint = "$it/api/login"
      logger.debug("Authenticating at $endpoint")
      jsonPost(endpoint, entity)
    }

    val responseFn: (HttpResponse) -> String = {
      val token = EntityUtils.toString(it.entity)
      logger.debug("Authentication token: $token")
      token
    }

    return attempt(requestFn, responseFn)
  }

  fun topic(topicName: String): Topic {

    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/topics/$topicName"
      logger.debug("Fetching topic @ $endpoint")
      jsonGet(endpoint)
    }

    // once we get 200
    val responseFn: (HttpResponse) -> Topic = {
      logger.debug("Topic json")
      val str = it.entity.content.bufferedReader().use { it.readText() }
      logger.debug(str)
      JacksonSupport.fromJson(str)
    }

    return attemptAuthenticated(requestFn, responseFn)
  }

  fun tables(): Array<Table> {

    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/jdbc/metadata/table"
      logger.debug("Fetching topics @ $endpoint")
      jsonGet(endpoint)
    }

    val responseFn: (HttpResponse) -> Array<Table> = {
      val str = it.entity.content.bufferedReader().use { it.readText() }
      JacksonSupport.fromJson(str)
    }

    return attemptAuthenticated(requestFn, responseFn)
  }

  private fun escape(url: String): String {
    //replace \n with ' '
    //captures SELECT/INSERT statements
    return URLEncoder.encode(url.trim().replace(System.lineSeparator(), " "), "UTF-8").replace("%20", "+")
  }

  fun insert(sql: String): InsertResponse {
    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/jdbc/insert"
      val entity = stringEntity(sql)
      logger.debug("Executing query $endpoint")
      logger.debug(sql)
      plainTextPost(endpoint, entity)
    }

    val responseFn: (HttpResponse) -> InsertResponse = {
      InsertResponse(it.statusLine.statusCode.toString())
      /*val json = it.entity.content.bufferedReader().use { it.readText() }
      try{
        JacksonSupport.fromJson(json)
      }catch (t:Throwable){
        InsertResponse(json)
      }*/
    }

    return attemptAuthenticatedWithRetry(requestFn, responseFn)
  }

  /**
   * Executes a prepared insert statement.
   *
   * @param topic the topic to run the insert again
   * @param records the insert variables for each row
   */
  fun executePreparedInsert(topic: String, keyType: String, valueType: String, records: List<InsertRecord>): Any {

    val requestFn: (String) -> HttpUriRequest = {
      val endpoint = "$it/api/jdbc/insert/prepared/$topic?kt=$keyType&vt=$valueType"
      val entity = jsonEntity(records)
      jsonPost(endpoint, entity)
    }

    // at the moment the response just returns ok or an error status
    // in the case of receiving an ok (201) there's not much to do but return true
    val responseFn: (HttpResponse) -> Boolean = {
      val entity = it.entity.content.bufferedReader().use { it.readText() }
      logger.debug("Prepared insert response $entity")
      true
    }

    return attemptAuthenticatedWithRetry(requestFn, responseFn)
  }

  fun select(sql: String): StreamingSelectResult {
    logger.debug("Executing query $sql")

    // hacky fix for spark
    val r = "SELECT.*?FROM\\s+SELECT".toRegex()
    val normalizedSql = sql.replaceFirst(r, "SELECT")

    logger.debug("Normalized query $normalizedSql")

    val escapedSql = escape(normalizedSql)
    val url = "${urls[0]}/api/ws/jdbc/data?sql=$escapedSql"
    val uri = URI.create(url.replace("https://", "ws://").replace("http://", "ws://"))

    val executor = Executors.newSingleThreadExecutor()
    val result = StreamingSelectResult()
    val endpoint = object : Endpoint() {

      override fun onOpen(session: Session, config: EndpointConfig) {
        session.addMessageHandler(MessageHandler.Whole<String> {
          executor.submit(messageHandler(it))
        })
      }

      fun messageHandler(message: String): Runnable = Runnable {
        try {
          when (message.take(1)) {
          // records
            "0" -> result.addRecord(message.drop(1))
          // error case
            "1" -> {
              val e = SQLException(message.drop(1))
              logger.error("Error from select protocol: $message")
              logger.debug("Original query: $uri")
              throw e
            }
          // schema
            "2" -> result.setSchema(message.drop(1))
          // all done
            "3" -> {
              executor.submit { result.endStream() }
              executor.shutdown()
            }
          }
        } catch (t: Throwable) {
          t.printStackTrace()
          result.setError(t)
          executor.submit { result.endStream() }
          executor.shutdown()
        }
      }
    }

    attemptAuthenticatedWithRetry(endpoint, uri)
    return result
  }

  fun prepareStatement(sql: String): PreparedInsertResponse {
    val requestFn: (String) -> HttpUriRequest = {
      val escapedSql = escape(sql)
      val endpoint = "$it/api/jdbc/insert/prepared?sql=$escapedSql"
      logger.debug("Executing query $endpoint")
      jsonGet(endpoint)
    }

    val responseFn: (HttpResponse) -> PreparedInsertResponse = {
      val entity = it.entity.content.bufferedReader().use { it.readText() }
      logger.debug("Prepare response $entity")
      JacksonSupport.fromJson(entity)
    }

    return attemptAuthenticated(requestFn, responseFn)
  }

  fun messages(sql: String): List<Message> {

    val requestFn: (String) -> HttpUriRequest = {
      val escapedSql = escape(sql)
      val endpoint = "$it/api/sql/data?sql=$escapedSql"
      logger.debug("Executing query $endpoint")
      jsonGet(endpoint)
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

    fun <T> jsonEntity(t: T): HttpEntity {
      val entity = StringEntity(JacksonSupport.toJson(t))
      entity.setContentType("application/json")
      return entity
    }

    fun stringEntity(string: String): HttpEntity {
      return StringEntity(string)
    }

    fun jsonGet(endpoint: String): HttpGet {
      return HttpGet(endpoint).apply {
        this.setHeader("Accept", "application/json")
      }
    }

    fun plainTextPost(endpoint: String, entity: HttpEntity): HttpPost {
      return HttpPost(endpoint).apply {
        this.entity = entity
        this.setHeader("Content-type", "text/plain")
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