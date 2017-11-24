package com.landoop.jdbc

import com.landoop.jdbc.domain.LoginRequest
import java.net.MalformedURLException
import java.net.URL
import java.sql.*
import java.util.*
import java.util.concurrent.Executor

class LsqlJdbcConnection : Connection {

  val urls: List<String>

  var info: Properties? = null
    private set

  var userName: String
    private set

  internal var password: String
    private set

  internal var token: String
    private set

  private var readOnly: Boolean = false
  private var autoCommit: Boolean = false

  constructor(jdbcUrl: String, info: Properties?) {

    val user = info?.getProperty("user", null)
    if (user == null) {
      throw SQLException("Missing the user parameter")
    }
    userName = user

    val pwd = info.getProperty("password", null)
    if (pwd == null) {
      throw SQLException("Missing the password parameter")
    }
    password = pwd


    val urls = jdbcUrl.replace("jdbc:lsql:kafka:", "").split(',')
    val restClient = LsqlRestClient(urls)
    val token: String = restClient.login(LoginRequest(userName, password)).orElseThrow {
      throw SQLException("Connection ")
    }

    this.urls = urls
    this.urls.forEach { url ->
      try {
        URL(url)
      } catch (e: MalformedURLException) {
        throw SQLException("${url} is not a valid URL.")
      }
    }

    this.info = info
    readOnly = false


    //status = ODatabase.STATUS.OPEN
  }


  @Throws(SQLException::class)
  override fun createStatement(): Statement {
    return LsqlJdbcStatement(this)
  }

  @Throws(SQLException::class)
  override fun prepareStatement(sql: String): PreparedStatement {
    return LsqlJdbcStatement(this, sql)
  }

  @Throws(SQLException::class)
  override fun prepareCall(sql: String): CallableStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun nativeSQL(sql: String): String {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun clearWarnings() {
  }

  @Throws(SQLException::class)
  override fun <T> unwrap(iface: Class<T>): T {

    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun isWrapperFor(iface: Class<*>): Boolean {

    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun close() {
  }

  @Throws(SQLException::class)
  override fun commit() {
  }

  @Throws(SQLException::class)
  override fun rollback() {
  }

  @Throws(SQLException::class)
  override fun isClosed(): Boolean {
    //TODO:check the rest client
    return false
  }

  @Throws(SQLException::class)
  override fun isReadOnly(): Boolean {
    return readOnly
  }

  @Throws(SQLException::class)
  override fun setReadOnly(iReadOnly: Boolean) {
    readOnly = iReadOnly
  }

  @Throws(SQLException::class)
  override fun isValid(timeout: Int): Boolean {
    return true
  }

  @Throws(SQLException::class)
  override fun createArrayOf(typeName: String, elements: Array<Any>): java.sql.Array? {
    return null
  }

  @Throws(SQLException::class)
  override fun createBlob(): Blob? {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun createClob(): Clob? {
    return null
  }

  @Throws(SQLException::class)
  override fun createNClob(): NClob? {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun createSQLXML(): SQLXML? {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement {
    return LsqlJdbcStatement(this)
  }

  @Throws(SQLException::class)
  override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
    return LsqlJdbcStatement(this)
  }

  @Throws(SQLException::class)
  override fun createStruct(typeName: String, attributes: Array<Any>): Struct? {
    return null
  }

  @Throws(SQLException::class)
  override fun getAutoCommit(): Boolean {
    return autoCommit
  }

  @Throws(SQLException::class)
  override fun setAutoCommit(autoCommit: Boolean) {
    this.autoCommit = autoCommit
  }

  @Throws(SQLException::class)
  override fun getCatalog(): String {
    return "default"
  }

  @Throws(SQLException::class)
  override fun setCatalog(catalog: String) {
  }

  @Throws(SQLException::class)
  override fun getClientInfo(): Properties? {
    return null
  }

  @Throws(SQLClientInfoException::class)
  override fun setClientInfo(properties: Properties) {
    // noop
  }

  @Throws(SQLException::class)
  override fun getClientInfo(name: String): String? {
    return null
  }

  @Throws(SQLException::class)
  override fun getHoldability(): Int {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT
  }

  @Throws(SQLException::class)
  override fun setHoldability(holdability: Int) {

  }

  @Throws(SQLException::class)
  override fun getMetaData(): DatabaseMetaData {
    return LsqlJdbcDatabaseMetaData(this)
  }

  @Throws(SQLException::class)
  override fun getTransactionIsolation(): Int {
    return Connection.TRANSACTION_SERIALIZABLE
  }

  @Throws(SQLException::class)
  override fun setTransactionIsolation(level: Int) {

  }

  @Throws(SQLException::class)
  override fun getTypeMap(): Map<String, Class<*>>? {
    return null
  }

  @Throws(SQLException::class)
  override fun setTypeMap(map: Map<String, Class<*>>) {

  }

  @Throws(SQLException::class)
  override fun getWarnings(): SQLWarning? {
    return null
  }

  @Throws(SQLException::class)
  override fun prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int): CallableStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun prepareStatement(sql: String, autoGeneratedKeys: Int): PreparedStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun prepareStatement(sql: String, columnIndexes: IntArray): PreparedStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun prepareStatement(sql: String, columnNames: Array<String>): PreparedStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): PreparedStatement {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun releaseSavepoint(savepoint: Savepoint) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun rollback(savepoint: Savepoint) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLClientInfoException::class)
  override fun setClientInfo(name: String, value: String) {
    // noop
  }

  @Throws(SQLException::class)
  override fun setSavepoint(): Savepoint? {

    return null
  }

  @Throws(SQLException::class)
  override fun setSavepoint(name: String): Savepoint? {

    return null
  }

  @Throws(SQLException::class)
  override fun abort(arg0: Executor) {

  }

  @Throws(SQLException::class)
  override fun getNetworkTimeout(): Int {
    return Constants.NetworkTimeoutException
  }

  /**
   * No schema is supported.
   */
  @Throws(SQLException::class)
  override fun getSchema(): String? {
    return null
  }

  @Throws(SQLException::class)
  override fun setSchema(arg0: String) {
  }

  @Throws(SQLException::class)
  override fun setNetworkTimeout(arg0: Executor, arg1: Int) {
    Constants.NetworkTimeoutException = arg1
  }
}
