package com.landoop.jdbc4

import com.landoop.jdbc4.client.LensesClient
import com.landoop.jdbc4.client.RestClient
import com.landoop.jdbc4.client.domain.Credentials
import com.landoop.jdbc4.statements.InsertPreparedStatement
import com.landoop.jdbc4.statements.LStatement
import com.landoop.jdbc4.statements.SelectPreparedStatement
import com.landoop.jdbc4.util.Logging
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.util.*
import java.util.concurrent.Executor

class LConnection(private val uri: String,
                  props: Properties) : Connection, AutoCloseable, Logging, IWrapper {

  private val user = props.getProperty("user") ?: throw SQLException("URI must specify username")
  private val password = props.getProperty("password", null) ?: throw SQLException("URI must specify password")
  private val weakSSL = props.getProperty("weakssl", "false")!!.toBoolean()

  private val urls = uri.replace(Constants.JdbcPrefix, "").split(',').apply {
    if (this.isEmpty())
      throw SQLException("URI must specify at least one REST endpoint")
    if (!this.all { it.startsWith("http") || it.startsWith("https") })
      throw SQLException("Endpoints must use http or https")
    logger.debug("Connection will use urls $this")
  }

  private val oldclient = RestClient(urls, Credentials(user, password), weakSSL)
  private val client = LensesClient(urls.first(), Credentials(user, password), weakSSL)

  override fun getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  override fun setNetworkTimeout(executor: Executor?, milliseconds: Int) {}

  override fun abort(executor: Executor?) {
    close()
  }

  override fun getClientInfo(name: String?): String? = null

  override fun getClientInfo(): Properties = Properties()

  override fun getAutoCommit(): Boolean = false

  override fun setCatalog(catalog: String?) {
    // javadoc requires no-op if not supported
  }

  override fun getWarnings(): SQLWarning? = null
  override fun clearWarnings() {}

  override fun getCatalog(): String? = null
  override fun getSchema(): String? = null

  // timeout is ignored, and the default timeout of the client is used
  override fun isValid(timeout: Int): Boolean = oldclient.isValid()

  override fun close() {
    oldclient.close()
  }

  override fun isClosed(): Boolean = oldclient.isClosed

  override fun createArrayOf(typeName: String?, elements: Array<out Any>?): java.sql.Array =
      throw SQLFeatureNotSupportedException()

  override fun setReadOnly(readOnly: Boolean) {}
  override fun isReadOnly(): Boolean = true

  override fun isWrapperFor(iface: Class<*>?): Boolean = _isWrapperFor(iface)
  override fun <T : Any?> unwrap(iface: Class<T>): T = _unwrap(iface)

  override fun nativeSQL(sql: String?): String = sql!!

  override fun setClientInfo(name: String?, value: String?) = throw SQLFeatureNotSupportedException()
  override fun setClientInfo(properties: Properties?) = throw SQLFeatureNotSupportedException()

  override fun createStatement(): Statement = LStatement(this, oldclient)
  override fun prepareStatement(sql: String): PreparedStatement {
    return if (sql.trim().toUpperCase().startsWith("SELECT")) SelectPreparedStatement(this,
        oldclient,
        sql)
    else InsertPreparedStatement(this, oldclient, sql)
  }

  override fun getTypeMap(): MutableMap<String, Class<*>> = throw SQLFeatureNotSupportedException()
  override fun getMetaData(): DatabaseMetaData = LDatabaseMetaData(this, client, uri, user)

  override fun setSchema(schema: String?) {
    // javadoc requests noop for non-supported
  }

  override fun getNetworkTimeout(): Int = oldclient.connectionRequestTimeout()

  override fun setTypeMap(map: MutableMap<String, Class<*>>?) = throw SQLFeatureNotSupportedException()

  // -- unsupported prepared statement variants

  override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int): PreparedStatement {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY)
      throw SQLFeatureNotSupportedException("ResultSetType $resultSetType is not supported")
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
      throw SQLFeatureNotSupportedException("ResultSetConcurrency $resultSetConcurrency is not supported")
    return prepareStatement(sql)
  }

  override fun prepareStatement(sql: String,
                                resultSetType: Int,
                                resultSetConcurrency: Int,
                                resultSetHoldability: Int): PreparedStatement = prepareStatement(sql, resultSetType, resultSetConcurrency)

  override fun prepareStatement(sql: String?, autoGeneratedKeys: Int): PreparedStatement = throw SQLFeatureNotSupportedException("Use prepareStatement(sql)")
  override fun prepareStatement(sql: String?, columnIndexes: IntArray?): PreparedStatement = throw SQLFeatureNotSupportedException("Use prepareStatement(sql)")
  override fun prepareStatement(sql: String?, columnNames: Array<out String>?): PreparedStatement = throw SQLFeatureNotSupportedException("Use prepareStatement(sql)")

  override fun prepareCall(sql: String?): CallableStatement = throw SQLFeatureNotSupportedException()
  override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int): CallableStatement = throw SQLFeatureNotSupportedException()
  override fun prepareCall(sql: String?, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): CallableStatement = throw SQLFeatureNotSupportedException()

  // -- unsupported create statement methods

  override fun createStatement(resultSetType: Int, resultSetConcurrency: Int): Statement =
      throw SQLFeatureNotSupportedException("ResultSet type and ResultSet concurrency are not supported, use the createStatement() function")

  override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement =
      throw SQLFeatureNotSupportedException("ResultSet type and ResultSet concurrency are not supported, use the createStatement() function")

  // -- tx methods are unsupported

  override fun setTransactionIsolation(level: Int) {}
  override fun getTransactionIsolation(): Int = Connection.TRANSACTION_NONE
  override fun setAutoCommit(autoCommit: Boolean) {}
  override fun rollback() = throw SQLFeatureNotSupportedException()
  override fun rollback(savepoint: Savepoint?) = throw SQLFeatureNotSupportedException()
  override fun commit() = throw SQLFeatureNotSupportedException()
  override fun setSavepoint(): Savepoint = throw SQLFeatureNotSupportedException()
  override fun setSavepoint(name: String?): Savepoint = throw SQLFeatureNotSupportedException()
  override fun releaseSavepoint(savepoint: Savepoint?) = throw SQLFeatureNotSupportedException()

  // -- unsupported methods

  override fun createClob(): Clob = throw   SQLFeatureNotSupportedException()
  override fun createNClob(): NClob = throw SQLFeatureNotSupportedException()
  override fun createBlob(): Blob = throw SQLFeatureNotSupportedException()
  override fun createSQLXML(): SQLXML = throw SQLFeatureNotSupportedException()
  override fun createStruct(typeName: String?, attributes: Array<out Any>?): Struct = throw SQLFeatureNotSupportedException()
  override fun setHoldability(holdability: Int) = throw SQLFeatureNotSupportedException()

}