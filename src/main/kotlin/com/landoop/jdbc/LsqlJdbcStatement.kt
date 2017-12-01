package com.landoop.jdbc

import com.landoop.jdbc.domain.JdbcData
import com.landoop.jdbc.domain.LoginRequest
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

class LsqlJdbcStatement(urls: List<String>,
                        private var token: String,
                        private val user: String,
                        private val password: String) : Statement, AutoCloseable {
  private var currentResultSet: LsqlJdbcResultSet?=null
  private var client: LsqlRestClient

  override fun close() {
    client.close()
  }

  @Throws(SQLException::class)
  override fun executeQuery(lsql: String): ResultSet? {
    this.execute(lsql)
    return currentResultSet
  }

  @Throws(SQLException::class)
  override fun executeUpdate(lsql: String): Int {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun execute(lsql: String): Boolean {
    val data: JdbcData = client.executeQuery(lsql, token , LoginRequest(user, password))

     // Parse response data
    val hasResultSets = data.data.isNotEmpty()

    this.currentResultSet = LsqlJdbcResultSet()

    return hasResultSets
  }

  @Throws(SQLException::class)
  override fun getResultSetConcurrency(): Int {
    return ResultSet.CONCUR_READ_ONLY
  }

  @Throws(SQLException::class)
  override fun getResultSetType(): Int {
    return ResultSet.TYPE_FORWARD_ONLY
  }

  @Throws(SQLException::class)
  override fun getResultSetHoldability(): Int {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT
  }

  @Throws(SQLException::class)
  override fun addBatch(sql: String) {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun clearBatch() {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun executeBatch(): IntArray {
    throw SQLFeatureNotSupportedException()
  }

  init {
    client = LsqlRestClient(urls, user, password)
  }
}