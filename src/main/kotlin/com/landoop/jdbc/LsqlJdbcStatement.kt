package com.landoop.jdbc

import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

class LsqlJdbcStatement : Statement, AutoCloseable {
  private var currentResultSet: LsqlJdbcJdbcResultSet
  private var httpClient: CloseableHttpClient

  constructor(urls: List<String>) {
    httpClient = HttpClientBuilder.create().build()
  }

  override fun close() {
    httpClient.close()
  }

  @Throws(SQLException::class)
  override fun executeQuery(lsql: String): ResultSet {
    this.execute(lsql)
    return currentResultSet
  }

  @Throws(SQLException::class)
  override fun executeUpdate(cypher: String): Int {
    throw SQLFeatureNotSupportedException()
  }

  @Throws(SQLException::class)
  override fun execute(lsql: String): Boolean {
    //httpConnection.close()

    // execute the query
    val response = httpConnection.executeQuery(lsq, null, java.lang.Boolean.TRUE)

    if (response.hasErrors()) {
      throw SQLException(response.displayErrors())
    }

    // Parse stats
    this.currentUpdateCount = response.getFirstResult().getUpdateCount()

    // Parse response data
    val hasResultSets = response.hasResultSets()

    this.currentResultSet = if (hasResultSets) HttpResultSet(this, response.getFirstResult()) else null

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
}