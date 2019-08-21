package com.landoop.jdbc4.statements

import arrow.core.getOrHandle
import com.landoop.jdbc4.client.LensesClient
import com.landoop.jdbc4.resultset.emptyResultSet
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException

open class LStatement(private val conn: Connection,
                      private val client: LensesClient) : DefaultStatement,
    AutoCloseable,
    IWrapperStatement,
    ReadOnlyStatement,
    OfflineStatement {

  // the last resultset retrieved by this statement
  private var rs: ResultSet = emptyResultSet

  /**
   * Executes the given SQL statement, which returns a single
   * [ResultSet] object.
   *
   * @param sql an SQL statement to be sent to the database, typically a
   *        static SQL SELECT statement
   * @return a [ResultSet] object that contains the data produced
   *         by the given query; never null
   */
  override fun executeQuery(sql: String): ResultSet = runBlocking {
    rs = client.execute(sql) { it }
        .getOrHandle { throw SQLException("Could not execute query: $it") }
    rs
  }

  /**
   *  @return true if the first result is a [ResultSet]
   *         object; false if it is an update count or there are
   *         no results
   */
  override fun execute(sql: String): Boolean {
    executeQuery(sql)
    return true
  }

  override fun getConnection(): Connection = conn
  override fun getResultSet(): ResultSet = rs

  override fun getQueryTimeout(): Int = 0 // client.connectionRequestTimeout()
  override fun setQueryTimeout(seconds: Int) = throw UnsupportedOperationException()
}