//package io.lenses.jdbc4.statements
//
//import io.lenses.jdbc4.client.domain.StreamingSelectResult
//import io.lenses.jdbc4.resultset.EmptyResultSetMetaData
//import io.lenses.jdbc4.resultset.emptyResultSet
//import io.lenses.jdbc4.util.Logging
//import java.sql.Connection
//import java.sql.ParameterMetaData
//import java.sql.ResultSet
//import java.sql.ResultSetMetaData
//import java.sql.SQLFeatureNotSupportedException
//import java.util.concurrent.TimeUnit
//
//class SelectPreparedStatement(private val conn: Connection,
//                              private val sql: String) : AbstractPreparedStatement,
//    ReadOnlyPreparedStatement,
//    UnsupportedTypesPreparedStatement,
//    Logging {
//
//  override fun getResultSet(): ResultSet = TODO()
//
//  override fun getConnection(): Connection = conn
//
//  override fun getQueryTimeout(): Int = 0 // client.connectionRequestTimeout()
//  override fun setQueryTimeout(seconds: Int) = throw UnsupportedOperationException()
//
//  override fun executeUpdate(): Int = throw SQLFeatureNotSupportedException("Cannot call updated on a select query")
//
//  // the last resultset generated by this statement
//  private var rs: ResultSet = emptyResultSet
//
//  /**
//   * Clears the current parameter values immediately.
//   * That is, the current record that is being "built" will be reset to empty.
//   */
//  override fun clearParameters() = throw SQLFeatureNotSupportedException()
//
//  override fun execute(): Boolean {
//    // in this execute method we must block until we are completed
//    // or we receive a record, otherwise we don't know if we can return true or false
//    val result = select(sql)
//    // todo rs = StreamingRowResultSet(this, result)
//    return result.hasData(1, TimeUnit.DAYS)
//  }
//
//  private fun select(sql: String): StreamingSelectResult {
//    //val result = client.select(sql)
//    // todo  rs = StreamingRowResultSet(this, result)
//    return TODO()
//  }
//
//  override fun executeQuery(): ResultSet {
//    select(sql)
//    return rs
//  }
//
//// -- meta data methods
//
//  /**
//   * @return an empty result set because we do not yet support prepared statements for queries
//   */
//  override fun getMetaData(): ResultSetMetaData = EmptyResultSetMetaData
//
//  override fun getParameterMetaData(): ParameterMetaData = throw SQLFeatureNotSupportedException()
//
//
//// == auto generated keys are not supported by kafka/lenses ==
//
//  override fun getGeneratedKeys(): ResultSet = throw SQLFeatureNotSupportedException("Auto generated keys are not supported by Lenses")
//}
//
