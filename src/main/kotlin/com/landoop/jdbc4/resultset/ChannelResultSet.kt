package com.landoop.jdbc4.resultset

import com.landoop.jdbc4.Row
import kotlinx.coroutines.channels.Channel
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement

/**
 * An implementation of [ResultSet] that retrieves records from a [Channel].
 */
class ChannelResultSet(private val stmt: Statement,
                       private val channel: Channel<Row>) : RowResultSet(),
    StreamingResultSet,
    ImmutableResultSet,
    UnsupportedTypesResultSet {

  private var closed = false
  private var rowNumber: Int = 0

  override fun close() {
    TODO()
  }

  override fun next(): Boolean {
    rowNumber++
    return false
  }

  override fun isClosed(): Boolean = closed

  override fun getRow(): Int = rowNumber

  override fun getMetaData(): ResultSetMetaData = meta()

  override var currentRow: Row = empty
  override val cursor: Int = TODO()

  override fun meta(): LResultSetMetaData = TODO()
  override fun getStatement(): Statement = stmt

}

