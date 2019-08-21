package com.landoop.jdbc4.resultset

import arrow.core.Either
import arrow.core.getOrHandle
import com.landoop.jdbc4.client.JdbcError
import com.landoop.jdbc4.row.Row
import kotlinx.coroutines.runBlocking
import org.apache.avro.Schema
import org.springframework.web.socket.TextMessage
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.BlockingQueue

/**
 * An implementation of [ResultSet] that retrieves records from a websocket via a queue.
 */
class WebSocketResultSet(private val stmt: Statement?,
                         private val schema: Schema, // the schema for the records that will follow
                         private val queue: BlockingQueue<String>,
                         private val converter: (String) -> Either<JdbcError.ParseError, Row?>,
                         private val mapper: (Row) -> Row) : RowResultSet(),
    PullForwardOnlyResultSet,
    ImmutableResultSet,
    UnsupportedTypesResultSet {

  private var closed = false
  private var rowNumber: Int = 0
  private var row: Row? = null

  override fun next(): Boolean {
    return when (val msg = queue.take()) {
      null -> {
        row = null
        false
      }
      else -> {
        rowNumber++
        val next = converter(msg).getOrHandle { throw SQLException(it.t) }
        row = if (next == null) null else mapper(next)
        row != null
      }
    }
  }

  override fun isClosed(): Boolean = closed
  override fun close() {
    // todo close socket
      closed = true
  }

  override fun getRow(): Int = rowNumber
  override fun currentRow(): Row = row!!

  override fun getMetaData(): ResultSetMetaData = meta()
  override fun meta(): AvroSchemaResultSetMetaData = AvroSchemaResultSetMetaData(schema)

  override fun getStatement(): Statement? = stmt
}

