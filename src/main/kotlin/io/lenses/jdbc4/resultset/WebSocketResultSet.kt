package io.lenses.jdbc4.resultset

import arrow.core.Either
import arrow.core.getOrHandle
import io.lenses.jdbc4.client.JdbcError
import io.lenses.jdbc4.row.Row
import io.lenses.jdbc4.util.Logging
import org.apache.avro.Schema
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.BlockingQueue

interface WebsocketConnection {
  val queue: BlockingQueue<String>
  fun close()
  fun isClosed(): Boolean
}

abstract class StreamingRowResultSet : RowResultSet(),
    PullForwardOnlyResultSet,
    ImmutableResultSet,
    UnsupportedTypesResultSet,
    Logging

/**
 * An implementation of [ResultSet] that retrieves records from a websocket via a queue.
 */
class WebSocketResultSet(private val stmt: Statement?,
                         private val schema: Schema, // the schema for the records that will follow
                         private val conn: WebsocketConnection,
                         private val converter: (String) -> Either<JdbcError.ParseError, Row?>) : StreamingRowResultSet() {

  private var rowNumber: Int = 0
  private var row: Row? = null
  private var completed = false

  override fun next(): Boolean {
    return if (completed) false else {
      when (val msg = conn.queue.take()) {
        null -> {
          row = null
          completed = true
          conn.close()
          false
        }
        else -> {
          rowNumber++
          row = converter(msg).getOrHandle { throw SQLException(it.cause) }
          row != null
        }
      }
    }
  }

  override fun isClosed(): Boolean = conn.isClosed()
  override fun close() {
    conn.close()
  }

  override fun getRow(): Int = rowNumber
  override fun currentRow(): Row = row!!

  override fun getMetaData(): ResultSetMetaData = meta()
  override fun meta(): AvroSchemaResultSetMetaData = AvroSchemaResultSetMetaData(schema)

  override fun getStatement(): Statement? = stmt
}

