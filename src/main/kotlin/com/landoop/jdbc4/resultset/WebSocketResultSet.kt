package com.landoop.jdbc4.resultset

import arrow.core.Either
import arrow.core.getOrHandle
import com.landoop.jdbc4.client.JdbcError
import com.landoop.jdbc4.row.Row
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.WebSocketSession
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.apache.avro.Schema
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement

/**
 * An implementation of [ResultSet] that retrieves records from a [WebSocketSession].
 */
class WebSocketResultSet(private val stmt: Statement?,
                         private val schema: Schema, // the schema for the records that will follow
                         private val ws: WebSocketSession,
                         private val converter: (Frame) -> Either<JdbcError.ParseError, Row?>,
                         private val mapper: (Row) -> Row) : RowResultSet(),
    PullForwardOnlyResultSet,
    ImmutableResultSet,
    UnsupportedTypesResultSet {

  private var closed = false
  private var rowNumber: Int = 0
  private var row: Row? = null

  @ObsoleteCoroutinesApi
  override fun next(): Boolean {
    val frame = runBlocking {
      ws.incoming.receiveOrNull()
    }
    return when (frame) {
      null -> {
        row = null
        false
      }
      else -> {
        rowNumber++
        val next = converter(frame).getOrHandle { throw SQLException(it.t) }
        row = if (next == null) null else mapper(next)
        row != null
      }
    }
  }

  override fun isClosed(): Boolean = closed
  override fun close() {
    runBlocking {
      ws.close()
      closed = true
    }
  }

  override fun getRow(): Int = rowNumber
  override fun currentRow(): Row = row!!

  override fun getMetaData(): ResultSetMetaData = meta()
  override fun meta(): AvroSchemaResultSetMetaData = AvroSchemaResultSetMetaData(schema)

  override fun getStatement(): Statement? = stmt
}

