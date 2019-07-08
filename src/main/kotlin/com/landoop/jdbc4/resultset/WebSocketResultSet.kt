package com.landoop.jdbc4.resultset

import arrow.core.Either
import arrow.core.Try
import arrow.core.getOrHandle
import com.landoop.jdbc4.JacksonSupport
import com.landoop.jdbc4.client.JdbcError
import com.landoop.jdbc4.row.JsonNodeRow
import com.landoop.jdbc4.row.Row
import io.ktor.http.cio.websocket.Frame
import io.ktor.http.cio.websocket.WebSocketSession
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Statement

/**
 * An implementation of [ResultSet] that retrieves records from a [WebSocketSession].
 */
class WebSocketResultSet(private val stmt: Statement?,
                         private val ws: WebSocketSession,
                         private val converter: (Frame) -> Either<JdbcError.ParseError, Row>,
                         private val mapper: (Row) -> Row) : RowResultSet(),
    StreamingResultSet,
    ImmutableResultSet,
    UnsupportedTypesResultSet {

  private var closed = false
  private var rowNumber: Int = 0
  private var row: Row? = null

  override fun close() {
    runBlocking {
      ws.close()
    }
  }

  override val offset: Int = rowNumber

  @ObsoleteCoroutinesApi
  override fun next(): Boolean {
    val frame = runBlocking {
      ws.incoming.receiveOrNull()
    }
    return when (frame) {
      null -> false
      else -> {
        rowNumber++
        row = mapper(converter(frame).getOrHandle { throw it.t })
        true
      }
    }
  }

  override fun isClosed(): Boolean = closed

  override fun getRow(): Int = rowNumber

  override fun currentRow(): Row = row!!

  override fun getMetaData(): ResultSetMetaData = meta()

  override fun meta(): LResultSetMetaData = TODO()

  override fun getStatement(): Statement? = stmt

  companion object {

    private val lensesJdbcRoute: (Frame) -> Either<JdbcError.ParseError, Row> = {
      Try {
        val node = JacksonSupport.mapper.readTree(it.data)
        when (val type = node["type"].textValue()) {
          "SCHEMA" -> JsonNodeRow(node["data"])
          "RECORD" -> JsonNodeRow(node["data"])
          "END" -> EmptyRow
          else -> throw UnsupportedOperationException("Unsupported row type $type")
        }
      }.toEither { JdbcError.ParseError(it) }
    }

    fun lensesJdbcRoute(stmt: Statement?, ws: WebSocketSession, f: (Row) -> Row) =
        WebSocketResultSet(stmt, ws, lensesJdbcRoute, f)
  }
}

