package com.landoop.jdbc4

import com.landoop.jdbc4.client.domain.StreamingSelectResult
import org.apache.avro.Schema
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

/**
 * An implementation of ResultSet that uses Avro as an underlying data format.
 * The resultset will generate the metadata from the Avro schema, and each row
 * will be generated from Avro Records.
 *
 * This ResultSet is powered by a [StreamingSelectResult] which is fed with
 * data from the underlying websocket.
 */
class StreamingRowResultSet(
    // the statement that created this resultset
    stmt: Statement?,
    private val result: StreamingSelectResult) : BaseResultSet(stmt) {

  // this is a pointer to the current row, starts off pointing to "before the data"
  // which is the way jdbc works - the user is expected to move the offset before accessing any data
  // jdbc manages row id starting at 1, but we use 0 like it should be, so public api methods
  // must remember to convert
  private var cursor = -1
  private var row: Row? = null

  override fun currentRow(): Row = row ?: throw SQLException("No rows have been fetched; invoke next()")

  override fun meta(): LsqlResultSetMetaData {
    // the call to schema will block until we have the schema
    val schemaString = result.getSchema()
        ?: throw SQLException("No records were retrieved, cannot get metadata on empty resultset")
    val schema = Schema.Parser().parse(schemaString)
    return LsqlResultSetMetaData(schema, this)
  }

  override fun getRow(): Int = cursor

  override fun isClosed(): Boolean = true

  // todo should feed back to the socket to cancel any further data
  override fun close() {}

  // streaming resultsets can only go forwards
  override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY

  override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
  override fun setFetchDirection(direction: Int): Unit = when (direction) {
    ResultSet.FETCH_FORWARD -> Unit
    else -> throw SQLException("Unsupported fetch direction $direction")
  }

  override fun getFetchSize(): Int = -1
  override fun setFetchSize(rows: Int) {} // no op since this resultset is streaming

  // == methods that mutate the cursor are not supported ==

  override fun isLast(): Boolean = false
  override fun isFirst(): Boolean = cursor == 0
  override fun isBeforeFirst(): Boolean = cursor < 0
  override fun isAfterLast(): Boolean = false

  override fun next(): Boolean {
    cursor += 1
    val record = result.next()
    row = if (record == null) null else {
      val node = JacksonSupport.mapper.readTree(record)
      JsonNodeRow(node)
    }
    return record != null
  }

  override fun previous() = throw SQLException("Cannot invoke previous() on streaming Results")
  override fun beforeFirst() = throw SQLException("Cannot invoke beforeFirst() on streaming Results")
  override fun afterLast() = throw SQLException("Cannot invoke afterLast() on streaming Results")
  override fun first() = throw SQLException("Cannot invoke first() on streaming Results")
  override fun last() = throw SQLException("Cannot invoke last() on streaming Results")
  override fun relative(rows: Int) = throw SQLException("Cannot invoke relative() on streaming Results")
  override fun absolute(row: Int) = throw SQLException("Cannot invoke relative() on streaming Results")

}