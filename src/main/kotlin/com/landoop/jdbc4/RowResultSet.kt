package com.landoop.jdbc4

import com.landoop.jdbc4.resultset.LResultSetMetaData
import com.landoop.jdbc4.row.RecordRow
import com.landoop.jdbc4.row.Row
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

/**
 * An implementation of ResultSet that uses Avro as an underlying data format.
 * The resultset will generate the metadata from the Avro schema, and each row
 * will be generated from Avro Records.
 */
class RowResultSet(
    // the statement that created this resultset
    stmt: Statement?,
    // the schema for this resultset
    val schema: Schema,
    // all rows for the resultset are wrapped in a Row abstraction to support extra methods
    private val rows: List<Row>) : BaseResultSet(stmt) {

  internal companion object {

    private val emptySchema: Schema = SchemaBuilder.fixed("empty").size(1)

    fun empty() = RowResultSet(null, emptySchema, emptyList())

    fun emptyOf(schema: Schema) = RowResultSet(null, schema, emptyList())

    fun fromRecords(schema: Schema, records: Collection<GenericData.Record>) =
        RowResultSet(
            null,
            schema,
            records.map { RecordRow(it) }
        )
  }

  // this is a pointer to the current row, starts off pointing to "before the data"
  // which is the way jdbc works - the user is expected to move the offset before accessing any data
  // jdbc manages row id starting at 1, but we use 0 like it should be, so public api methods
  // must remember to convert
  private var cursor = -1

  // these resultsets are entirely offline
  override fun isClosed(): Boolean = true

  override fun close() {}

  override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

  override fun meta(): LResultSetMetaData = LResultSetMetaData(
      schema,
      this)

  // == methods which mutate or query the resultset fetch parameters ==

  // our resultsets are always offline so can be scroll insensitive
  override fun getType(): Int = ResultSet.TYPE_SCROLL_INSENSITIVE

  override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
  override fun setFetchDirection(direction: Int) = throw SQLException("This resultset is offline, so fetch direction does not need to be set")

  override fun getFetchSize(): Int = -1
  override fun setFetchSize(rows: Int) {} // no op since this resultset is offline

  // == methods that mutate or query the cursor ==

  private val size = rows.size
  private val last = size - 1

  override fun getRow(): Int = cursor

  // returns the row at the current cursor position
  override fun currentRow(): Row = rows[cursor]

  override fun isLast(): Boolean = cursor == last
  override fun isFirst(): Boolean = cursor == 0
  override fun isBeforeFirst(): Boolean = cursor < 0
  override fun isAfterLast(): Boolean = cursor > last

  override fun next(): Boolean {
    cursor += 1
    if (cursor > last + 1)
      cursor = last + 1
    return cursor in 0..last
  }

  override fun previous(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke previous() on ResultSet.TYPE_FORWARD_ONLY")
    cursor -= 1
    if (cursor < -1)
      cursor = -1
    return cursor in 0..last
  }

  override fun beforeFirst() {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke beforeFirst() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = -1
  }

  override fun afterLast() {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke afterLast() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = last + 1
  }

  override fun first(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke first() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = 0
    return true
  }

  override fun last(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke last() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = rows.size - 1
    return true
  }

  override fun relative(rows: Int): Boolean {
    val p = cursor + rows
    checkCursorBounds(p)
    cursor = p
    return true
  }

  override fun absolute(row: Int): Boolean {
    return if (row < 0) {
      val positiveRow = size - Math.abs(row) + 1
      absolute(positiveRow)
    } else {
      // minus 1 because the public API is 1 indexed
      checkCursorBounds(row - 1)
      cursor = row - 1
      true
    }
  }

  private fun checkCursorBounds(p: Int) {
    if (p < 0 || p > last)
      throw IndexOutOfBoundsException("Attempted to move cursor out of bounds: $p")
  }
}