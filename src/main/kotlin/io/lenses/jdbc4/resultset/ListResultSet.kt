package io.lenses.jdbc4.resultset

import io.lenses.jdbc4.row.Row
import org.apache.avro.Schema
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement

class ListResultSet(val stmt: Statement?,
                    private val schema: Schema?,
                    private val rows: List<Row>) : RowResultSet(),
    ImmutableResultSet,
    UnsupportedTypesResultSet {

  companion object {
    fun emptyOf(schema:Schema) = ListResultSet(null, schema, emptyList())
  }

  private var rowNumber: Int = -1
  private var row: Row? = null

  override fun getRow(): Int = rowNumber
  override fun currentRow(): Row = row ?: throw SQLException("No current row")

  override fun meta(): ResultSetMetaData = if (schema == null) EmptyResultSetMetaData else AvroSchemaResultSetMetaData(schema)

  private fun fetchRow(): Boolean {
    return if (0 <= rowNumber && rowNumber < rows.size) {
      row = rows[rowNumber]
      true
    } else {
      row = null
      false
    }
  }

  override fun next(): Boolean {
    rowNumber++
    return fetchRow()
  }

  override fun isClosed(): Boolean = true
  override fun close() {}

  override fun getStatement(): Statement? = stmt
  override fun getMetaData(): ResultSetMetaData = meta()

  override fun beforeFirst() {
    rowNumber = -1
  }

  override fun isFirst(): Boolean = rowNumber == 0

  override fun isLast(): Boolean = rowNumber == rows.size - 1

  override fun last(): Boolean {
    rowNumber = rows.size - 1
    return fetchRow()
  }

  override fun isAfterLast(): Boolean = rowNumber >= rows.size

  override fun relative(rows: Int): Boolean {
    rowNumber += rows
    return fetchRow()
  }

  override fun absolute(row: Int): Boolean {
    rowNumber = if (row < 0)
      rows.size + row
    else
      row - 1
    return fetchRow()
  }

  override fun first(): Boolean {
    rowNumber = 0
    return fetchRow()
  }

  override fun getType(): Int {
    return ResultSet.TYPE_SCROLL_INSENSITIVE
  }

  override fun setFetchSize(rows: Int) {}

  override fun afterLast() {
    rowNumber = rows.size
  }

  override fun previous(): Boolean {
    rowNumber--
    return rowNumber >= 0 && rowNumber < rows.size
  }

  override fun setFetchDirection(direction: Int) = throw SQLFeatureNotSupportedException()

  override fun getFetchSize(): Int = -1

  override fun isBeforeFirst(): Boolean {
    return rowNumber < 0
  }

  override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
}