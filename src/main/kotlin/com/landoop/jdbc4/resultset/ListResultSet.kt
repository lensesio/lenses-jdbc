package com.landoop.jdbc4.resultset

import com.landoop.jdbc4.row.Row
import org.apache.avro.Schema
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement

class ListResultSet(val stmt: Statement?,
                    private val schema: Schema?,
                    private val rows: List<Row>) : RowResultSet(),
    ImmutableResultSet,
    UnsupportedTypesResultSet,
    PullForwardOnlyResultSet {

  companion object {
    fun emptyOf(schema:Schema) = ListResultSet(null, schema, emptyList())
  }

  private var rowNumber: Int = 0
  private var row: Row? = null

  override fun getRow(): Int = rowNumber
  override fun currentRow(): Row = row ?: throw SQLException("No current row")

  override fun meta(): ResultSetMetaData = if (schema == null) EmptyResultSetMetaData else AvroSchemaResultSetMetaData(schema)

  override fun next(): Boolean {
    rowNumber++
    return if (rowNumber < rows.size) {
      row = rows[rowNumber]
      true
    } else {
      false
    }
  }

  override fun isClosed(): Boolean = true
  override fun close() {}

  override fun getStatement(): Statement? = stmt
  override fun getMetaData(): ResultSetMetaData = meta()
}