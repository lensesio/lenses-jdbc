package io.lenses.jdbc4.resultset

import io.lenses.jdbc4.row.Row
import org.apache.avro.Schema
import java.sql.ResultSetMetaData
import java.sql.Statement

fun RowResultSet.map(schema: Schema, f: (Row) -> Row): RowResultSet {

  val self = this

  return object : StreamingRowResultSet() {

    override fun currentRow(): Row = f(self.currentRow())
    override fun getRow(): Int = self.row

    override fun next(): Boolean = self.next()
    override fun getStatement(): Statement = self.statement

    override fun close(): Unit = self.close()
    override fun isClosed(): Boolean = self.isClosed

    override fun meta(): ResultSetMetaData = AvroSchemaResultSetMetaData(schema)
    override fun getMetaData(): ResultSetMetaData = meta()
  }
}