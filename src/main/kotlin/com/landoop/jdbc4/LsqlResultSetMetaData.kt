package com.landoop.jdbc4

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types

class LsqlResultSetMetaData(private val schema: Schema,
                            private val rs: ResultSet) : ResultSetMetaData {

  override fun getTableName(column: Int): String = TODO()

  override fun isNullable(column: Int): Int {
    return when (fieldForIndex(column).schema().isNullable()) {
      true -> ResultSetMetaData.columnNullable
      false -> ResultSetMetaData.columnNullableUnknown
    }
  }

  override fun <T : Any?> unwrap(iface: Class<T>): T {
    try {
      return iface.cast(this)
    } catch (cce: ClassCastException) {
      throw SQLException("Unable to unwrap instance as " + iface.toString())
    }
  }

  override fun isDefinitelyWritable(column: Int): Boolean = false

  override fun isSearchable(column: Int): Boolean = true

  override fun getPrecision(column: Int): Int {
    val field = fieldForIndex(column)
    return when (field.schema().type) {
      Schema.Type.BYTES ->
        when (field.schema().logicalType) {
          is LogicalTypes.Decimal -> (field.schema().logicalType as LogicalTypes.Decimal).precision
          else -> 0
        }
      Schema.Type.FIXED -> field.schema().fixedSize
      else -> 0
    }
  }

  override fun isCaseSensitive(column: Int): Boolean = true

  override fun getScale(column: Int): Int {
    val field = fieldForIndex(column)
    return when (field.schema().type) {
      Schema.Type.BYTES ->
        when (field.schema().logicalType) {
          is LogicalTypes.Decimal -> (field.schema().logicalType as LogicalTypes.Decimal).scale
          else -> 0
        }
      else -> 0
    }
  }

  // required "" when not supported
  override fun getSchemaName(column: Int): String = ""

  // required "" when not supported
  override fun getCatalogName(column: Int): String = ""

  override fun getColumnClassName(column: Int): String = rs.getObject(column).javaClass.canonicalName

  override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(iface)

  override fun getColumnType(column: Int): Int {
    val field = fieldForIndex(column)
    return when (field.schema().type) {
      Schema.Type.ARRAY -> Types.ARRAY
      Schema.Type.BOOLEAN -> Types.BOOLEAN
      Schema.Type.BYTES ->
        when (field.schema().logicalType) {
          is LogicalTypes.Decimal -> Types.DECIMAL
          else -> Types.BINARY
        }
      Schema.Type.DOUBLE -> Types.DOUBLE
      Schema.Type.ENUM -> Types.VARCHAR
      Schema.Type.FIXED -> Types.BINARY
      Schema.Type.FLOAT -> Types.FLOAT
      Schema.Type.INT ->
        when (field.schema().logicalType) {
          is LogicalTypes.TimeMillis -> Types.TIMESTAMP
          is LogicalTypes.Date -> Types.DATE
          else -> Types.INTEGER
        }
      Schema.Type.LONG ->
        when (field.schema().logicalType) {
          is LogicalTypes.TimestampMillis -> Types.TIMESTAMP
          is LogicalTypes.TimestampMicros -> Types.TIMESTAMP
          is LogicalTypes.TimeMicros -> Types.TIMESTAMP
          else -> Types.INTEGER
        }
      Schema.Type.MAP -> Types.STRUCT
      Schema.Type.NULL -> Types.NULL
      Schema.Type.RECORD -> Types.STRUCT
      Schema.Type.STRING -> Types.VARCHAR
      Schema.Type.UNION -> Types.STRUCT
      else -> throw SQLFeatureNotSupportedException()
    }
  }

  override fun isCurrency(column: Int): Boolean = false

  override fun getColumnLabel(column: Int): String = fieldForIndex(column).name()

  override fun isWritable(column: Int): Boolean = false

  override fun isReadOnly(column: Int): Boolean = true

  override fun isSigned(column: Int): Boolean {
    val field = fieldForIndex(column)
    return when (field.schema().type) {
      Schema.Type.BYTES ->
        when (field.schema().logicalType) {
          is LogicalTypes.Decimal -> true
          else -> false
        }
      Schema.Type.DOUBLE -> true
      Schema.Type.FLOAT -> true
      Schema.Type.INT ->
        when (field.schema().logicalType) {
          is LogicalTypes.TimeMillis -> false
          is LogicalTypes.Date -> false
          else -> true
        }
      Schema.Type.LONG ->
        when (field.schema().logicalType) {
          is LogicalTypes.TimestampMillis -> false
          is LogicalTypes.TimestampMicros -> false
          is LogicalTypes.TimeMicros -> false
          else -> true
        }
      else -> false
    }
  }

  override fun getColumnTypeName(column: Int): String = fieldForIndex(column).schema().type.name

  private fun fieldForIndex(index: Int): Schema.Field {
    if (index < 0 || index > schema.fields.size - 1)
      throw IndexOutOfBoundsException("Index $index is out of bounds")
    return schema.fields[index + 1]
  }

  override fun getColumnName(column: Int): String = fieldForIndex(column).name()

  override fun isAutoIncrement(column: Int): Boolean = false

  override fun getColumnDisplaySize(column: Int): Int = 0

  override fun getColumnCount(): Int = schema.fields.size

  // returns the index for a given column label
  internal fun indexForLabel(label: String): Int {
    val index = schema.fields.indexOfFirst { it.name() == label }
    if (index < 0 || index > schema.fields.size - 1)
      throw SQLException("Unknown column $label")
    return index
  }

  // returns the field for a given column label
  internal fun fieldForLabel(label: String): Schema.Field {
    return schema.fields.find { it.name() == label } ?: throw SQLException("Unknown column $label")
  }
}