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
    return when (schemaForIndex(column).isNullable()) {
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
    val schema = schemaForIndex(column)
    return when (typeForIndex(column)) {
      Schema.Type.BYTES ->
        when (schema.logicalType) {
          is LogicalTypes.Decimal -> (schema.logicalType as LogicalTypes.Decimal).precision
          else -> 0
        }
      Schema.Type.FIXED -> schema.fixedSize
      Schema.Type.STRING -> Int.MAX_VALUE
      else -> 0
    }
  }

  override fun isCaseSensitive(column: Int): Boolean = true

  override fun getScale(column: Int): Int {
    return when (typeForIndex(column)) {
      Schema.Type.BYTES -> {
        val logicalType = schemaForIndex(column).logicalType
        when (logicalType) {
          is LogicalTypes.Decimal -> logicalType.scale
          else -> 0
        }
      }
      else -> 0
    }
  }

  // required "" when not supported
  override fun getSchemaName(column: Int): String = ""

  // required "" when not supported
  override fun getCatalogName(column: Int): String = ""

  override fun getColumnClassName(column: Int): String {
    val type = typeForIndex(column)
    return when (type) {
      Schema.Type.BOOLEAN -> java.lang.Boolean::class.java.canonicalName
      Schema.Type.BYTES -> byteArrayOf(1)::class.java.canonicalName
      Schema.Type.ENUM -> java.lang.String::class.java.canonicalName
      Schema.Type.DOUBLE -> java.lang.Double::class.java.canonicalName
      Schema.Type.FLOAT -> java.lang.Float::class.java.canonicalName
      Schema.Type.INT -> java.lang.Integer::class.java.canonicalName
      Schema.Type.LONG -> java.lang.Long::class.java.canonicalName
      Schema.Type.STRING -> java.lang.String::class.java.canonicalName
      else -> throw SQLException("Unknown class name for $type")
    }
  }

  override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(iface)

  override fun getColumnType(column: Int): Int {

    fun getColumnType(schema: Schema): Int {
      return when (schema.type) {
        Schema.Type.ARRAY -> Types.ARRAY
        Schema.Type.BOOLEAN -> Types.BOOLEAN
        Schema.Type.BYTES ->
          when (schema.logicalType) {
            null -> Types.BINARY
            is LogicalTypes.Decimal -> Types.DECIMAL
            else -> {
              if (schema.logicalType.name == "uuid") Types.VARCHAR
              else Types.BINARY
            }
          }
        Schema.Type.DOUBLE -> Types.DOUBLE
        Schema.Type.ENUM -> Types.VARCHAR
        Schema.Type.FIXED -> Types.BINARY
        Schema.Type.FLOAT -> Types.FLOAT
        Schema.Type.INT ->
          when (schema.logicalType) {
            is LogicalTypes.TimeMillis -> Types.TIME
            is LogicalTypes.Date -> Types.DATE
            else -> Types.INTEGER
          }
        Schema.Type.LONG ->
          when (schema.logicalType) {
            is LogicalTypes.TimestampMillis -> Types.TIMESTAMP
            is LogicalTypes.TimestampMicros -> Types.TIMESTAMP
            is LogicalTypes.TimeMicros -> Types.TIMESTAMP
            else -> Types.BIGINT
          }
        Schema.Type.MAP -> Types.STRUCT
        Schema.Type.NULL -> Types.NULL
        Schema.Type.RECORD -> Types.STRUCT
        Schema.Type.STRING -> Types.VARCHAR
        Schema.Type.UNION -> getColumnType(schema.fromUnion())
        else -> throw SQLFeatureNotSupportedException()
      }
    }

    val schema = schemaForIndex(column)
    return getColumnType(schema)
  }

  override fun isCurrency(column: Int): Boolean = false

  override fun getColumnName(column: Int): String = getColumnLabel(column)
  override fun getColumnLabel(column: Int): String {
    return when (schema.type) {
      Schema.Type.RECORD -> schema.fields[column - 1].name()
      else -> "unnamed"
    }
  }

  override fun isWritable(column: Int): Boolean = false

  override fun isReadOnly(column: Int): Boolean = true

  override fun isSigned(column: Int): Boolean {
    val type = typeForIndex(column)
    val schema = schemaForIndex(column)
    return when (type) {
      Schema.Type.BYTES ->
        when (schema.logicalType) {
          is LogicalTypes.Decimal -> true
          else -> false
        }
      Schema.Type.DOUBLE -> true
      Schema.Type.FLOAT -> true
      Schema.Type.INT ->
        when (schema.logicalType) {
          is LogicalTypes.TimeMillis -> false
          is LogicalTypes.Date -> false
          else -> true
        }
      Schema.Type.LONG ->
        when (schema.logicalType) {
          is LogicalTypes.TimestampMillis -> false
          is LogicalTypes.TimestampMicros -> false
          is LogicalTypes.TimeMicros -> false
          else -> true
        }
      else -> false
    }
  }

  override fun getColumnTypeName(column: Int): String = typeForIndex(column).name

  private fun schemaForIndex(index: Int): Schema {
    return when (schema.type) {
      Schema.Type.RECORD -> {
        if (index < 1 || index > schema.fields.size)
          throw IndexOutOfBoundsException("Index $index is out of bounds; note: JDBC drivers are 1-indexed")
        schema.fields[index - 1].schema()
      }
      else -> {
        if (index != 1)
          throw IndexOutOfBoundsException("Index $index is out of bounds; note: JDBC drivers are 1-indexed")
        schema
      }
    }
  }

  private fun typeForIndex(index: Int): Schema.Type {
    return when (schema.type) {
      Schema.Type.RECORD -> {
        if (index < 1 || index > schema.fields.size)
          throw IndexOutOfBoundsException("Index $index is out of bounds; note: JDBC drivers are 1-indexed")
        schema.fields[index - 1].schema().type
      }
      else -> {
        if (index != 1)
          throw IndexOutOfBoundsException("Index $index is out of bounds; note: JDBC drivers are 1-indexed")
        schema.type
      }
    }
  }

  override fun isAutoIncrement(column: Int): Boolean = false

  override fun getColumnDisplaySize(column: Int): Int = 0

  override fun getColumnCount(): Int {
    // can be a record or a single field
    return when (schema.type) {
      Schema.Type.RECORD -> schema.fields.size
      else -> 1
    }
  }

  // returns the index for a given column label
  // 1-indexed
  internal fun indexForLabel(label: String): Int {
    val index = schema.fields.indexOfFirst { it.name() == label }
    if (index < 0 || index > schema.fields.size -1)
      throw SQLException("Unknown column $label")
    return index + 1
  }

  // returns the field for a given column label
  internal fun fieldForLabel(label: String): Schema.Field {
    return schema.fields.find { it.name() == label } ?: throw SQLException("Unknown column $label")
  }
}