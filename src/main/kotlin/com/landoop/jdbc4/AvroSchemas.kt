package com.landoop.jdbc4

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types

object AvroSchemas {

  fun normalizedName(schema: Schema): String {
    return when (schema.type) {
      Schema.Type.UNION -> schema.fromUnion().type.name
      else -> schema.type.name
    }
  }

  /**
   * @return the JVM fully qualified classname for the given avro type.
   */
  fun jvmClassName(type: Schema.Type): String {
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

  fun sqlType(schema: Schema): Int {
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
      Schema.Type.UNION -> sqlType(schema.fromUnion())
      else -> throw SQLFeatureNotSupportedException()
    }
  }
}