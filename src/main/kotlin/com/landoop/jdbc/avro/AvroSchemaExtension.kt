package com.landoop.jdbc.avro


import com.landoop.jdbc.domain.JdbcField
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import java.sql.SQLException
import java.sql.Types
import java.sql.Types.*

fun Schema.isNullable(): Boolean {
  return this.getType() == Schema.Type.UNION &&
      this.getTypes().firstOrNull { it -> it.getType() == Schema.Type.NULL } != null
}


fun Schema.fromUnion(): Schema {
  val schemaTypes = this.getTypes()
  if (schemaTypes.size == 1) {
    return types[0]
  } else if (schemaTypes.size == 2) {
    return schemaTypes.filter { it -> it.type != Schema.Type.NULL }.first()
  } else {
    throw IllegalArgumentException("Not a Union schema")
  }
}


/**
 * Should we convert complex types to string and let the user see the string
 */
fun Schema.toJdbcFields(): List<JdbcField>{
  return when (this.type) {
    Schema.Type.RECORD ->
      this.fields.foldIndexed(emptyList<JdbcField>(), { index, list, it ->
        val jdbcField = it.toJdbc(index)
        list + jdbcField
      })
    Schema.Type.ARRAY -> listOf(JdbcField("", 0, VARCHAR))
    Schema.Type.NULL -> listOf(JdbcField("", 0, VARCHAR))
    Schema.Type.BOOLEAN -> listOf(JdbcField("", 0, BOOLEAN))
    Schema.Type.BYTES -> {
      val typeName = this.getProp(LogicalType.LOGICAL_TYPE_PROP)
      val jdbcType = when (typeName) {
        "decimal" -> Types.DECIMAL
        "uuid" -> Types.VARCHAR
        "date" -> Types.DATE
        "time-millis" -> Types.DATE
        "time-micros" -> Types.DATE
        "timestamp-millis" -> Types.DATE
        "timestamp-micros" -> Types.DATE
        else -> BINARY
      }
      listOf(JdbcField("", 0, jdbcType))
    }
        Schema . Type . FIXED ->listOf(JdbcField("", 0, BINARY))
    Schema.Type.DOUBLE -> listOf(JdbcField("", 0, DOUBLE))
    Schema.Type.FLOAT -> listOf(JdbcField("", 0, FLOAT))
    Schema.Type.ENUM -> listOf(JdbcField("", 0, VARCHAR))
    Schema.Type.INT -> listOf(JdbcField("", 0, INTEGER))
    Schema.Type.LONG -> listOf(JdbcField("", 0, BIGINT))
    Schema.Type.MAP -> listOf(JdbcField("", 0, VARCHAR))
    Schema.Type.STRING -> listOf(JdbcField("", 0, VARCHAR))
    Schema.Type.UNION ->
      if (!this.isNullable()) {
        //throw SQLException("Invalid type metadata received. Union of types is not allowed.")
        listOf(JdbcField("", 0, VARCHAR))
      } else {
        this.fromUnion().toJdbcFields()
      }
  }
}


fun Schema.Field.toJdbc(index: Int): JdbcField {
  fun mapAvroTypeToJdbc(schema: Schema): Int {
    val type = when (schema.type) {
      Schema.Type.RECORD -> VARCHAR
      Schema.Type.ARRAY -> VARCHAR
      Schema.Type.NULL -> VARCHAR
      Schema.Type.BOOLEAN -> BOOLEAN
      Schema.Type.BYTES -> BINARY
      Schema.Type.FIXED -> BINARY
      Schema.Type.DOUBLE -> DOUBLE
      Schema.Type.FLOAT -> FLOAT
      Schema.Type.ENUM -> VARCHAR
      Schema.Type.INT -> INTEGER
      Schema.Type.LONG -> BIGINT
      Schema.Type.MAP -> VARCHAR
      Schema.Type.STRING -> VARCHAR
      Schema.Type.UNION ->
        if (!this.schema().isNullable()) VARCHAR
        else mapAvroTypeToJdbc(this.schema().fromUnion())
      else -> VARCHAR
    }
    return type
  }

  val typeName = this.schema().getProp(LogicalType.LOGICAL_TYPE_PROP)

  val jdbcType = when (typeName) {
    "decimal" -> Types.DECIMAL
    "uuid" -> Types.VARCHAR
    "date" -> Types.DATE
    "time-millis" -> Types.DATE
    "time-micros" -> Types.DATE
    "timestamp-millis" -> Types.DATE
    "timestamp-micros" -> Types.DATE
    else -> mapAvroTypeToJdbc(this.schema())
  }

  return JdbcField(this.name(), index, jdbcType)
}