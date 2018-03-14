package com.landoop.jdbc4

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema

fun Schema.isNullable(): Boolean {
  return this.type == Schema.Type.UNION &&
      this.types.firstOrNull { it -> it.type == Schema.Type.NULL } != null
}

fun Schema.isNumber(): Boolean {
  return when (this.type) {
    Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.DOUBLE, Schema.Type.LONG -> true
    else -> false
  }
}

fun Schema.scale(): Int {
  val logicalType = this.logicalType
  return when (logicalType) {
    is LogicalTypes.Decimal -> logicalType.scale
    else -> 0
  }
}

fun Schema.fromUnion(): Schema {
  return when (this.type) {
    Schema.Type.UNION -> {
      val schemaTypes = this.types
      return when {
        schemaTypes.size == 1 -> types[0]
        schemaTypes.size == 2 -> schemaTypes.first { it -> it.type != Schema.Type.NULL }
        else -> throw IllegalArgumentException("Not a Union schema")
      }
    }
    else -> this
  }
}