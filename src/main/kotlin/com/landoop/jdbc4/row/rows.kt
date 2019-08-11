package com.landoop.jdbc4.row

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.generic.GenericData
import java.math.BigDecimal
import java.sql.SQLException

/**
 * An implementation of [Row] that uses a static list of values
 * provided at construction time.
 */
class ArrayRow(private val array: Array<Any?>) : ConvertingRow() {
  override fun getObject(index: Int): Any? = array[index - 1]
}

class ListRow(private val list: List<Any?>) : ConvertingRow() {
  override fun getObject(index: Int): Any? = list[index - 1]
}

/**
 * An implementation of [Row] that uses a Json node to provide values.
 */
class JsonNodeRow(private val node: JsonNode) : ConvertingRow() {
  override fun getObject(index: Int): Any? {
    // remember jdbc is 1-indexed
    val n = node.fields().asSequence().toList()[index - 1].value
    return when {
      n.isBigDecimal -> BigDecimal(n.bigIntegerValue())
      n.isBigInteger -> n.bigIntegerValue()
      n.isBinary -> n.binaryValue()
      n.isBoolean -> n.asBoolean()
      n.isDouble -> n.doubleValue()
      n.isFloat -> n.floatValue()
      n.isInt -> n.intValue()
      n.isLong -> n.longValue()
      n.isShort -> n.shortValue()
      n.isTextual -> n.asText()
      else -> throw SQLException("Unsupported node type ${n.javaClass}:$n")
    }
  }
}

class RecordRow(val record: GenericData.Record) : ConvertingRow() {
  override fun getObject(index: Int): Any? {
    return record.get(index - 1)
  }
}