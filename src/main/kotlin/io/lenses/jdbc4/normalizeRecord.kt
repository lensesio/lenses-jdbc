package io.lenses.jdbc4

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.NullNode
import org.apache.avro.Schema

fun normalizeRecord(schema: Schema, node: JsonNode, prefix: String = ""): List<Pair<String, Any?>> {
  // todo expand this algo to cover non-record types
  require(schema.type == Schema.Type.RECORD) {
    val a = "qwe"
    "Unsupported type $schema"
  }
  return schema.fields.flatMap { field ->
    val childNode = node[field.name()]
    when {
      childNode == null && field.schema().type == Schema.Type.RECORD ->
        normalizeRecord(field.schema(), NullNode.instance, prefix + field.name() + ".")
      childNode == null -> listOf((prefix + field.name()) to null)
      childNode.isArray -> listOf((prefix + field.name()) to childNode.elements().asSequence().map {
        normalizeRecord(field.schema(), it, prefix)
      })
      childNode.isObject -> normalizeRecord(field.schema(), childNode, prefix + field.name() + ".")
      else -> {
        val value = valueFromNode(childNode)
        listOf((prefix + field.name()) to value)
      }
    }
  }
}

fun valueFromNode(node: JsonNode): Any? = when {
  node.isBigDecimal -> node.decimalValue()
  node.isTextual -> node.textValue()
  node.isBigInteger -> node.bigIntegerValue()
  node.isBinary -> node.binaryValue()
  node.isBoolean -> node.booleanValue()
  node.isDouble -> node.doubleValue()
  node.isFloat -> node.floatValue()
  node.isInt -> node.intValue()
  node.isLong -> node.longValue()
  node.isNull -> null
  node.isShort -> node.shortValue()
  else -> throw UnsupportedOperationException()
}