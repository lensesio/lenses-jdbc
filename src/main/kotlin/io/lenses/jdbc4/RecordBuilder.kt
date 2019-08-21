package io.lenses.jdbc4

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.lenses.jdbc4.client.domain.InsertRecord
import io.lenses.jdbc4.client.domain.PreparedInsertInfo
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.SQLException

// used by prepared statements to build up records
class RecordBuilder(val info: PreparedInsertInfo) {

  private val values = mutableMapOf<Int, Any?>()

  fun build(): InsertRecord {
    val root = io.lenses.jdbc4.JacksonSupport.mapper.createObjectNode()
    fun find(parents: List<String>): ObjectNode = parents.fold(root, { node, field ->
      if(node.has(field)) {
        node[field] as ObjectNode
      }else
       {
        node.putObject(field)
      }
    })
    // the key is not included in the value json
    info.fields.withIndex().filterNot { it.value.isKey }.forEach {
      val node = find(it.value.parents)
      val value = values[it.index]
      when (value) {
        null -> node.putNull(it.value.name)
        is String -> node.put(it.value.name, value)
        is Boolean -> node.put(it.value.name, value)
        is Long -> node.put(it.value.name, value)
        is Float -> node.put(it.value.name, value)
        is Int -> node.put(it.value.name, value)
        is Double -> node.put(it.value.name, value)
        is BigInteger -> node.put(it.value.name, value.toDouble())
        is BigDecimal -> node.put(it.value.name, value)
        else -> throw SQLException("Unsupported value type $value")
      }
    }
    val keyValue = info.fields.withIndex()
            .filter { it.value.isKey }
            .map { values[it.index] }
            .firstOrNull()?.toString()
    val key = TextNode(keyValue)
    return InsertRecord(key, root)
  }

  // sets a value by index, where the index is the original position in the sql query
  fun put(index: Int, value: Any?) {
    checkBounds(index)
    // remember jdbc indexes are 1 based
    values[index - 1] = value
  }

  private fun checkBounds(k: Int) {
    if (k < 1 || k > info.fields.size)
      throw IndexOutOfBoundsException("$k is out of bounds")
  }

  // throws an exception if this record is not valid because of missing values
  fun checkRecord() {
    for (k in 0 until info.fields.size) {
      val field = info.fields[k]
      if (!values.containsKey(k))
        throw SQLException("Variable ${field.path()} was not set; You must set all values before executing; if null is desired, explicitly set this using setNull(pos)")
    }
  }
}