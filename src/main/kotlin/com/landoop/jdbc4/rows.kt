package com.landoop.jdbc4

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.generic.GenericData

class JsonNodeRow(val node: JsonNode) : ConvertingRow() {
  override fun getObject(index: Int): Any? {
    return node.get(index)
  }
}

class RecordRow(val record: GenericData.Record) : ConvertingRow() {
  override fun getObject(index: Int): Any? {
    return record.get(index)
  }
}