package com.landoop.rest.domain

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import java.sql.SQLException

data class Message(
    val timestamp: Long,
    val partition: Int,
    val key: String,
    val offset: Long,
    val topic: String,
    val value: String)

data class JdbcData(
    val topic: String?,
    val data: List<String>,
    val schema: String?
)

data class InsertResponse(val name: String)

data class PreparedInsertBody(val topic: String, val records: List<InsertRecord>)

data class InsertRecord(val key: JsonNode, val value: JsonNode)

data class PreparedInsertResponse(val info: PreparedInsertInfo?,
                                  val error: String?)

data class PreparedInsertInfo(val topic: String,
                              val fields: List<InsertField>,
                              val keyType: String,
                              val valueType: String,
                              val keySchema: String?,
                              val valueSchema: String) {

  // returns an avro field that matches the field identified by the parameter index
  // if the field we are asking for is the key then it will return a string type
  // todo support more than string key types
  // 0-indexed
  fun schemaField(param: Int): Schema.Field {
    val parsedSchema = Schema.Parser().parse(valueSchema)
    val field = fields[param]
    return if (field.isKey) {
      Schema.Field(field.name, Schema.create(Schema.Type.STRING), null, null)
    } else {
      // get the schema for our immediate parent, if we have parents, by traversing the tree
      val schema = field.parents.fold(parsedSchema, { schema, part -> schema.getField(part).schema() })
      schema.getField(field.name) ?: throw SQLException("Could not find field $param in ${parsedSchema.fields}")
    }
  }

  /**
   * @return true if the field given by the index is a key field
   * 0-indexed
   */
  fun isKey(param: Int): Boolean = fields[param].isKey
}

data class InsertField(val name: String, val parents: List<String>, val isKey: Boolean) {

  data class Path(val parts: List<String>) {
    override fun toString(): String = parts.joinToString(".")
  }

  fun path(): Path {
    val parents = this.parents.toMutableList()
    parents.add(this.name)
    return Path(parents)
  }
}