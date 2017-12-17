package com.landoop.jdbc.domain

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.sql.SQLException
import java.util.function.Function

data class LsqlJdbcRow(private val underlyingData: ObjectNode,
                       private val conversions: Map<String, Function<JsonNode, Any>>,
                       private val columnMap: Map<String, Int>) : JdbcRow {
  override fun getProperty(column: Int): Any? {
    require(column >= 0 && column < conversions.size - 1, { throw SQLException("Invalid column index $column. Expecting a value between 0 and ${conversions.size}") })
    underlyingData.get(column)
    return null
  }

  override fun getProperty(column: String): Any? {
    val value = underlyingData.get(column)
    if (value == null) {
      return null
    }
    val conversionFn = conversions.getOrElse(column, { throw SQLException("Column '${column}' can not be found. Available columns are:${columnMap.keys.joinToString { "," }} ") })
    return conversionFn.apply(value)
  }
}