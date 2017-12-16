package com.landoop.jdbc.domain

data class GenericJdbcRow(private val array: Array<Any>,
                          private val columnMap: Map<String, Int>) : JdbcRow {
  override fun getProperty(column: Int): Any? {
    require(column >= 0 && column < array.size)
    return array.get(column)
  }

  override fun getProperty(column: String): Any? {
    val index = columnMap.getOrElse(column, { throw IllegalArgumentException("The column '$column' is not present") })
    return getProperty(index)
  }

}