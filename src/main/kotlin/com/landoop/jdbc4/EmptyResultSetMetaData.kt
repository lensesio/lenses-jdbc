package com.landoop.jdbc4

import java.sql.ResultSetMetaData

object EmptyResultSetMetaData : ResultSetMetaData, IWrapper {
  override fun isNullable(column: Int): Int = ResultSetMetaData.columnNoNulls
  override fun <T : Any?> unwrap(iface: Class<T>): T = _unwrap(iface)
  override fun isDefinitelyWritable(column: Int): Boolean = false
  override fun isSearchable(column: Int): Boolean = false
  override fun getPrecision(column: Int): Int = 0
  override fun isCaseSensitive(column: Int): Boolean = false
  override fun getScale(column: Int): Int = 0
  override fun getSchemaName(column: Int): String? = null
  override fun getColumnClassName(column: Int): String? = null
  override fun getCatalogName(column: Int): String? = null
  override fun isWrapperFor(iface: Class<*>?): Boolean = false
  override fun getColumnType(column: Int): Int = 0
  override fun isCurrency(column: Int): Boolean = false
  override fun getColumnLabel(column: Int): String? = null
  override fun isWritable(column: Int): Boolean = false
  override fun isReadOnly(column: Int): Boolean = false
  override fun isSigned(column: Int): Boolean = false
  override fun getColumnTypeName(column: Int): String? = null
  override fun getColumnName(column: Int): String? = null
  override fun isAutoIncrement(column: Int): Boolean = false
  override fun getColumnDisplaySize(column: Int): Int = 0
  override fun getColumnCount(): Int = 0
  override fun getTableName(column: Int): String? = null
}
