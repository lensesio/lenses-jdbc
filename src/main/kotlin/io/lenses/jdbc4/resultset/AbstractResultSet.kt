package io.lenses.jdbc4.resultset

import io.lenses.jdbc4.IWrapper
import java.sql.ResultSet
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLWarning

interface AbstractResultSet : ResultSet, IWrapper {

  override fun getCursorName(): String = throw SQLFeatureNotSupportedException()

  override fun clearWarnings() {}
  override fun getWarnings(): SQLWarning? = null

  override fun isWrapperFor(iface: Class<*>?): Boolean = _isWrapperFor(iface)
  override fun <T : Any?> unwrap(iface: Class<T>): T = _unwrap(iface)

}
