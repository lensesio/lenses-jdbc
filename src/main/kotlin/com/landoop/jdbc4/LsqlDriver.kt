package com.landoop.jdbc4

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverPropertyInfo
import java.sql.SQLFeatureNotSupportedException
import java.util.*
import java.util.logging.Logger

class LsqlDriver : Driver {

  override fun getParentLogger(): Logger = throw SQLFeatureNotSupportedException()

  override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> = emptyArray()

  override fun jdbcCompliant(): Boolean = false

  override fun acceptsURL(url: String?): Boolean {
    return url?.toLowerCase(Locale.ENGLISH)?.startsWith(Constants.JdbcPrefix) ?: false
  }

  override fun connect(url: String?, props: Properties?): Connection? {
    if (url == null) {
      throw NullPointerException("url cannot be null")
    } else {
      return if (!acceptsURL(url)) {
        null
      } else {
        LsqlConnection(url, props ?: Properties())
      }
    }
  }

  override fun getMinorVersion(): Int = 0
  override fun getMajorVersion(): Int = 0
}