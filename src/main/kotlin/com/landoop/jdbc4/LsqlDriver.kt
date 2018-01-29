package com.landoop.jdbc4

import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.*
import java.util.logging.Logger

class LsqlDriver : Driver, Logging {

  companion object : Logging {
    init {
      logger.debug("Registering LSQL JDBC Driver with DriverManager")
      DriverManager.registerDriver(LsqlDriver())
    }
  }

  override fun getParentLogger(): Logger = throw SQLFeatureNotSupportedException()

  override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
    return arrayOf(
        DriverPropertyInfo("user", null).apply {
          this.required = true
          this.description = "Username for credentials"
        },
        DriverPropertyInfo("password", null).apply {
          this.required = true
          this.description = "Password for credentials"
        },
        DriverPropertyInfo("weakssl", null).apply {
          this.required = false
          this.description = "Set to true if the driver should accept self signed SSL certificates"
        }
    )
  }

  internal fun parseUrl(url: String): Pair<String, Properties> {
    val props = Properties()
    val parts = url.split('?')
    if (parts.size == 2) {
      parts[1].split('&').forEach {
        val (key, value) = it.split('=')
        props[key] = value
      }
    }
    return Pair(parts[0], props)
  }

  override fun jdbcCompliant(): Boolean = false

  override fun acceptsURL(url: String?): Boolean {
    return url?.toLowerCase(Locale.ENGLISH)?.startsWith(Constants.JdbcPrefix) ?: false
  }

  override fun connect(url: String?, props: Properties?): Connection? {
    if (url == null) {
      throw SQLException("url cannot be null")
    } else {
      return if (!acceptsURL(url)) {
        null
      } else {
        val (baseUrl, urlProps) = parseUrl(url)
        if (props != null) {
          props.putAll(urlProps)
        }
        LsqlConnection(baseUrl, props ?: Properties())
      }
    }
  }

  override fun getMinorVersion(): Int = 0
  override fun getMajorVersion(): Int = 0
}