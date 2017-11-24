package com.landoop.jdbc

import org.slf4j.LoggerFactory
import java.sql.*
import java.util.*
import java.util.logging.Logger

class LsqlJdbcDriver : Driver {
  companion object {
    private val Logger = LoggerFactory.getLogger(LsqlJdbcDriver::class.java)

    init {
      try {
        DriverManager.registerDriver(LsqlJdbcDriver())
      } catch (e: SQLException) {
        Logger.error("An error has occurred registering LSQL Jdbc Driver", e)
      }
    }

    fun getVersion(): String {
      return "LSQL ${Constants.getVersion()} JDBC Driver"
    }
  }


  override fun getParentLogger(): Logger? {
    return null
  }

  override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
    return emptyArray()
  }

  override fun jdbcCompliant(): Boolean {
    return false
  }

  override fun acceptsURL(url: String?): Boolean {
    return url?.toLowerCase(Locale.ENGLISH)?.startsWith(Constants.JdbcPrefix) ?: false
  }

  override fun connect(url: String?, info: Properties?): Connection? {
    if (url == null) {
      throw NullPointerException("Null value for the url is not allowed.")
    }
    return if (!acceptsURL(url)) {
      null
    } else {
      LsqlJdbcConnection(url, info)
    }
  }

  override fun getMajorVersion(): Int {
    return Constants.getVersionMajor()
  }

  override fun getMinorVersion(): Int {
    return Constants.getVersionMinor()
  }
}