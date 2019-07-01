package com.landoop.jdbc4

import com.landoop.jdbc4.util.Logging
import java.util.*

object Versions : Logging {

  private val properties = Properties()

  init {
    this.javaClass.getResourceAsStream("/lsql.versions").use {
      properties.load(it)
    }
  }

  private fun loadOrDefault(key: String): Int {
    val version = properties.getProperty(key)
    return if (version == null) {
      logger.warn("Can not retrieve version information for this build.", null)
      -1
    } else {
      version.toInt()
    }
  }

  fun driverMajorVersion(): Int = loadOrDefault("driver.major")
  fun driverMinorVersion(): Int = loadOrDefault("driver.minor")
  fun databaseMajorVersion(): Int = loadOrDefault("lenses.major")
  fun databaseMinorVersion(): Int = loadOrDefault("lenses.major")
}