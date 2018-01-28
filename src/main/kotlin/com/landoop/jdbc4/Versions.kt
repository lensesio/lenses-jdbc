package com.landoop.jdbc4

import java.util.*

object Versions : Logging {

  private val properties = Properties()

  init {
    this.javaClass.getResourceAsStream("/lsql.versions").use {
      properties.load(it)
    }
  }

  private fun loadOrDefault(key: String, default: Int): Int {
    val version = properties.getProperty(key)
    return if (version == null) {
      logger.warn("Can not retrieve version information for this build.", null)
      -1
    } else {
      version.toInt()
    }
  }

  fun driverMajorVersion(): Int = loadOrDefault("driver.major", -1)
  fun driverMinorVersion(): Int = loadOrDefault("driver.minor", -1)
  fun databaseMajorVersion(): Int = loadOrDefault("lenses.major", -1)
  fun databaseMinorVersion(): Int = loadOrDefault("lenses.major", -1)
}