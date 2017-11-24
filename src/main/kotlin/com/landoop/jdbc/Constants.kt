package com.landoop.jdbc

import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*

object Constants {
  private val Logger = LoggerFactory.getLogger(Constants.javaClass)

  val JdbcPrefix = "jdbc:lsql:kafka:"
  val DatabaseName = "default"
  var NetworkTimeoutException = 15000
  private val properties = Properties()

  init {
    val inputStream = Constants::class.java.getResourceAsStream("lsql.properties")
    try {
      properties.load(inputStream)
    } catch (e: IOException) {
      Logger.warn("Failed to load LSQL properties", e)
    } finally {
      try {
        inputStream?.close()
      } catch (t: Throwable) {
      }
    }
  }

  /**
   * @return Major part of OrientDB version
   */
  fun getVersionMajor(): Int {
    val versions = properties.getProperty("version").split('.').dropLastWhile { it.isEmpty() }.toTypedArray()
    if (versions.size == 0) {
      Logger.warn("Can not retrieve version information for this build.", null)
      return -1
    }

    try {
      return Integer.parseInt(versions[0])
    } catch (nfe: NumberFormatException) {
      Logger.warn("Can not retrieve major version information for this build.", nfe)
      return -1
    }

  }

  /**
   * @return Minor part of OrientDB version
   */
  fun getVersionMinor(): Int {
    val versions = properties.getProperty("version").split('.').dropLastWhile { it.isEmpty() }.toTypedArray()
    if (versions.size < 2) {
      Logger.warn("Can not retrieve minor version information for this build")
      return -1
    }

    try {
      return Integer.parseInt(versions[1])
    } catch (nfe: NumberFormatException) {
      Logger.warn("Can not retrieve minor version information for this build", nfe)
      return -1
    }

  }


  /**
   * @return Returns only current version without build number and etc.
   */
  fun getRawVersion(): String {
    return properties.getProperty("version")
  }

  /**
   * Returns the complete text of the current OrientDB version.
   */
  fun getVersion(): String {
    return "${properties.getProperty("version")} (build ${properties.getProperty("revision")})"
  }
}