package io.lenses.jdbc4.row

import java.io.Reader
import java.io.StringReader
import java.math.BigDecimal
import java.sql.RowId
import java.sql.SQLException
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.ZoneId
import java.util.*

/**
 * An implementation of [Row] that will attempt to convert its values
 * to the required data type.
 */
abstract class ConvertingRow : Row {

  override fun getByte(index: Int): Byte {
    return when (val value = getObject(index)) {
      is Int -> value.toByte()
      is Long -> value.toByte()
      is String -> value.toByte()
      else -> throw SQLException("Unable to convert $value to byte")
    }
  }

  override fun charStream(index: Int): Reader {
    return when (val value = getObject(index)) {
      is String -> StringReader(value)
      else -> throw SQLException("Unable to convert $value to Reader")
    }
  }

  override fun getFloat(index: Int): Float {
    return when (val value = getObject(index)) {
      is Double -> value.toFloat()
      is Float -> value
      is String -> value.toFloat()
      else -> throw SQLException("Unable to convert $value to float")
    }
  }

  override fun getBoolean(index: Int): Boolean {
    return when (val value = getObject(index)) {
      is Boolean -> value
      is String -> value == "true"
      else -> throw SQLException("Unable to convert $value to boolean")
    }
  }

  override fun getBigDecimal(index: Int, scale: Int): BigDecimal {
    return when (val value = getObject(index)) {
      is Long -> BigDecimal.valueOf(value, scale)
      is Int -> BigDecimal.valueOf(value.toLong(), scale)
      is String -> BigDecimal(value)
      else -> throw SQLException("Unable to convert $value to BigDecimal")
    }
  }

  override fun getBigDecimal(index: Int): BigDecimal {
    return when (val value = getObject(index)) {
      is Long -> BigDecimal.valueOf(value)
      is Int -> BigDecimal.valueOf(value.toLong())
      is Double -> BigDecimal.valueOf(value)
      is Float -> BigDecimal.valueOf(value.toDouble())
      is String -> BigDecimal(value)
      else -> throw SQLException("Unable to convert $value to BigDecimal")
    }
  }

  override fun getTime(index: Int): Time = getTime(index, null)
  override fun getTime(index: Int, cal: Calendar?): Time {
    val instant = when (val value = getObject(index)) {
      is Int -> Instant.ofEpochMilli(value.toLong())
      is Long -> Instant.ofEpochMilli(value)
      is String -> Instant.ofEpochMilli(value.toLong())
      else -> throw SQLException("Unable to convert $value to Time")
    }
    val zone = cal?.timeZone?.toZoneId() ?: ZoneId.of("Z")
    return Time.valueOf(instant.atZone(zone).toLocalTime())
  }

  override fun getDate(index: Int): java.sql.Date = getDate(index, null)
  override fun getDate(index: Int, cal: Calendar?): java.sql.Date {
    val instant = when (val value = getObject(index)) {
      is Int -> Instant.ofEpochMilli(value.toLong())
      is Long -> Instant.ofEpochMilli(value)
      is String -> Instant.ofEpochMilli(value.toLong())
      else -> throw SQLException("Unable to convert $value to java.sql.Date")
    }
    val zone = cal?.timeZone?.toZoneId() ?: ZoneId.of("Z")
    return java.sql.Date(instant.atZone(zone).toInstant().toEpochMilli())
  }

  override fun getTimestamp(index: Int): Timestamp = getTimestamp(index, null)
  override fun getTimestamp(index: Int, cal: Calendar?): Timestamp {
    val instant = when (val value = getObject(index)) {
      is Int -> Instant.ofEpochMilli(value.toLong())
      is Long -> Instant.ofEpochMilli(value)
      is String -> Instant.ofEpochMilli(value.toLong())
      else -> throw SQLException("Unable to convert $value to Timestamp")
    }
    val zone = cal?.timeZone?.toZoneId() ?: ZoneId.of("Z")
    return Timestamp.valueOf(instant.atZone(zone).toLocalDateTime())
  }

  override fun getBytes(index: Int): ByteArray {
    return when (val value = getObject(index)) {
      is ByteArray -> value
      else -> throw SQLException("Unable to convert $value to byte[]")
    }
  }

  override fun getDouble(index: Int): Double {
    return when (val value = getObject(index)) {
      is Double -> value
      is Float -> value.toDouble()
      is Int -> value.toDouble()
      is Long -> value.toDouble()
      is String -> value.toDouble()
      else -> throw SQLException("Unable to convert $value to double")
    }
  }

  override fun getString(index: Int): String? {
    val value = getObject(index)
    return value?.toString()
  }

  override fun getLong(index: Int): Long {
    return when (val value = getObject(index)) {
      is Int -> value.toLong()
      is Long -> value
      is String -> value.toLong()
      else -> throw SQLException("Unable to convert $value to String")
    }
  }

  override fun getInt(index: Int): Int {
    return when (val value = getObject(index)) {
      is Int -> value
      is Long -> value.toInt()
      is String -> value.toInt()
      else -> throw SQLException("Unable to convert $value to int")
    }
  }

  override fun getShort(index: Int): Short {
    return when (val value = getObject(index)) {
      is Int -> value.toShort()
      is Long -> value.toShort()
      is String -> value.toShort()
      else -> throw SQLException("Unable to convert $value to short")
    }
  }

  override fun getRowId(index: Int): RowId = LongRowId(index.toLong())
}