package com.landoop.jdbc4

import java.io.Reader
import java.math.BigDecimal
import java.sql.Date
import java.sql.RowId
import java.sql.Time
import java.sql.Timestamp
import java.util.*

interface Row {

  fun getRowId(index: Int): RowId

  fun getByte(index: Int): Byte

  fun charStream(index: Int): Reader
  fun getObject(index: Int): Any

  fun getFloat(index: Int): Float

  fun getBoolean(index: Int): Boolean
  fun getBigDecimal(index: Int, scale: Int): BigDecimal
  fun getBigDecimal(index: Int): BigDecimal

  fun getTime(index: Int): Time
  fun getTime(index: Int, cal: Calendar?): Time

  fun getDate(index: Int): java.sql.Date
  fun getDate(index: Int, cal: Calendar?): java.sql.Date

  fun getTimestamp(index: Int): Timestamp
  fun getTimestamp(index: Int, cal: Calendar?): Timestamp

  fun getBytes(index: Int): ByteArray
  fun getDouble(index: Int): Double

  fun getString(index: Int): String

  fun getLong(index: Int): Long
  fun getInt(index: Int): Int
  fun getShort(index: Int): Short
}