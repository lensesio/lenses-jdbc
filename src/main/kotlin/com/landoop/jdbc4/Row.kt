package com.landoop.jdbc4

import java.io.Reader
import java.math.BigDecimal
import java.sql.Date
import java.sql.RowId
import java.sql.Time
import java.sql.Timestamp
import java.util.*

interface Row {

  fun getRowId(columnIndex: Int): RowId

  fun getByte(index: Int): Byte

  fun charStream(index: Int): Reader
  fun getObject(index: Int): Any

  fun getFloat(index: Int): Float

  fun getBoolean(columnIndex: Int): Boolean
  fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal
  fun getBigDecimal(columnIndex: Int): BigDecimal

  fun getTime(columnIndex: Int): Time
  fun getTime(columnIndex: Int, cal: Calendar?): Time

  fun getDate(columnIndex: Int): Date
  fun getDate(columnIndex: Int, cal: Calendar?): Date

  fun getTimestamp(columnIndex: Int): Timestamp
  fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp

  fun getBytes(columnIndex: Int): ByteArray
  fun getDouble(columnIndex: Int): Double

  fun getString(index: Int): String

  fun getLong(columnIndex: Int): Long
  fun getInt(columnIndex: Int): Int
  fun getShort(columnIndex: Int): Short
}