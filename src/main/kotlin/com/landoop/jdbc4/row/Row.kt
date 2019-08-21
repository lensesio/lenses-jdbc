package com.landoop.jdbc4.row

import java.io.Reader
import java.math.BigDecimal
import java.sql.RowId
import java.sql.Time
import java.sql.Timestamp
import java.util.*

interface Row {

  fun getObject(index: Int): Any?

  fun getRowId(index: Int): RowId

  fun charStream(index: Int): Reader?

  fun getBigDecimal(index: Int, scale: Int): BigDecimal?
  fun getBigDecimal(index: Int): BigDecimal?
  fun getBoolean(index: Int): Boolean
  fun getByte(index: Int): Byte
  fun getBytes(index: Int): ByteArray?

  fun getDate(index: Int): java.sql.Date?
  fun getDate(index: Int, cal: Calendar?): java.sql.Date?

  fun getFloat(index: Int): Float

  fun getInt(index: Int): Int

  fun getTime(index: Int): Time?
  fun getTime(index: Int, cal: Calendar?): Time?

  fun getLong(index: Int): Long

  fun getTimestamp(index: Int): Timestamp?
  fun getTimestamp(index: Int, cal: Calendar?): Timestamp?

  fun getDouble(index: Int): Double

//  fun indexOf(alias: String): Int
//  fun getString(alias: String): String? = getString(indexOf(alias))
  fun getString(index: Int): String?

  fun getShort(index: Int): Short
}