package com.landoop.jdbc4.resultset

import com.landoop.jdbc4.Row
import java.io.Reader
import java.math.BigDecimal
import java.sql.Date
import java.sql.RowId
import java.sql.Time
import java.sql.Timestamp
import java.util.*

object empty : Row {

  override fun getObject(index: Int): Any? {
    TODO()
  }

  override fun getRowId(index: Int): RowId {
    TODO()
  }

  override fun charStream(index: Int): Reader? {
    TODO()
  }

  override fun getBigDecimal(index: Int, scale: Int): BigDecimal? {
    TODO()
  }

  override fun getBigDecimal(index: Int): BigDecimal? {
    TODO()
  }

  override fun getBoolean(index: Int): Boolean {
    TODO()
  }

  override fun getByte(index: Int): Byte {
    TODO()
  }

  override fun getBytes(index: Int): ByteArray? {
    TODO()
  }

  override fun getDate(index: Int): Date? {
    TODO()
  }

  override fun getDate(index: Int, cal: Calendar?): Date? {
    TODO()
  }

  override fun getFloat(index: Int): Float {
    TODO()
  }

  override fun getInt(index: Int): Int {
    TODO()
  }

  override fun getTime(index: Int): Time? {
    TODO()
  }

  override fun getTime(index: Int, cal: Calendar?): Time? {
    TODO()
  }

  override fun getLong(index: Int): Long {
    TODO()
  }

  override fun getTimestamp(index: Int): Timestamp? {
    TODO()
  }

  override fun getTimestamp(index: Int, cal: Calendar?): Timestamp? {
    TODO()
  }

  override fun getDouble(index: Int): Double {
    TODO()
  }

  override fun getString(index: Int): String? {
    TODO()
  }

  override fun getShort(index: Int): Short {
    TODO()
  }
}
