package com.landoop.jdbc4.resultset

import com.landoop.jdbc4.row.Row
import java.io.Reader
import java.math.BigDecimal
import java.sql.Date
import java.sql.ResultSet
import java.sql.Time
import java.sql.Timestamp
import java.util.*

/**
 * A base implemetation of [ResultSet] that models returned data rows as
 * instances of [Row]. It provides implementations of the resultset getXXX
 * conversion functions as delegations to the current row object.
 */
abstract class RowResultSet : AbstractResultSet {

  // returns the row which the cursor is currently pointing to
  protected abstract var currentRow: Row

  // each time we invoke getXXX, this is the value that was returned last
  private var lastValue: Any? = null


  // returns the meta data for the current row
  abstract fun meta(): LResultSetMetaData

  override fun findColumn(label: String): Int = meta().indexForLabel(label)

  // updates the last returned value each time getXXX is invoked
  private fun <T> trackObject(t: T): T {
    this.lastValue = t
    return t
  }

  override fun wasNull(): Boolean = lastValue == null

  override fun getDate(index: Int): Date? = trackObject(currentRow.getDate(index))
  override fun getDate(label: String): Date? = trackObject(getDate(meta().indexForLabel(label)))
  override fun getDate(index: Int, cal: Calendar?): Date? = trackObject(currentRow.getDate(index, cal))
  override fun getDate(label: String, cal: Calendar?): Date? = trackObject(currentRow.getDate(meta().indexForLabel(
      label), cal))

  override fun getBoolean(index: Int): Boolean = trackObject(currentRow.getBoolean(index))
  override fun getBoolean(label: String): Boolean = trackObject(currentRow.getBoolean(meta().indexForLabel(label)))
  override fun getBigDecimal(index: Int, scale: Int): BigDecimal? = trackObject(currentRow.getBigDecimal(index,
      scale))

  override fun getBigDecimal(label: String,
                             scale: Int): BigDecimal? = trackObject(currentRow.getBigDecimal(meta().indexForLabel(
      label), scale))

  override fun getBigDecimal(index: Int): BigDecimal? = trackObject(currentRow.getBigDecimal(index))
  override fun getBigDecimal(label: String): BigDecimal? = trackObject(currentRow.getBigDecimal(meta().indexForLabel(
      label)))

  override fun getTime(index: Int): Time? = trackObject(currentRow.getTime(index))
  override fun getTime(label: String): Time? = trackObject(currentRow.getTime(meta().indexForLabel(label)))
  override fun getTime(index: Int, cal: Calendar): Time? = trackObject(currentRow.getTime(index, cal))
  override fun getTime(label: String, cal: Calendar): Time? = trackObject(currentRow.getTime(meta().indexForLabel(
      label), cal))

  override fun getByte(index: Int): Byte = trackObject(currentRow.getByte(index))
  override fun getByte(label: String): Byte = trackObject(currentRow.getByte(meta().indexForLabel(label)))
  override fun getString(index: Int): String? = trackObject(currentRow.getString(index))
  override fun getString(label: String): String? = trackObject(currentRow.getString(meta().indexForLabel(label)))
  override fun getObject(index: Int): Any? = trackObject(currentRow.getObject(index))
  override fun getObject(label: String): Any? = trackObject(currentRow.getObject(meta().indexForLabel(label)))
  override fun getLong(index: Int): Long = trackObject(currentRow.getLong(index))
  override fun getLong(label: String): Long = trackObject(currentRow.getLong(meta().indexForLabel(label)))
  override fun getFloat(index: Int): Float = trackObject(currentRow.getFloat(index))
  override fun getFloat(label: String): Float = trackObject(currentRow.getFloat(meta().indexForLabel(label)))
  override fun getInt(index: Int): Int = trackObject(currentRow.getInt(index))
  override fun getInt(label: String): Int = trackObject(currentRow.getInt(meta().indexForLabel(label)))
  override fun getShort(index: Int): Short = trackObject(currentRow.getShort(index))
  override fun getShort(label: String): Short = trackObject(currentRow.getShort(meta().indexForLabel(label)))
  override fun getTimestamp(index: Int): Timestamp? = trackObject(currentRow.getTimestamp(index))
  override fun getTimestamp(label: String): Timestamp? = trackObject(currentRow.getTimestamp(meta().indexForLabel(
      label)))

  override fun getTimestamp(index: Int, cal: Calendar): Timestamp? = trackObject(currentRow.getTimestamp(index, cal))
  override fun getTimestamp(label: String,
                            cal: Calendar): Timestamp? = trackObject(currentRow.getTimestamp(meta().indexForLabel(
      label), cal))

  override fun getBytes(index: Int): ByteArray? = trackObject(currentRow.getBytes(index))
  override fun getBytes(label: String): ByteArray? = trackObject(currentRow.getBytes(meta().indexForLabel(label)))
  override fun getDouble(index: Int): Double = trackObject(currentRow.getDouble(index))
  override fun getDouble(label: String): Double = trackObject(currentRow.getDouble(meta().indexForLabel(label)))
  override fun getNString(index: Int): String? = trackObject(currentRow.getString(index))
  override fun getNString(label: String): String? = trackObject(currentRow.getString(meta().indexForLabel(label)))
  override fun getCharacterStream(index: Int): Reader? = currentRow.charStream(index)
  override fun getCharacterStream(label: String): Reader? = currentRow.charStream(meta().indexForLabel(label))
}