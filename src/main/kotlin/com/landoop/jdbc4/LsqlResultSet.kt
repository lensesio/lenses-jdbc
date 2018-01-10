package com.landoop.jdbc4

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.Blob
import java.sql.Clob
import java.sql.Date
import java.sql.NClob
import java.sql.Ref
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.RowId
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Statement
import java.sql.Time
import java.sql.Timestamp
import java.util.*

class LsqlResultSet(
    // the statement that created this resultset
    private val stmt: Statement?,
    tableName: String,
    private val fetchType: Int,
    // the schema for this resultset
    schema: Schema,
    // all rows for the resultset are wrapped in a Row abstraction to support extra methods
    private val records: Array<Row>) : ResultSet {

  internal companion object {
    val emptySchema: Schema = SchemaBuilder.fixed("empty").size(1)
    fun empty() = LsqlResultSet(null, "", ResultSet.TYPE_FORWARD_ONLY, emptySchema, emptyArray())
  }

  // this is a pointer to the current row, starts off pointing to "before the data"
  // which is the way jdbc works - the user is expected to move the offset before accessing any data
  // jdbc manages row id starting at 1, but we use 0 like it should be, so public api methods
  // must remember to convert
  var cursor = -1
  var direction = ResultSet.FETCH_FORWARD

  private val meta = LsqlResultSetMetaData(tableName, schema, this)
  override fun getMetaData(): ResultSetMetaData = meta

  override fun findColumn(label: String): Int = meta.indexForLabel(label)

  override fun getStatement(): Statement? = stmt

  override fun getWarnings(): SQLWarning? = null

  override fun close() {
    // these resultsets are entirely offline
  }

  override fun <T : Any?> unwrap(iface: Class<T>): T {
    try {
      return iface.cast(this)
    } catch (cce: ClassCastException) {
      throw SQLException("Unable to unwrap instance as " + iface.toString())
    }
  }

  override fun wasNull(): Boolean = false

  // == methods which mutate or query the resultset fetch parameters ==

  override fun getType(): Int = fetchType

  override fun setFetchDirection(direction: Int) {
    when (direction) {
      ResultSet.FETCH_FORWARD -> this.direction = ResultSet.FETCH_FORWARD
      ResultSet.FETCH_REVERSE -> {
        if (type == ResultSet.TYPE_FORWARD_ONLY)
          throw SQLException("Cannot set fetch direction to reverse on ResultSet.TYPE_FORWARD_ONLY")
        this.direction = ResultSet.FETCH_REVERSE
      }
      else -> throw SQLException("Unsupported fetch direction $direction")
    }
  }

  override fun getFetchDirection(): Int = direction

  override fun getFetchSize(): Int = -1

  override fun setFetchSize(rows: Int) { // no op since this resultset is offline
  }

  // == methods that mutate or query the cursor ==

  private val last = records.size - 1

  override fun getRow(): Int = cursor

  // returns the row at the current cursor position
  private fun currentRow(): Row = records[cursor]

  override fun isLast(): Boolean = cursor == last
  override fun isFirst(): Boolean = cursor == 0
  override fun isAfterLast(): Boolean = cursor > last

  override fun next(): Boolean {
    when (direction) {
      ResultSet.FETCH_FORWARD -> cursor += 1
      ResultSet.FETCH_REVERSE -> cursor -= 1
    }
    return cursor in 0..last
  }

  override fun previous(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke previous() on ResultSet.TYPE_FORWARD_ONLY")
    when (direction) {
      ResultSet.FETCH_FORWARD -> cursor -= 1
      ResultSet.FETCH_REVERSE -> cursor += 1
    }
    return cursor in 0..last
  }

  override fun beforeFirst() {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke beforeFirst() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = -1
  }

  override fun afterLast() {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke afterLast() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = last + 1
  }

  override fun first(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke first() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = 0
    return true
  }

  override fun last(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke last() on ResultSet.TYPE_FORWARD_ONLY")
    cursor = records.size - 1
    return true
  }

  override fun relative(rows: Int): Boolean {
    val p = cursor + rows
    checkCursorBounds(p)
    cursor = p
    return true
  }

  override fun absolute(row: Int): Boolean {
    checkCursorBounds(row)
    cursor = row
    return true
  }

  private fun checkCursorBounds(p: Int) {
    if (p < 0 || p > last)
      throw IndexOutOfBoundsException("Attempted to move cursor out of bounds: $p")
  }

  override fun getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  // == methods that return data types -> just delegation to the row interface

  override fun getDate(index: Int): Date = currentRow().getDate(index)
  override fun getDate(label: String): Date = getDate(meta.indexForLabel(label))
  override fun getDate(index: Int, cal: Calendar?): Date = currentRow().getDate(index, cal)
  override fun getDate(label: String, cal: Calendar?): Date = currentRow().getDate(meta.indexForLabel(label), cal)
  override fun getBoolean(index: Int): Boolean = currentRow().getBoolean(index)
  override fun getBoolean(label: String): Boolean = currentRow().getBoolean(meta.indexForLabel(label))
  override fun getBigDecimal(index: Int, scale: Int): BigDecimal = currentRow().getBigDecimal(index, scale)
  override fun getBigDecimal(label: String, scale: Int): BigDecimal = currentRow().getBigDecimal(meta.indexForLabel(label), scale)
  override fun getBigDecimal(index: Int): BigDecimal = currentRow().getBigDecimal(index)
  override fun getBigDecimal(label: String): BigDecimal = currentRow().getBigDecimal(meta.indexForLabel(label))
  override fun getTime(index: Int): Time = currentRow().getTime(index)
  override fun getTime(label: String): Time = currentRow().getTime(meta.indexForLabel(label))
  override fun getTime(index: Int, cal: Calendar): Time = currentRow().getTime(index, cal)
  override fun getTime(label: String, cal: Calendar): Time = currentRow().getTime(meta.indexForLabel(label), cal)
  override fun getByte(index: Int): Byte = currentRow().getByte(index)
  override fun getByte(label: String): Byte = currentRow().getByte(meta.indexForLabel(label))
  override fun getString(index: Int): String = currentRow().getString(index)
  override fun getString(label: String): String = currentRow().getString(meta.indexForLabel(label))
  override fun getObject(index: Int): Any = currentRow().getObject(index)
  override fun getObject(label: String): Any = currentRow().getObject(meta.indexForLabel(label))
  override fun getLong(index: Int): Long = currentRow().getLong(index)
  override fun getLong(label: String): Long = currentRow().getLong(meta.indexForLabel(label))
  override fun getFloat(index: Int): Float = currentRow().getFloat(index)
  override fun getFloat(label: String): Float = currentRow().getFloat(meta.indexForLabel(label))
  override fun getInt(index: Int): Int = currentRow().getInt(index)
  override fun getInt(label: String): Int = currentRow().getInt(meta.indexForLabel(label))
  override fun getShort(index: Int): Short = currentRow().getShort(index)
  override fun getShort(label: String): Short = currentRow().getShort(meta.indexForLabel(label))
  override fun getTimestamp(index: Int): Timestamp = currentRow().getTimestamp(index)
  override fun getTimestamp(label: String): Timestamp = currentRow().getTimestamp(meta.indexForLabel(label))
  override fun getTimestamp(index: Int, cal: Calendar): Timestamp = currentRow().getTimestamp(index, cal)
  override fun getTimestamp(label: String, cal: Calendar): Timestamp = currentRow().getTimestamp(meta.indexForLabel(label), cal)
  override fun getBytes(index: Int): ByteArray = currentRow().getBytes(index)
  override fun getBytes(label: String): ByteArray = currentRow().getBytes(meta.indexForLabel(label))
  override fun getDouble(index: Int): Double = currentRow().getDouble(index)
  override fun getDouble(label: String): Double = currentRow().getDouble(meta.indexForLabel(label))
  override fun getRowId(index: Int): RowId = KafkaRowId(cursor.toString())
  override fun getRowId(label: String?): RowId = KafkaRowId(cursor.toString())
  override fun getNString(index: Int): String = currentRow().getString(index)
  override fun getNString(label: String): String = currentRow().getString(meta.indexForLabel(label))

  override fun getCursorName(): String = throw SQLFeatureNotSupportedException()

  override fun isClosed(): Boolean = true

  override fun getCharacterStream(index: Int): Reader = currentRow().charStream(index)
  override fun getCharacterStream(label: String): Reader = currentRow().charStream(meta.indexForLabel(label))

  override fun isBeforeFirst(): Boolean = cursor < 0

  override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(this)

  override fun clearWarnings() {}


  override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

  // unsupported data types

  override fun getNClob(index: Int): NClob = throw SQLFeatureNotSupportedException()
  override fun getNClob(label: String?): NClob = throw SQLFeatureNotSupportedException()
  override fun getBinaryStream(index: Int): InputStream? = throw SQLFeatureNotSupportedException()
  override fun getBinaryStream(label: String): InputStream? = throw SQLFeatureNotSupportedException()
  override fun getBlob(index: Int): Blob? = throw SQLFeatureNotSupportedException()
  override fun getBlob(label: String): Blob? = throw SQLFeatureNotSupportedException()
  override fun getUnicodeStream(index: Int): InputStream = throw SQLFeatureNotSupportedException()
  override fun getUnicodeStream(label: String?): InputStream = throw SQLFeatureNotSupportedException()
  override fun getNCharacterStream(index: Int): Reader = throw SQLFeatureNotSupportedException()
  override fun getNCharacterStream(label: String?): Reader = throw SQLFeatureNotSupportedException()
  override fun getAsciiStream(index: Int): InputStream = throw SQLFeatureNotSupportedException()
  override fun getAsciiStream(label: String?): InputStream = throw SQLFeatureNotSupportedException()
  override fun getSQLXML(index: Int): SQLXML = throw SQLFeatureNotSupportedException()
  override fun getSQLXML(label: String?): SQLXML = throw SQLFeatureNotSupportedException()
  override fun getURL(index: Int): URL = throw SQLFeatureNotSupportedException()
  override fun getURL(label: String?): URL = throw SQLFeatureNotSupportedException()
  override fun getObject(index: Int, map: MutableMap<String, Class<*>>?): Any = throw SQLFeatureNotSupportedException()
  override fun getObject(label: String?, map: MutableMap<String, Class<*>>?): Any = throw SQLFeatureNotSupportedException()
  override fun <T : Any?> getObject(index: Int, type: Class<T>?): T = throw SQLFeatureNotSupportedException()
  override fun <T : Any?> getObject(label: String?, type: Class<T>?): T = throw SQLFeatureNotSupportedException()
  override fun getClob(index: Int): Clob = throw SQLFeatureNotSupportedException()
  override fun getClob(label: String?): Clob = throw SQLFeatureNotSupportedException()
  override fun getArray(index: Int): java.sql.Array = throw SQLFeatureNotSupportedException()
  override fun getArray(label: String?): java.sql.Array = throw SQLFeatureNotSupportedException()
  override fun getRef(index: Int): Ref = throw SQLFeatureNotSupportedException()
  override fun getRef(label: String?): Ref = throw SQLFeatureNotSupportedException()

  // == mutation methods not supported ==

  override fun refreshRow() {}
  override fun rowInserted(): Boolean = throw SQLFeatureNotSupportedException()
  override fun rowDeleted(): Boolean = throw SQLFeatureNotSupportedException()
  override fun moveToInsertRow() = throw SQLFeatureNotSupportedException()
  override fun insertRow() = throw SQLFeatureNotSupportedException()
  override fun rowUpdated(): Boolean = throw SQLFeatureNotSupportedException()
  override fun moveToCurrentRow() {}
  override fun cancelRowUpdates() = throw SQLFeatureNotSupportedException()

  override fun updateShort(index: Int, x: Short) = throw SQLFeatureNotSupportedException()
  override fun updateShort(label: String?, x: Short) = throw SQLFeatureNotSupportedException()
  override fun updateLong(index: Int, x: Long) = throw SQLFeatureNotSupportedException()
  override fun updateLong(label: String?, x: Long) = throw SQLFeatureNotSupportedException()
  override fun updateClob(index: Int, x: Clob?) = throw SQLFeatureNotSupportedException()
  override fun updateClob(label: String?, x: Clob?) = throw SQLFeatureNotSupportedException()
  override fun updateClob(index: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateClob(label: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateClob(index: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateClob(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateTimestamp(index: Int, x: Timestamp?) = throw SQLFeatureNotSupportedException()
  override fun updateTimestamp(label: String?, x: Timestamp?) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(index: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(label: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(index: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateNCharacterStream(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateInt(index: Int, x: Int) = throw SQLFeatureNotSupportedException()
  override fun updateInt(label: String?, x: Int) = throw SQLFeatureNotSupportedException()
  override fun updateNString(index: Int, nString: String?) = throw SQLFeatureNotSupportedException()
  override fun updateNString(label: String?, nString: String?) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(index: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(label: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(index: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(label: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(index: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateBinaryStream(label: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateNull(index: Int) = throw SQLFeatureNotSupportedException()
  override fun updateNull(label: String?) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(index: Int, x: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(index: Int, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(label: String?, reader: Reader?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(index: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateCharacterStream(label: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBoolean(index: Int, x: Boolean) = throw SQLFeatureNotSupportedException()
  override fun updateBoolean(label: String?, x: Boolean) = throw SQLFeatureNotSupportedException()
  override fun updateBigDecimal(index: Int, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
  override fun updateBigDecimal(label: String?, x: BigDecimal?) = throw SQLFeatureNotSupportedException()
  override fun updateString(index: Int, x: String?) = throw SQLFeatureNotSupportedException()
  override fun updateString(label: String?, x: String?) = throw SQLFeatureNotSupportedException()
  override fun updateTime(index: Int, x: Time?) = throw SQLFeatureNotSupportedException()
  override fun updateTime(label: String?, x: Time?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(index: Int, nClob: NClob?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(label: String?, nClob: NClob?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(index: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(label: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(index: Int, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateNClob(label: String?, reader: Reader?) = throw SQLFeatureNotSupportedException()
  override fun updateRef(index: Int, x: Ref?) = throw SQLFeatureNotSupportedException()
  override fun updateRef(label: String?, x: Ref?) = throw SQLFeatureNotSupportedException()
  override fun updateObject(index: Int, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
  override fun updateObject(index: Int, x: Any?) = throw SQLFeatureNotSupportedException()
  override fun updateObject(label: String?, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException()
  override fun updateObject(label: String?, x: Any?) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(index: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(label: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(index: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(label: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(index: Int, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateAsciiStream(label: String?, x: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateBytes(index: Int, x: ByteArray?) = throw SQLFeatureNotSupportedException()
  override fun updateBytes(label: String?, x: ByteArray?) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(index: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(label: String?, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(index: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(label: String?, inputStream: InputStream?) = throw SQLFeatureNotSupportedException()
  override fun updateRowId(index: Int, x: RowId?) = throw SQLFeatureNotSupportedException()
  override fun updateRowId(label: String?, x: RowId?) = throw SQLFeatureNotSupportedException()
  override fun updateArray(index: Int, x: java.sql.Array?) = throw SQLFeatureNotSupportedException()
  override fun updateArray(label: String?, x: java.sql.Array?) = throw SQLFeatureNotSupportedException()
  override fun updateSQLXML(index: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
  override fun updateSQLXML(label: String?, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException()
  override fun updateDate(index: Int, x: Date?) = throw SQLFeatureNotSupportedException()
  override fun updateDate(label: String?, x: Date?) = throw SQLFeatureNotSupportedException()
  override fun updateDouble(index: Int, x: Double) = throw SQLFeatureNotSupportedException()
  override fun updateDouble(label: String?, x: Double) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(index: Int, x: Blob?) = throw SQLFeatureNotSupportedException()
  override fun updateBlob(label: String?, x: Blob?) = throw SQLFeatureNotSupportedException()
  override fun updateByte(index: Int, x: Byte) = throw SQLFeatureNotSupportedException()
  override fun updateByte(label: String?, x: Byte) = throw SQLFeatureNotSupportedException()
  override fun updateRow() = throw SQLFeatureNotSupportedException()
  override fun deleteRow() = throw SQLFeatureNotSupportedException()
  override fun updateFloat(index: Int, x: Float) = throw SQLFeatureNotSupportedException()
  override fun updateFloat(label: String?, x: Float) = throw SQLFeatureNotSupportedException()
}