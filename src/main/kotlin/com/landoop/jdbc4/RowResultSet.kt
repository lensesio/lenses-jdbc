package com.landoop.jdbc4

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
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

/**
 * An implementation of ResultSet that uses Avro as an underlying data format.
 * The resultset will generate the metadata from the Avro schema, and each row
 * will be generated from Avro Records.
 */
class RowResultSet(
    // the statement that created this resultset
    private val stmt: Statement?,
    // the schema for this resultset
    schema: Schema,
    // all rows for the resultset are wrapped in a Row abstraction to support extra methods
    private val rows: List<Row>) : ResultSet {

  internal companion object {

    val emptySchema: Schema = SchemaBuilder.fixed("empty").size(1)

    fun empty() = RowResultSet(null, emptySchema, emptyList())

    fun emptyOf(schema: Schema) = RowResultSet(null, schema, emptyList())

    fun fromRecords(schema: Schema, records: Collection<GenericData.Record>) =
        RowResultSet(
            null,
            schema,
            records.map { RecordRow(it) }
        )
  }

  // this is a pointer to the current row, starts off pointing to "before the data"
  // which is the way jdbc works - the user is expected to move the offset before accessing any data
  // jdbc manages row id starting at 1, but we use 0 like it should be, so public api methods
  // must remember to convert
  private var cursor = -1
  private var lastValue: Any? = null

  private val meta = LsqlResultSetMetaData(schema, this)
  override fun getMetaData(): ResultSetMetaData = meta

  override fun findColumn(label: String): Int = meta.indexForLabel(label)

  override fun getStatement(): Statement? = stmt

  override fun clearWarnings() {}
  override fun getWarnings(): SQLWarning? = null

  // these resultsets are entirely offline
  override fun isClosed(): Boolean = true
  override fun close() {}

  override fun getCursorName(): String = throw SQLFeatureNotSupportedException()
  override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

  override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(this)
  override fun <T : Any?> unwrap(iface: Class<T>): T {
    try {
      return iface.cast(this)
    } catch (cce: ClassCastException) {
      throw SQLException("Unable to unwrap instance as " + iface.toString())
    }
  }

  override fun wasNull(): Boolean = lastValue == null

  // == methods which mutate or query the resultset fetch parameters ==

  // our resultsets are always offline so can be scroll insensitive
  override fun getType(): Int = ResultSet.TYPE_SCROLL_INSENSITIVE

  // we do not support updates, so this method is irrelevant
  // CLOSE is the nearest match
  override fun getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
  override fun setFetchDirection(direction: Int) = throw SQLException("This resultset is offline, so fetch direction does not need to be set")

  override fun getFetchSize(): Int = -1
  override fun setFetchSize(rows: Int) {} // no op since this resultset is offline

  // == methods that mutate or query the cursor ==

  private val size = rows.size
  private val last = size - 1

  override fun getRow(): Int = cursor

  // returns the row at the current cursor position
  private fun currentRow(): Row = rows[cursor]

  override fun isLast(): Boolean = cursor == last
  override fun isFirst(): Boolean = cursor == 0
  override fun isBeforeFirst(): Boolean = cursor < 0
  override fun isAfterLast(): Boolean = cursor > last

  override fun next(): Boolean {
    cursor += 1
    if (cursor > last + 1)
      cursor = last + 1
    return cursor in 0..last
  }

  override fun previous(): Boolean {
    if (type == ResultSet.TYPE_FORWARD_ONLY)
      throw SQLException("Cannot invoke previous() on ResultSet.TYPE_FORWARD_ONLY")
    cursor -= 1
    if (cursor < -1)
      cursor = -1
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
    cursor = rows.size - 1
    return true
  }

  override fun relative(rows: Int): Boolean {
    val p = cursor + rows
    checkCursorBounds(p)
    cursor = p
    return true
  }

  override fun absolute(row: Int): Boolean {
    return if (row < 0) {
      val positiveRow = size - Math.abs(row) + 1
      absolute(positiveRow)
    } else {
      // minus 1 because the public API is 1 indexed
      checkCursorBounds(row - 1)
      cursor = row - 1
      true
    }
  }

  private fun checkCursorBounds(p: Int) {
    if (p < 0 || p > last)
      throw IndexOutOfBoundsException("Attempted to move cursor out of bounds: $p")
  }

  private fun <T> trackObject(t: T): T {
    this.lastValue = t
    return t
  }

  // == methods that return data types -> just delegation to the row interface

  override fun getDate(index: Int): Date? = trackObject(currentRow().getDate(index))
  override fun getDate(label: String): Date? = trackObject(getDate(meta.indexForLabel(label)))
  override fun getDate(index: Int, cal: Calendar?): Date? = trackObject(currentRow().getDate(index, cal))
  override fun getDate(label: String, cal: Calendar?): Date? = trackObject(currentRow().getDate(meta.indexForLabel(label), cal))
  override fun getBoolean(index: Int): Boolean = trackObject(currentRow().getBoolean(index))
  override fun getBoolean(label: String): Boolean = trackObject(currentRow().getBoolean(meta.indexForLabel(label)))
  override fun getBigDecimal(index: Int, scale: Int): BigDecimal? = trackObject(currentRow().getBigDecimal(index, scale))
  override fun getBigDecimal(label: String, scale: Int): BigDecimal? = trackObject(currentRow().getBigDecimal(meta.indexForLabel(label), scale))
  override fun getBigDecimal(index: Int): BigDecimal? = trackObject(currentRow().getBigDecimal(index))
  override fun getBigDecimal(label: String): BigDecimal? = trackObject(currentRow().getBigDecimal(meta.indexForLabel(label)))
  override fun getTime(index: Int): Time? = trackObject(currentRow().getTime(index))
  override fun getTime(label: String): Time? = trackObject(currentRow().getTime(meta.indexForLabel(label)))
  override fun getTime(index: Int, cal: Calendar): Time? = trackObject(currentRow().getTime(index, cal))
  override fun getTime(label: String, cal: Calendar): Time? = trackObject(currentRow().getTime(meta.indexForLabel(label), cal))
  override fun getByte(index: Int): Byte = trackObject(currentRow().getByte(index))
  override fun getByte(label: String): Byte = trackObject(currentRow().getByte(meta.indexForLabel(label)))
  override fun getString(index: Int): String? = trackObject(currentRow().getString(index))
  override fun getString(label: String): String? = trackObject(currentRow().getString(meta.indexForLabel(label)))
  override fun getObject(index: Int): Any? = trackObject(currentRow().getObject(index))
  override fun getObject(label: String): Any? = trackObject(currentRow().getObject(meta.indexForLabel(label)))
  override fun getLong(index: Int): Long = trackObject(currentRow().getLong(index))
  override fun getLong(label: String): Long = trackObject(currentRow().getLong(meta.indexForLabel(label)))
  override fun getFloat(index: Int): Float = trackObject(currentRow().getFloat(index))
  override fun getFloat(label: String): Float = trackObject(currentRow().getFloat(meta.indexForLabel(label)))
  override fun getInt(index: Int): Int = trackObject(currentRow().getInt(index))
  override fun getInt(label: String): Int = trackObject(currentRow().getInt(meta.indexForLabel(label)))
  override fun getShort(index: Int): Short = trackObject(currentRow().getShort(index))
  override fun getShort(label: String): Short = trackObject(currentRow().getShort(meta.indexForLabel(label)))
  override fun getTimestamp(index: Int): Timestamp? = trackObject(currentRow().getTimestamp(index))
  override fun getTimestamp(label: String): Timestamp? = trackObject(currentRow().getTimestamp(meta.indexForLabel(label)))
  override fun getTimestamp(index: Int, cal: Calendar): Timestamp? = trackObject(currentRow().getTimestamp(index, cal))
  override fun getTimestamp(label: String, cal: Calendar): Timestamp? = trackObject(currentRow().getTimestamp(meta.indexForLabel(label), cal))
  override fun getBytes(index: Int): ByteArray? = trackObject(currentRow().getBytes(index))
  override fun getBytes(label: String): ByteArray? = trackObject(currentRow().getBytes(meta.indexForLabel(label)))
  override fun getDouble(index: Int): Double = trackObject(currentRow().getDouble(index))
  override fun getDouble(label: String): Double = trackObject(currentRow().getDouble(meta.indexForLabel(label)))
  override fun getRowId(index: Int): RowId = trackObject(LongRowId(cursor.toLong()))
  override fun getRowId(label: String?): RowId = trackObject(LongRowId(cursor.toLong()))
  override fun getNString(index: Int): String? = trackObject(currentRow().getString(index))
  override fun getNString(label: String): String? = trackObject(currentRow().getString(meta.indexForLabel(label)))
  override fun getCharacterStream(index: Int): Reader? = currentRow().charStream(index)
  override fun getCharacterStream(label: String): Reader? = currentRow().charStream(meta.indexForLabel(label))

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